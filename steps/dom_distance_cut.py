import numpy as np
from scipy.spatial import ConvexHull

from icecube import phys_services, icetray, dataclasses

class OversizeStream(object):
    def __init__(self,
                 stream_id,
                 distance_cut,
                 dom_limit,
                 oversize_factor):
        self.stream_id = stream_id
        self.distance_cut = distance_cut
        self.dom_limit = dom_limit
        self.oversize_factor = oversize_factor
        if not isinstance(stream_id, int):
            raise TypeError('stream id has to be int!')
        elif stream_id == -1:
            self.stream_id = -1
            self.stream_name = 'MCOversizeStreamDefault'
            self.file_addition = 'OversizeStreamDefault'
        else:
            self.stream_name = 'MCOversizeStream{}'.format(self.stream_id)
            self.file_addition = 'OversizeStream{}'.format(
                self.stream_id)


    def __call__(self, frame):
        if frame.Stop == icetray.I3Frame.DAQ:
            if frame.Has(self.stream_name):
                if frame[self.stream_name]:
                    return True
                else:
                    return False
            else:
                raise KeyError('{} not found'.format(self.stream_name))
        else:
            return True

    def transform_filepath(self, filepath):
        return filepath.replace('i3.bz2',
                                '{}.i3.bz2'.format(self.file_addition))


def generate_stream_object(cut_distances, dom_limits, oversize_factors):
    order = np.argsort(cut_distances)
    cut_distances = cut_distances[order]
    dom_limits = dom_limits[order]
    oversize_factors = oversize_factors[order]

    stream_id = 0
    stream_objects = []

    for dist_i, lim_i, factor_i in zip(cut_distances,
                                       dom_limits,
                                       oversize_factors):
        if dist_i < 0:
            id_ = -1
        else:
            id_ = stream_id
            stream_id += 1
        stream_objects.append(
            OversizeStream(id_,
                           distance_cut=dist_i,
                           dom_limit=lim_i,
                           oversize_factor=factor_i))

    return stream_objects


class OversizeSplitterNSplits(icetray.I3ConditionalModule):
    S_stream = icetray.I3Frame.Stream('S')

    def __init__(self, context):
        icetray.I3ConditionalModule.__init__(self, context)
        self.AddParameter('thresholds',
                          'Cut distance',
                          [10])
        self.AddParameter('thresholds_doms',
                          'Treshold for too many close DOMs',
                          1)
        self.AddParameter('oversize_factors',
                          'Treshold for too many close DOMs',
                          None)
        self.AddParameter('relevance_dist',
                          'Max distance to cosinder a DOM as relevant',
                          200.)

    def Configure(self):
        self.thresholds = np.atleast_1d(self.GetParameter('thresholds'))
        self.lim_doms = np.atleast_1d(self.GetParameter('thresholds_doms'))
        if len(self.thresholds) != len(self.lim_doms):
            if len(self.lim_doms) == 1:
                self.lim_doms = np.ones_like(self.thresholds) * self.lim_doms
            else:
                raise ValueError('Provide either a DOM limit for each distance'
                                 ' or one for all!')
        if self.GetParameter('oversize_factors') is None:
            ValueError('No OversizeFactors Provided! You better document '
                          'your settings!')
        else:
            self.oversize_factors = np.atleast_1d(
                self.GetParameter('oversize_factors'))
            if not len(self.oversize_factors) == len(self.thresholds):
                raise ValueError('You should provide a oversize factor for '
                                 'split!')
        order = np.argsort(self.thresholds)
        self.thresholds = self.thresholds[order]
        self.lim_doms = self.lim_doms[order]
        if any(self.thresholds == -1.):
            self.default_idx = np.where(self.thresholds == -1.)[0][0]
        else:
            self.default_idx = None
        relevance_dist_needed = any([x < 1. for i, x in enumerate(self.lim_doms)
                                     if i != self.default_idx])
        if relevance_dist_needed:
            self.relevance_dist = self.GetParameter('relevance_dist')
        else:
            self.relevance_dist = None
        self.stream_objects = generate_stream_object(self.thresholds,
                                                     self.lim_doms,
                                                     self.oversize_factors)
        self.hist = np.zeros(len(self.stream_objects), dtype=int)
        self.min_distances = []
        self.Register(self.S_stream, self.SFrame)

    def Geometry(self, frame):
        omgeo = frame['I3Geometry'].omgeo
        self.dom_positions = np.zeros((len(omgeo), 3))
        self.PushFrame(frame)

    def SFrame(self, frame):
        frame['MCDistanceCuts'] = dataclasses.I3VectorDouble(self.thresholds)
        frame['MCDomThresholds'] = dataclasses.I3VectorDouble(self.lim_doms)
        if self.relevance_dist is not None:
            frame['MCRelevanceDist'] = dataclasses.I3Double(
                self.relevance_dist)
        if self.oversize_factors is not None:
            frame['MCOversizing'] = dataclasses.I3VectorDouble(
                self.oversize_factors)
        self.PushFrame(frame)

    def DAQ(self, frame):
        particle = frame['MCMuon']
        v_dir = np.array([particle.dir.x, particle.dir.y, particle.dir.z])
        v_pos = np.array(particle.pos)
        distances = np.linalg.norm(np.cross(v_dir, v_pos - self.dom_positions),
                                   axis=1)
        if self.relevance_dist is not None:
            n_relevant_doms = distances < self.relevance_dist

        already_added = False

        for i, stream_i in enumerate(self.stream_objects):
            if i == self.default_idx:
                continue
            if already_added:
                is_in_stream = False
            else:
                if stream_i.dom_limit < 1.:
                    limit_i = n_relevant_doms * stream_i.dom_limit
                else:
                    limit_i = stream_i.dom_limit
                is_in_stream = np.sum(distances < stream_i.distance_cut) >= limit_i
            frame[stream_i.stream_name] = icetray.I3Bool(is_in_stream)
            if is_in_stream:
                self.hist[i] += 1
                already_added = True
        if self.default_idx is not None:
            if already_added and self.default_idx is not None:
                frame['MCOversizeStreamDefault'] = icetray.I3Bool(False)
            else:
                self.hist[self.default_idx] += 1
                frame['MCOversizeStreamDefault'] = icetray.I3Bool(True)
        self.PushFrame(frame)

    def Finish(self):
        print(self.hist)
        print(np.sort(self.min_distances))
