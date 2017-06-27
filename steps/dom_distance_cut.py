import warnings
import numpy as np
from scipy.spatial import ConvexHull

from icecube import phys_services, icetray, dataclasses


def ray_triangle_intersection(ray_near, ray_dir, triangle):
    (v1, v2, v3) = triangle
    eps = 0.000001
    edge1 = v2 - v1
    edge2 = v3 - v1
    pvec = np.cross(ray_dir, edge2)
    det = edge1.dot(pvec)
    if abs(det) < eps:
        return np.nan
    inv_det = 1. / det
    tvec = ray_near - v1
    u = tvec.dot(pvec) * inv_det
    if u < 0. or u > 1.:
        return np.nan
    qvec = np.cross(tvec, edge1)
    v = ray_dir.dot(qvec) * inv_det
    if v < 0. or u + v > 1.:
        return np.nan
    t = edge2.dot(qvec) * inv_det
    if t < eps:
        return np.nan
    return t


def get_intersections(convex_hull, v_pos, v_dir, eps=1e-4):
    if not isinstance(v_pos, np.ndarray):
        v_pos = np.array(v_pos)
    if not isinstance(v_dir, np.ndarray):
        v_dir = np.array(v_dir)
    t_s = [ray_triangle_intersection(v_pos,
                                     v_dir,
                                     convex_hull.points[simp])
           for simp in convex_hull.simplices]
    t_s = np.array(t_s)
    t_s = t_s[np.isfinite(t_s)]
    if len(t_s) != 2:
        t_s_back = [ray_triangle_intersection(v_pos,
                                              -v_dir,
                                              convex_hull.points[simp])
                    for simp in convex_hull.simplices]
        t_s_back = np.array(t_s_back)
        t_s_back = t_s_back[np.isfinite(t_s_back)]
        t_s = np.hstack((t_s, t_s_back * (-1.)))
    if isinstance(eps, float):  # Remove similar intersections
        if eps >= 0.:
            t_selected = []
            intersections = []
            for t_i in t_s:
                intersection_i = v_pos + t_i * v_dir
                distances = [np.linalg.norm(intersection_i - intersection_j)
                             for intersection_j in intersections]
                if not (np.array(distances) < eps).any():
                    t_selected.append(t_i)
                    intersections.append(intersection_i)
            t_s = np.array(t_selected)
    return t_s


def particle_is_inside(convex_hull, particle):
    if particle is None:
        return False
    v_pos = np.array(particle.pos)
    v_dir = np.array([particle.dir.x,
                      particle.dir.y,
                      particle.dir.z])
    t_s = get_intersections(
        convex_hull=convex_hull,
        v_pos=v_pos,
        v_dir=v_dir,
        eps=1e-4)
    intersections = []
    for t_i in t_s:
        pos = dataclasses.I3Position()
        pos.x, pos.y, pos.z = v_pos + t_i * v_dir
        intersections.append(pos)
    return len(intersections) > 0


def low_oversize_stream(frame):
    if frame.Stop == icetray.I3Frame.DAQ:
        if frame.Has('MCLowOversizeStream'):
            if frame['MCLowOversizeStream']:
                return True
            else:
                return False
        else:
            raise KeyError('MCLowOversizeStream not found')
    else:
        return True

def high_oversize_stream(frame):
    if frame.Stop == icetray.I3Frame.DAQ:
        if frame.Has('MCHighOversizeStream'):
            if frame['MCHighOversizeStream']:
                return True
            else:
                return False
        else:
            raise KeyError('MCHighOversizeStream not found')
    else:
        return True

class oversize_stream(object):
    def __init__(self, stream_id=None):
        if stream_id is None:
            self.stream_name = 'MCOversizeStreamDefault'
        elif isinstance(stream_id, int):
            self.stream_name = 'MCOversizeStream{}'.format(stream_id)
        else:
            raise TypeError('stream_id must be int or None')
        self.stream_id = stream_id

    def __call__(self, frame):
        if frame.Stop == icetray.I3Frame.DAQ:
            if frame.Has(self.stream_name):
                if frame[self.stream_name]:
                    return True
                else:
                    return False
            else:
                raise KeyError('MCHighOversizeStream not found')
        else:
            return True

    def transform_outfile(self, file_path):
        if self.stream_id is None:
            self.stream_name = 'MCOversizeStreamDefault'
            addition = 'OversizeStreamDefault'
        else:
            addition = 'OversizeStream{}'.format(self.stream_id)
        return file_path.replace('i3.bz2', '{}.i3.bz2'.format(addition))



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
            warnings.warn('No OversizeFactors Provided! You better document '
                          'your settings!')
        else:
            self.oversize_factors = self.GetParameter('oversize_factors')
            if isinstance(self.oversize_factors, list):
                for i, factor_i in enumerate(self.oversize_factors):
                    if isinstance(factor_i, str):
                        if factor_i.lower() == 'dima':
                            self.oversize_factors = 16.
            self.oversize_factors = np.atleast_1d(self.oversize_factors)
            if not len(self.oversize_factors) == len(self.thresholds) + 1:
                raise ValueError('You should provide n_thresholds + 1 '
                                 'oversize factors. The last should be the '
                                 'default!')
        order = np.argsort(self.thresholds)
        self.thresholds = self.thresholds[order]
        self.lim_doms = self.lim_doms[order]
        if any(self.lim_doms) < 1.:
            self.relevance_dist = self.GetParameter('relevance_dist')
        else:
            self.relevance_dist = None

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
        for i, [threshold_i, limit_i] in enumerate(
                zip(self.thresholds, self.lim_doms)):
            stream_name = 'MCOversizeStream{}'.format(i)
            if already_added:
                is_in_stream = False
            else:
                if limit_i < 1.:
                    limit_i = n_relevant_doms * limit_i
                is_in_stream = np.sum(distances < threshold_i) >= limit_i
            print('===')
            print(threshold_i)
            print(np.sum(distances < threshold_i))
            print(np.min(distances))
            print('===')
            frame[stream_name] = icetray.I3Bool(is_in_stream)
            if is_in_stream:
                already_added = True
        if already_added:
            frame['MCOversizeStreamDefault'] = icetray.I3Bool(False)
        else:
            frame['MCOversizeStreamDefault'] = icetray.I3Bool(True)
        self.PushFrame(frame)



class qStreamSwitcher(icetray.I3ConditionalModule):
    q_stream = icetray.I3Frame.Stream('q')

    def __init__(self, context):
        icetray.I3ConditionalModule.__init__(self, context)

    def Configure(self):
        self.Register(self.q_stream, self.qFrame)
        self.switch = True

    def qFrame(self, frame):
        if self.switch:
            frame.stop = icetray.I3Frame.DAQ
        self.PushFrame(frame)


class OversizeSplitter(qStreamSwitcher):
    S_stream = icetray.I3Frame.Stream('S')

    def __init__(self, context):
        icetray.I3ConditionalModule.__init__(self, context)
        self.AddParameter('threshold',
                          'Cut distance',
                          10)
        self.AddParameter('split_streams',
                          'Split into DAQ-frames and q-frames.',
                          False)
        self.AddParameter('threshold_doms',
                          'Treshold for too many close DOMs',
                          1)
        self.AddParameter('relevance_dist',
                          'Max distance to cosinder a DOM as relevant',
                          200.)
        self.AddParameter('check_containment',
                          'Check if contained (computing intensiv)',
                          True)
        self.AddParameter('containment_padding',
                          'Padding used to define the detector volume',
                          60.)
        self.AddParameter('cut_distances',
                          'Padding used to define the detector volume',
                          [2., 4., 6., 8., 10., 15., 20., 25.,
                           30., 35., 40., 45., 50., 55., 60.])

    def Configure(self):
        super(qStreamSwitcher, self).Configure()
        self.threshold = self.GetParameter('threshold')
        self.relevance_dist = self.GetParameter('relevance_dist')
        self.split_streams = self.GetParameter('split_streams')
        self.threshold_doms = self.GetParameter('threshold_doms')
        self.check_containment = self.GetParameter('check_containment')
        self.containment_padding = self.GetParameter('containment_padding')
        self.multiple_distance = self.GetParameter('cut_distances')
        self.switch = False
        self.Register(self.S_stream, self.SFrame)

    def Geometry(self, frame):
        omgeo = frame['I3Geometry'].omgeo
        self.dom_positions = np.zeros((len(omgeo), 3))
        for i, (_, om) in enumerate(omgeo.iteritems()):
            self.dom_positions[i, :] = np.array(om.position)
        if self.check_containment:
            self.setup_convex_hull()
        self.PushFrame(frame)

    def setup_convex_hull(self):
        points_for_convex_hull = np.zeros_like(self.dom_positions)
        if self.containment_padding > 0.:
            mean_pos = np.mean(self.dom_positions, axis=0)
            for i, pos in enumerate(self.dom_positions):
                v_dir = (pos - mean_pos)
                len_pos = np.sqrt(np.sum(v_dir**2))
                v_dir /= len_pos
                new_pos = pos + v_dir * self.containment_padding
                points_for_convex_hull[i] = new_pos
        self._convex_hull = ConvexHull(points_for_convex_hull)

    def SFrame(self, frame):
        if self.multiple_distance is not None:
            frame['MCMultipleDistanceCutValues'] = dataclasses.I3VectorDouble(
                self.multiple_distance)
        self.PushFrame(frame)

    def DAQ(self, frame):
        particle = frame['MCMuon']
        v_dir = np.array([particle.dir.x, particle.dir.y, particle.dir.z])
        v_pos = np.array(particle.pos)
        distances = np.linalg.norm(np.cross(v_dir, v_pos - self.dom_positions),
                                   axis=1)
        n_close_doms = np.sum(distances < self.threshold)
        n_relevant_doms = np.sum(distances < self.relevance_dist)
        frame['MCNCloseDoms'] = icetray.I3Int(n_close_doms)
        frame['MCNRelevantDoms'] = icetray.I3Int(n_relevant_doms)
        frame['MCDistanceNearestDOM'] = dataclasses.I3Double(np.min(distances))
        if self.threshold_doms < 1.:
            threshold_doms = n_relevant_doms * self.threshold_doms
        else:
            threshold_doms = self.threshold_doms
        if n_close_doms <= threshold_doms:
            frame['MCLowOversizeStream'] = icetray.I3Bool(True)
            if self.split_streams:
                frame.stop = self.q_stream
        else:
            frame['MCLowOversizeStream'] = icetray.I3Bool(False)
        if self.check_containment:
            is_contained = particle_is_inside(self._convex_hull, particle)
            frame['MCMuonIsContained'] = icetray.I3Bool(is_contained)
        if self.multiple_distance is not None:
            n_close_doms = [np.sum(distances < dist_i)
                            for dist_i in self.multiple_distance]
            frame['MCNCloseDomsMultipleDistances'] = dataclasses.I3VectorInt(
                n_close_doms)
        self.PushFrame(frame)
