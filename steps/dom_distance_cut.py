import warnings
import numpy as np

from icecube import phys_services, icetray, dataclasses, MuonGun


def get_numu_particles(frame, numu):
    particles = []

    def crawl(parent):
        for p in frame['I3MCTree'].get_daughters(parent):
            if p.type == p.NuMu or p.type == p.NuMuBar:
                crawl(p)
            elif p.type in [p.Hadrons, p.MuMinus, p.MuPlus]:
                if p.location_type == p.LocationType.InIce:
                    particles.append(p)

    crawl(numu)
    return particles


def get_nue_particles(frame, nue):
    particles = []

    def crawl(parent):
        for p in frame['I3MCTree'].get_daughters(parent):
            if p.type == p.NuE or p.type == p.NuEBar:
                crawl(p)
            elif p.type in [p.Hadrons]:
                if p.location_type == p.LocationType.InIce:
                    particles.append(p)

    crawl(nue)
    return particles



def is_infront_of_point(v_dir, v_pos, points):
    a = np.dot(v_pos, v_dir) * -1.
    dist_plain = np.dot(points, v_dir) + a
    return dist_plain > 0


def get_muon_v_stop(frame, muon):
    v_stop = None
    for t in frame['MMCTrackList']:
        if t.particle.id == muon.id:
            if t.Ef < 0:
            # For stopping muons, the negative length in 
            # meter is saved in Ef: 
            # Ef = - length [meter]
                v_stop = muon.pos + muon.dir * muon.length
    return np.array(v_stop)


class OversizeStream(object):
    def __init__(self,
                 distance_cut,
                 dom_limit,
                 oversize_factor):
        if not isinstance(dom_limit, int) or not isinstance(dom_limit, float):
            self.distance_cut = distance_cut
        else:
            raise ValueError("distance_cut has to be provided as float or int")

        if not isinstance(oversize_factor, int) or not \
                isinstance(oversize_factor, float):
            self.oversize_factor = oversize_factor
        else:
            raise ValueError(
                "oversize_factor has to be provided as float or int")

        if not isinstance(dom_limit, int) or not isinstance(dom_limit, float):
            self.dom_limit = dom_limit
        else:
            if dom_limit is None:
                warnings.warn("'dom_limit' was set to None using default: 1")
                dom_limit = 1
            else:
                raise TypeError("dom_limit has to be int or float")

        self._stream_id = None
        self.stream_name = None
        self.file_addition = None

    @property
    def stream_id(self):
        if self._steam_id is None:
            raise RuntimeError('No stream_id set!')
        else:
            return self._stream_id

    @stream_id.setter
    def stream_id(self, value):
        if not isinstance(value, int):
            raise TypeError('stream_id has to be int!')
        else:
            if value < -1:
                raise ValueError('stream_id has greater than -2!')
            else:
                self._stream_id = value
        if self._stream_id == -1:
            self.stream_name = 'MCOversizeStreamDefault'
            self.file_addition = 'OversizeStreamDefault'
        else:
            self.stream_name = 'MCOversizeStream{}'.format(self._stream_id)
            self.file_addition = 'OversizeStream{}'.format(self._stream_id)

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

    def __lt__(self, other):
        if isinstance(other, OversizeStream):
            other = other.distance_cut
        if isinstance(other, float) or isinstance(other, int):
            if self.distance_cut < 0:
                return False
            elif other < 0:
                return True
            else:
                return self.distance_cut < other

    def __str__(self):
        s = '{} - Id: {}; Distance: {}; DOM limit: {}; Factor {}'
        s = s.format(self.stream_name,
                     self._stream_id,
                     self.distance_cut,
                     self.dom_limit,
                     self.oversize_factor)
        return s

    def __repr__(self):
        return self.__str__()

    def transform_filepath(self, filepath):
        return filepath.replace('i3.bz2',
                                '{}.i3.bz2'.format(self.file_addition))

      
def generate_stream_object(cut_distances, dom_limits, oversize_factors):
    cut_distances = np.atleast_1d(cut_distances)
    dom_limits = np.atleast_1d(dom_limits)
    oversize_factors = np.atleast_1d(oversize_factors)

    if np.sum(cut_distances < 0) > 1:
        raise ValueError('More than one default stream provided!')
    if len(cut_distances) != len(dom_limits):
         if len(dom_limits) == 1:
             dom_limits = np.ones_like(cut_distances) * dom_limits
         else:
             raise ValueError('Provide either a DOM limit for each distance'
                              ' or one for all!')
    if len(oversize_factors) != len(cut_distances):
        raise ValueError('You should provide a oversize factor for split!')

    stream_objects = []
    for dist_i, lim_i, factor_i in zip(cut_distances,
                                       dom_limits,
                                       oversize_factors):
        stream_objects.append(
            OversizeStream(distance_cut=dist_i,
                           dom_limit=lim_i,
                           oversize_factor=factor_i))
    stream_objects = sorted(stream_objects)
    stream_id = 0
    for stream_i in stream_objects:
        if stream_i.distance_cut > 0:
            stream_i.stream_id = stream_id
            stream_id += 1
        else:
            stream_i.stream_id = -1
    return stream_objects


class OversizeSplitterNSplits(icetray.I3ConditionalModule):
    S_stream = icetray.I3Frame.Stream('S')
    supported_simulations = ['muongun', 'numu', 'nue']

    def __init__(self, context):
        icetray.I3ConditionalModule.__init__(self, context)
        self.AddParameter('thresholds',
                          'Cut distance',
                          [10])
        self.AddParameter('thresholds_doms',
                          'Treshold for too many close DOMs',
                          1)
        self.AddParameter(
            'oversize_factors',
            'Oversize_factors used in case of too many close DOMs',
            None)
        self.AddParameter('relevance_dist',
                          'Max distance to cosinder a DOM as relevant',
                          None)
        self.AddParameter('simulaton_type',
                          'Specify type of simulation. Currently available '
                          '["muongun", "numu", "nue"]',
                          'muongun')

    def Configure(self):
        self.stream_objects = generate_stream_object(
            cut_distances=self.GetParameter('thresholds'),
            dom_limits=self.GetParameter('thresholds_doms'),
            oversize_factors=self.GetParameter('oversize_factors'))
        self.thresholds = np.zeros(len(self.stream_objects), dtype=float)
        self.lim_doms = np.zeros_like(self.thresholds)
        self.oversize_factors = np.zeros_like(self.thresholds)
        for i, stream_i in enumerate(self.stream_objects):
            self.thresholds[i] = stream_i.distance_cut
            self.lim_doms[i] = stream_i.dom_limit
            self.oversize_factors[i] = stream_i.oversize_factor

        self.simulation_type = self.GetParameter('simulaton_type').lower()
        if self.simulation_type not in self.supported_simulations:
            s = ', '.join(self.supported_simulations)
            raise AttributeError(
                'Unsupported simulation type! Available: [{}]'.format(s))

        if any(self.thresholds == -1.):
            self.default_idx = np.where(self.thresholds == -1.)[0][0]
        else:
            self.default_idx = None

            self.relevance_dist = self.GetParameter('relevance_dist')

        self.Register(self.S_stream, self.SFrame)

    def Geometry(self, frame):
        omgeo = frame['I3Geometry'].omgeo
        self.dom_positions = np.zeros((len(omgeo), 3))
        for i, (_, om) in enumerate(omgeo.iteritems()):
            self.dom_positions[i, :] = np.array(om.position)
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

    def get_distances(self,
                      frame,
                      particle,
                      check_starting=False,
                      check_stopping=False):
        v_dir = np.array([particle.dir.x, particle.dir.y, particle.dir.z])
        v_pos = np.array(particle.pos)
        if particle.type == particle.Hadrons:
            v_pos = np.array(particle.pos)
            return np.linalg.norm(v_pos - self.dom_positions, axis=1)
        elif particle.type in [particle.MuMinus, particle.MuPlus]:
            distances = np.linalg.norm(
                np.cross(v_dir,v_pos - self.dom_positions),
                axis=1)
            if check_starting:
                is_infront = is_infront_of_point(v_dir,
                                                 v_pos,
                                                 self.dom_positions)
                distances[~is_infront] = np.linalg.norm(
                    v_pos - self.dom_positions[~is_infront, :],
                    axis=1)
            if check_stopping:
                v_stop = get_muon_v_stop(frame, particle)
                if v_stop is not None:
                    is_infront = is_infront_of_point(v_dir,
                                                     v_stop,
                                                     self.dom_positions)
                    distances[is_infront] = np.linalg.norm(
                        v_stop - self.dom_positions[is_infront, :],
                        axis=1)
        return distances

    def DAQ(self, frame):
        if self.simulation_type == 'muongun':
            particle_list = [frame['MCMuon']]
            check_starting = False
            check_stopping = False
        elif self.simulation_type == 'nue':
            particle_list = get_nue_particles(frame, frame['NuGPrimary'])
            check_starting = False
            check_stopping = False
        elif self.simulation_type == 'numu':
            particle_list = get_numu_particles(frame, frame['NuGPrimary'])
            check_starting = True
            check_stopping = True

        stream_list = []
        for p in particle_list:
            distances = self.get_distances(
                frame,
                p,
                check_starting=check_starting,
                check_stopping=check_stopping)

            if self.relevance_dist is not None:
                n_relevant_doms = distances < self.relevance_dist
            else:
                n_relevant_doms = self.dom_positions.shape[0]

            for i, stream_i in enumerate(self.stream_objects):
                if stream_i.dom_limit < 1.:
                    limit_i = n_relevant_doms * stream_i.dom_limit
                else:
                    limit_i = stream_i.dom_limit
                if np.sum(distances < stream_i.distance_cut) >= limit_i:
                    stream_list.append(stream_i)
            if self.default_idx is not None:
                stream_list.append(self.stream_objects[self.default_idx])

        selected_stream = None
        lowest_oversize = None
        for i, stream_i in enumerate(stream_list):
            if lowest_oversize is None:
                lowest_oversize = stream_i.oversize_factor
                selected_stream = stream_i
            else:
                if stream_i.oversize_factor < lowest_oversize:
                    selected_stream = stream_i
                    lowest_oversize = stream_i.oversize_factor

        for stream_i in self.stream_objects:
            if stream_i is selected_stream:
                frame[stream_i.stream_name] = icetray.I3Bool(True)
            else:
                frame[stream_i.stream_name] = icetray.I3Bool(False)
        self.PushFrame(frame)

