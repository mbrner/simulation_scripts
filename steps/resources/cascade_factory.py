from __future__ import division
import numpy as np

from I3Tray import I3Tray, I3Units
from icecube import icetray, dataclasses

from . import geometry


class CascadeFactory(icetray.I3ConditionalModule):
    def __init__(self, context):
        """Class to create and inject Cascades.

        Parameters
        ----------
        context : TYPE
            Description
        """
        icetray.I3ConditionalModule.__init__(self, context)
        self.AddOutBox('OutBox')
        self.AddParameter('azimuth_range',
                          '[min, max] of primary azimuth in degree.', [0, 360])
        self.AddParameter('zenith_range',
                          '[min, max] of primary zenith in degree.', [0, 180])
        self.AddParameter('sample_uniformly_on_sphere',
                          'If True, zenith is sampled uniformly in cos(zenith)'
                          ' which results in a uniform distribution on the '
                          'sphere. If False, zenith is sampled uniformly in '
                          'zenith which leads to more densly sampled points '
                          'at the poles of the sphere', False)
        self.AddParameter('primary_energy_range', '', [10000, 10000])
        self.AddParameter('fractional_energy_in_hadrons_range',
                          'Fraction of primary energy in hadrons', [0, 1.])
        self.AddParameter('time_range', '[min, max] of vertex time in ns.',
                          [9000, 12000])
        self.AddParameter('x_range',
                          '[min, max] of vertex x-coordinate in meters.',
                          [-500, 500])
        self.AddParameter('y_range',
                          '[min, max] of vertex y-coordinate in meters.',
                          [-500, 500])
        self.AddParameter('z_range',
                          '[min, max] of vertex z-coordinate in meters.',
                          [-500, 500])
        self.AddParameter('max_vertex_distance',
                          'Maximum distance of vertex outside of convex hull '
                          'around IceCube. If the drawn vertex is further '
                          'outside of the convex hull than the specified '
                          'amount, a new vertex position will be drawn.'
                          'If max_vertex_distance is None, the sampled vertex '
                          'position will be accepted regardless of its '
                          'distance to the convex hull.',
                          None)
        self.AddParameter('flavors',
                          'List of neutrino flavors to simulate.',
                          ['NuE', 'NuMu', 'NuTau'])
        self.AddParameter('interaction_types',
                          'List of interaction types to simulate: CC or NC',
                          ['CC', 'NC'])
        self.AddParameter('random_state', '', 1337)
        self.AddParameter('random_service', '', None)
        self.AddParameter('num_events', '', 1)
        self.AddParameter('oversampling_factor',
                          'Oversampling Factor to be used. Simulation is '
                          'averaged over these many simulations.',
                          None)
        self.AddParameter('constant_vars',
                          'These variables are only sampled once when the '
                          'module is being configured. They are kept constant '
                          'afterwards. This can for instance be used to keep '
                          'certain parameters such as the direction constant '
                          'for events of a run. Allowed options are: '
                          'vertex, zenith, azimuth, primary_energy, flavor'
                          'fractional_energy_in_hadrons, time, '
                          'interaction_type. The variables must be passed as '
                          'a list of str',
                          None)

    def Configure(self):
        """Configures CascadeFactory.

        Raises
        ------
        ValueError
            If interaction type or flavor is unkown.
        """
        self.azimuth_range = self.GetParameter('azimuth_range')
        self.zenith_range = self.GetParameter('zenith_range')
        self.sample_in_cos = self.GetParameter('sample_uniformly_on_sphere')
        self.cos_zenith_range = [np.cos(np.deg2rad(self.zenith_range[1])),
                                 np.cos(np.deg2rad(self.zenith_range[0]))]
        self.primary_energy_range = self.GetParameter('primary_energy_range')
        self.log_primary_energy_range = [
                                    np.log10(self.primary_energy_range[0]),
                                    np.log10(self.primary_energy_range[1])]
        self.fractional_energy_in_hadrons_range = self.GetParameter(
                                        'fractional_energy_in_hadrons_range')
        self.time_range = self.GetParameter('time_range')
        self.x_range = self.GetParameter('x_range')
        self.y_range = self.GetParameter('y_range')
        self.z_range = self.GetParameter('z_range')
        self.max_vertex_distance = self.GetParameter('max_vertex_distance')
        self.flavors = self.GetParameter('flavors')
        self.num_flavors = len(self.flavors)
        self.interaction_types = self.GetParameter('interaction_types')
        self.num_interaction_types = len(self.interaction_types)
        self.random_state = self.GetParameter('random_state')
        self.random_service = self.GetParameter('random_service')
        if not isinstance(self.random_state, np.random.RandomState):
            self.random_state = np.random.RandomState(self.random_state)
        self.num_events = self.GetParameter('num_events')
        self.oversampling_factor = self.GetParameter('oversampling_factor')
        if self.oversampling_factor is None:
            self.oversampling_factor = 1
        if self.max_vertex_distance is None:
            self.max_vertex_distance = float('inf')
        self.constant_vars = self.GetParameter('constant_vars')
        if self.constant_vars is None:
            self.constant_vars = []
        self.events_done = 0

        # make lowercase
        self.flavors = [f.lower() for f in self.flavors]
        self.constant_vars = [f.lower() for f in self.constant_vars]
        self.interaction_types = [i.lower() for i in self.interaction_types]

        # --------------
        # sanity checks:
        # --------------
        for const_var in self.constant_vars:
            if const_var not in ['vertex', 'zenith', 'azimuth', 'time',
                                 'primary_energy', 'flavor',
                                 'fractional_energy_in_hadrons',
                                 'interaction_type']:
                raise ValueError('Var unknown: {!r}'.format(const_var))

        for int_type in self.interaction_types:
            if int_type not in ['cc', 'nc']:
                raise ValueError('Interaction unknown: {!r}'.format(int_type))

        for flavor in self.flavors:
            if flavor not in ['nue', 'numu', 'nutau']:
                raise ValueError('Flavor unknown: {!r}'.format(flavor))

        if self.oversampling_factor < 1:
            raise ValueError('Oversampling must be set to "None" or integer'
                             ' greater than 1. It is currently set to: '
                             '{!r}'.format(self.oversampling_factor))
        # --------------------
        # sample constant vars
        # --------------------
        # vertex
        if 'vertex' in self.constant_vars:
            self.vertex = self._sample_vertex()

        if 'time' in self.constant_vars:
            self.vertex_time = \
                self.random_service.uniform(*self.time_range)*I3Units.ns

        # direction
        if 'azimuth' in self.constant_vars:
            self.azimuth = \
                self.random_service.uniform(*self.azimuth_range)*I3Units.deg
        if 'zenith' in self.constant_vars:
            if self.sample_in_cos:
                zenith = np.rad2deg(np.arccos(
                    self.random_service.uniform(*self.cos_zenith_range)))
            else:
                zenith = self.random_service.uniform(*self.zenith_range)
            self.zenith = zenith*I3Units.deg

        # energy
        if 'primary_energy' in self.constant_vars:
            self.log_primary_energy = self.random_service.uniform(
                                *self.log_primary_energy_range) * I3Units.GeV
        if 'fractional_energy_in_hadrons' in self.constant_vars:
            self.fraction = self.random_service.uniform(
                                    *self.fractional_energy_in_hadrons_range)

        # flavor and interaction
        if 'flavor' in self.constant_vars:
            self.flavor = \
                self.flavors[self.random_service.integer(self.num_flavors)]
        if 'interaction_type' in self.constant_vars:
            self.interaction_type = self.interaction_types[
                    self.random_service.integer(self.num_interaction_types)]
        # --------------------

    def _sample_vertex(self):
        """Sample a vertex within allowd distance of IceCube Convex Hull.

        Returns
        -------
        TYPE
            Description
        """
        # vertex
        point_is_inside = False
        while not point_is_inside:
            vertex_x = self.random_service.uniform(*self.x_range) * I3Units.m
            vertex_y = self.random_service.uniform(*self.y_range) * I3Units.m
            vertex_z = self.random_service.uniform(*self.z_range) * I3Units.m
            vertex = dataclasses.I3Position(
                            vertex_x * I3Units.m,
                            vertex_y * I3Units.m,
                            vertex_z * I3Units.m)
            dist = geometry.distance_to_icecube_hull(vertex)
            point_is_inside = dist < self.max_vertex_distance
        return vertex

    def DAQ(self, frame):
        """Inject casacdes into I3MCtree.

        Parameters
        ----------
        frame : icetray.I3Frame.DAQ
            An I3 q-frame.

        Raises
        ------
        ValueError
            If interaction type is unknown.
        """
        # --------------
        # sample cascade
        # --------------
        # vertex
        if 'vertex' in self.constant_vars:
            vertex = self.vertex
        else:
            vertex = self._sample_vertex()

        if 'time' in self.constant_vars:
            vertex_time = self.vertex_time
        else:
            vertex_time = \
                self.random_service.uniform(*self.time_range)*I3Units.ns

        # direction
        if 'azimuth' in self.constant_vars:
            azimuth = self.azimuth
        else:
            azimuth = \
                self.random_service.uniform(*self.azimuth_range)*I3Units.deg
        if 'zenith' in self.constant_vars:
            zenith = self.zenith
        else:
            if self.sample_in_cos:
                zenith = np.rad2deg(np.arccos(
                    self.random_service.uniform(*self.cos_zenith_range)))
            else:
                zenith = self.random_service.uniform(*self.zenith_range)
            zenith = zenith*I3Units.deg

        # energy
        if 'primary_energy' in self.constant_vars:
            log_primary_energy = self.log_primary_energy
        else:
            log_primary_energy = self.random_service.uniform(
                                *self.log_primary_energy_range) * I3Units.GeV
        primary_energy = 10**log_primary_energy
        if 'fractional_energy_in_hadrons' in self.constant_vars:
            fraction = self.fraction
        else:
            fraction = self.random_service.uniform(
                                    *self.fractional_energy_in_hadrons_range)
        hadron_energy = primary_energy * fraction
        daughter_energy = primary_energy - hadron_energy

        # flavor and interaction
        if 'flavor' in self.constant_vars:
            flavor = self.flavor
        else:
            flavor = \
                self.flavors[self.random_service.integer(self.num_flavors)]
        if 'interaction_type' in self.constant_vars:
            interaction_type = self.interaction_type
        else:
            interaction_type = self.interaction_types[
                    self.random_service.integer(self.num_interaction_types)]

        # create pseduo I3MCWeightDict
        mc_dict = {}
        if interaction_type == 'cc':
            # Charged Current Interaction: 1
            mc_dict['InteractionType'] = 1
        else:
            # Neutral Current Interaction: 2
            mc_dict['InteractionType'] = 2
        frame['I3MCWeightDict'] = dataclasses.I3MapStringDouble(mc_dict)

        # create particle
        primary = dataclasses.I3Particle()
        daughter = dataclasses.I3Particle()

        primary.time = vertex_time * I3Units.ns
        primary.dir = dataclasses.I3Direction(zenith, azimuth)
        primary.energy = primary_energy * I3Units.GeV
        primary.pos = vertex
        primary.speed = dataclasses.I3Constants.c
        # Assume the vertex position in range is in ice, so the primary is the
        # in ice neutrino that interacts
        primary.location_type = dataclasses.I3Particle.LocationType.InIce
        daughter.location_type = dataclasses.I3Particle.LocationType.InIce

        daughter.time = primary.time
        daughter.dir = primary.dir
        daughter.speed = primary.speed
        daughter.pos = primary.pos
        daughter.energy = daughter_energy * I3Units.GeV

        if interaction_type == 'cc' and flavor == 'numu':
            daughter.shape = dataclasses.I3Particle.InfiniteTrack
        else:
            daughter.shape = dataclasses.I3Particle.Cascade

        if flavor == 'numu':
            primary.type = dataclasses.I3Particle.ParticleType.NuMu
            if interaction_type == 'cc':
                daughter.type = dataclasses.I3Particle.ParticleType.MuMinus
            elif interaction_type == 'nc':
                daughter.type = dataclasses.I3Particle.ParticleType.NuMu
        elif flavor == 'nutau':
            primary.type = dataclasses.I3Particle.ParticleType.NuTau
            if interaction_type == 'cc':
                daughter.type = dataclasses.I3Particle.ParticleType.TauMinus
            elif interaction_type == 'nc':
                daughter.type = dataclasses.I3Particle.ParticleType.NuTau
        elif flavor == 'nue':
            primary.type = dataclasses.I3Particle.ParticleType.NuE
            if interaction_type == 'cc':
                daughter.type = dataclasses.I3Particle.ParticleType.EMinus
            elif interaction_type == 'nc':
                daughter.type = dataclasses.I3Particle.ParticleType.NuE
        else:
            raise ValueError(('particle_type {!r} not known or not ' +
                              'implemented'.format(self.particle_type)))

        # add hadrons
        hadrons = dataclasses.I3Particle()
        hadrons.energy = hadron_energy * I3Units.GeV
        hadrons.pos = daughter.pos
        hadrons.time = daughter.time
        hadrons.dir = daughter.dir
        hadrons.speed = daughter.speed
        hadrons.type = dataclasses.I3Particle.ParticleType.Hadrons
        hadrons.location_type = daughter.location_type
        hadrons.shape = dataclasses.I3Particle.Cascade

        # oversampling
        for i in range(self.oversampling_factor):
            if i > 0:
                # create a new frame
                frame = icetray.I3Frame(frame)
                del frame['I3MCTree_preMuonProp']
                del frame['oversampling']

            # Fill primary and daughter particles into a MCTree
            primary_copy = dataclasses.I3Particle(primary)
            mctree = dataclasses.I3MCTree()
            mctree.add_primary(primary_copy)
            mctree.append_child(primary_copy, dataclasses.I3Particle(daughter))
            mctree.append_child(primary_copy, dataclasses.I3Particle(hadrons))

            frame['I3MCTree_preMuonProp'] = mctree
            if self.oversampling_factor > 1:
                frame['oversampling'] = dataclasses.I3MapStringInt({
                                        'event_num_in_run': self.events_done,
                                        'oversampling_num': i,
                                    })
            self.PushFrame(frame)

        self.events_done += 1
        if self.events_done >= self.num_events:
            self.RequestSuspension()
