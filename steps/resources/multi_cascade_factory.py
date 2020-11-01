from __future__ import division
import numpy as np
from scipy.optimize import minimize

from I3Tray import I3Tray, I3Units
from icecube import icetray, dataclasses

from . import geometry


class MultiCascadeFactory(icetray.I3ConditionalModule):
    def __init__(self, context):
        """Class to create and inject neutrino interactions.

        Parameters
        ----------
        context : TYPE
            Description
        """
        icetray.I3ConditionalModule.__init__(self, context)
        self.AddOutBox('OutBox')
        self.AddParameter(
            'n_cascades',
            'Number of cascades to inject along the particle '
            'direction.', 2)
        self.AddParameter(
            'azimuth_range',
            '[min, max] of primary azimuth in degree.', [0, 360])
        self.AddParameter(
            'zenith_range',
            '[min, max] of primary zenith in degree.', [0, 180])
        self.AddParameter(
            'sample_uniformly_on_sphere',
            'If True, zenith is sampled uniformly in cos(zenith)'
            ' which results in a uniform distribution on the '
            'sphere. If False, zenith is sampled uniformly in '
            'zenith which leads to more densly sampled points '
            'at the poles of the sphere', False)
        self.AddParameter(
            'primary_energy_range', '', [10000, 10000])
        self.AddParameter(
            'fractional_energy_in_hadrons_range',
            'Fraction of primary energy in hadrons', [0, 1.])
        self.AddParameter(
            'time_range', '[min, max] of vertex time in ns.',
            [9000, 12000])
        self.AddParameter(
            'x_range',
            '[min, max] of vertex x-coordinate in meters.',
            [-500, 500])
        self.AddParameter(
            'y_range',
            '[min, max] of vertex y-coordinate in meters.',
            [-500, 500])
        self.AddParameter(
            'z_range',
            '[min, max] of vertex z-coordinate in meters.',
            [-500, 500])
        self.AddParameter(
            'shift_vertex_distance',
            'If provided, the vertex will be shifted back along '
            'the direction of the hypothetical track until it '
            'is located at a distance of `shift_vertex_distance`'
            ' to the convex hull around IceCube. If there is '
            'no such point, a new vertex will be drawn.',
            None)
        self.AddParameter(
            'max_vertex_distance',
            'Maximum distance of vertex outside of convex hull '
            'around IceCube. If the drawn (and shifted) vertex '
            'is further outside of the convex hull than the '
            'specified amount, a new vertex position will be '
            'drawn.'
            'If max_vertex_distance is None, the sampled vertex '
            'position will be accepted regardless of its '
            'distance to the convex hull.'
            'Note: this setting should not be used in '
            'combination with `shift_vertex_distance`.',
            None)
        self.AddParameter(
            'max_track_distance',
            'Maximum distance of an infinite track starting at '
            'the (shifted) vertex to the convex hull around '
            'IceCube. If the closest approach of the track is '
            'further outside of the convex hull than the '
            'specified amount, a new vertex position will be '
            'drawn. If max_track_distance is None, the sampled '
            'vertex position will be accepted regardless of the '
            'track distance to the convex hull.',
            None)
        self.AddParameter(
            'convex_hull_distance_function',
            'This defines which convex hull distance function '
            'to use in case '
            'any options are provided that require this. '
            'Options are: "IceCube" or "DeepCore". In '
            'addition, a function f(pos) -> distance may be '
            'passed. The distance is positive if the point is '
            'outside of the convex hull and negative if inside',
            'IceCube')
        self.AddParameter(
            'cascade_distribution_mode',
            'Defines how cascades will be distributed along the '
            'track. Options: ["equidistant", "uniform"]',
            'uniform')
        self.AddParameter(
            'cascade_distance_range',
            'Defines the range of allowed distances for the '
            'additional cascades. The distance is relative to '
            'the primary cascade.',
            [0., 500.])
        self.AddParameter(
            'flavors',
            'List of neutrino flavors to simulate.',
            ['NuE'])
        self.AddParameter(
            'interaction_types',
            'List of interaction types to simulate: CC or NC',
            ['CC'])
        self.AddParameter(
            'random_service', '', None)
        self.AddParameter(
            'num_events', '', 1)
        self.AddParameter(
            'oversampling_factor',
            'Oversampling Factor to be used. Simulation is '
            'averaged over these many simulations.',
            None)
        self.AddParameter(
            'constant_vars',
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
        """Configures NeutrinoFactory.

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
        self.max_track_distance = self.GetParameter('max_track_distance')
        self.shift_vertex_distance = self.GetParameter('shift_vertex_distance')
        self.convex_hull_distance_function = \
            self.GetParameter('convex_hull_distance_function')
        self.flavors = self.GetParameter('flavors')
        self.num_flavors = len(self.flavors)
        self.interaction_types = self.GetParameter('interaction_types')
        self.num_interaction_types = len(self.interaction_types)
        self.random_service = self.GetParameter('random_service')
        self.num_events = self.GetParameter('num_events')
        self.oversampling_factor = self.GetParameter('oversampling_factor')
        if self.oversampling_factor is None:
            self.oversampling_factor = 1
        self.constant_vars = self.GetParameter('constant_vars')
        if self.constant_vars is None:
            self.constant_vars = []
        self.events_done = 0

        self.n_cascades = self.GetParameter('n_cascades')
        self.cascade_distribution_mode = self.GetParameter(
            'cascade_distribution_mode')
        self.cascade_distance_range = self.GetParameter(
          'cascade_distance_range')

        if isinstance(self.convex_hull_distance_function, str):
            if self.convex_hull_distance_function == 'IceCube':
                self.convex_hull_distance_function = \
                    geometry.distance_to_icecube_hull
            elif self.convex_hull_distance_function == 'DeepCore':
                self.convex_hull_distance_function = \
                    geometry.distance_to_deepcore_hull
            else:
                raise ValueError('Unknown option: {}'.format(
                    self.convex_hull_distance_function))
        elif not callable(self.convex_hull_distance_function):
            raise ValueError(
                'Provided convex hull distance function is not a callable')

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

        # vertex
        if 'vertex' in self.constant_vars:
            self.vertex = self._sample_vertex(
                zenith=self.zenith, azimuth=self.azimuth)

        if 'time' in self.constant_vars:
            self.vertex_time = \
                self.random_service.uniform(*self.time_range)*I3Units.ns

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

    def _find_point_on_track(self, vertex, zenith, azimuth, desired_distance,
                             forwards=True):
        """Find point on track whose distance to the convex hull is closest
        to the desired distance.

        Parameters
        ----------
        vertex : I3Position
            Vertex of the track.
        zenith : float
            Zenith angle of track in radians.
        azimuth : float
            Azimuth angle of track in radians.
        desired_distance : float
            Desired distance to convex hull. The minimizer will try to find
            the point on the track that is closest to this.
        forwards : bool, optional
            Search forward in time starting at the vertex if True.
            If False, search backward in time, e.g. before the vertex.

        Returns
        -------
        I3Position
            The point on the track that is closest to the desired distance.
        float
            The distance loss at the best fit point.
        """
        direction = dataclasses.I3Direction(zenith, azimuth)

        def get_signed_t(t):
            if forwards:
                t = np.abs(t)
            else:
                t = -np.abs(t)
            return t

        def distance_loss(t):
            """Distance of point on track at time t to convex hull"""

            pos = vertex + get_signed_t(t[0]) * direction
            distance_to_hull = self.convex_hull_distance_function(pos)
            return (distance_to_hull - desired_distance)**2

        result = minimize(distance_loss, x0=0., method='Nelder-Mead')
        result_pos = vertex + get_signed_t(result.x[0]) * direction
        return result_pos, result.fun

    def _sample_vertex(self, zenith, azimuth):
        """Sample a vertex

        The vertex must be within allowed distance of specified convex hull
        and an outgoing track must have less than the maximum allowed track
        distance to the convex hull.

        If `shift_vertex_distance` is provided, the drawn vertex will be
        shifted along the track until it is at the specified distance to
        the convex hull. If this is not possible, a new vertex is sampled.

        Returns
        -------
        TYPE
            Description
        """

        # vertex
        point_is_ok = False
        while not point_is_ok:

            # draw a vertex within specified range
            vertex_x = self.random_service.uniform(*self.x_range) * I3Units.m
            vertex_y = self.random_service.uniform(*self.y_range) * I3Units.m
            vertex_z = self.random_service.uniform(*self.z_range) * I3Units.m
            vertex = dataclasses.I3Position(
                            vertex_x * I3Units.m,
                            vertex_y * I3Units.m,
                            vertex_z * I3Units.m)

            # shift vertex to specified distance, abort if not possible
            if self.shift_vertex_distance is not None:
                vertex, dist_loss = self._find_point_on_track(
                    vertex, zenith, azimuth,
                    desired_distance=self.shift_vertex_distance,
                    forwards=False)
                if dist_loss > 1:
                    continue

            # check vertex distance to convex hull
            if self.max_vertex_distance is not None:
                dist = self.convex_hull_distance_function(vertex)
                if dist > self.max_vertex_distance:
                    continue

            # check track distance to convex hull
            if self.max_track_distance is not None:

                # try and find a smaller track distance
                pos, dist_loss = self._find_point_on_track(
                    vertex, zenith, azimuth,
                    desired_distance=self.max_track_distance - 1000,
                    forwards=True)
                dist = self.convex_hull_distance_function(pos)
                if dist > self.max_track_distance:
                    continue

            # everything is good
            point_is_ok = True

        return vertex

    def _get_direction(self):
        """Get or sample direction.
        """
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
        return zenith, azimuth

    def _get_vertex(self, zenith, azimuth):
        """Get or sample vertex.
        """
        if 'vertex' in self.constant_vars:
            vertex = self.vertex
        else:
            vertex = self._sample_vertex(zenith=zenith, azimuth=azimuth)

        if 'time' in self.constant_vars:
            vertex_time = self.vertex_time
        else:
            vertex_time = \
                self.random_service.uniform(*self.time_range)*I3Units.ns
        return vertex, vertex_time

    def _get_energy(self):
        """Get or sample energy.
        """
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
        return primary_energy, hadron_energy, daughter_energy

    def _get_flavor_and_int_type(self):
        """Get or sample flavor and interaction type.
        """
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
        return flavor, interaction_type

    def _create_particles(
            self,
            zenith,
            azimuth,
            vertex,
            vertex_time,
            primary_energy,
            daughter_energy,
            hadron_energy,
            interaction_type,
            flavor,
            ):
        """Create primary and daughter particles
        """
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

        return primary, daughter, hadrons

    def _get_sub_cascade_vertices(self, vertex, vertex_time, zenith, azimuth):
        """Get vertices of sub cascades.

        Parameters
        ----------
        vertex : I3Position
            The position of the primary cascade.
        vertex_time : float
            The time of the primary vertex.
        zenith : float
            The zenith angle of the primary cascade.
        azimuth : float
            The azimuth angle of the primary cascade.

        Returns
        -------
        list of I3Position
            List of vertices of sub-cascades.
        list of float
            List of verticec times of sub-cascades.
        """

        # sample distances of cascades
        if self.cascade_distribution_mode == 'equidistant':
            distances = np.linspace(
               self.cascade_distance_range[0],
               self.cascade_distance_range[1],
               self.n_cascades,
            )[1:]
        elif self.cascade_distribution_mode == 'uniform':
            distances = []
            for i in range(1, self.n_cascades):
                distance_i = self.random_service.uniform(
                   self.cascade_distance_range[0],
                   self.cascade_distance_range[1],
                )
                distances.append(distance_i)
        else:
            raise ValueError('Unknown distance distribution mode: {}'.format(
                self.cascade_distribution_mode))

        assert len(distances) == self.n_cascades - 1, distances

        direction = dataclasses.I3Direction(zenith, azimuth)
        vertices = []
        times = []
        for dist in distances:
            vertices.append(vertex + dist * direction)
            times.append(vertex_time + dist / dataclasses.I3Constants.c)

        return vertices, times

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

        # direction
        zenith, azimuth = self._get_direction()

        # vertex
        vertex, vertex_time = self._get_vertex(zenith=zenith, azimuth=azimuth)

        # energy
        primary_energy, hadron_energy, daughter_energy = self._get_energy()

        # flavor and interaction
        flavor, interaction_type = self._get_flavor_and_int_type()

        # create pseduo I3MCWeightDict
        mc_dict = {}
        if interaction_type == 'cc':
            # Charged Current Interaction: 1
            mc_dict['InteractionType'] = 1
        else:
            # Neutral Current Interaction: 2
            mc_dict['InteractionType'] = 2
        frame['I3MCWeightDict'] = dataclasses.I3MapStringDouble(mc_dict)

        # create primary cascade interaction
        primary, daughter, hadrons = self._create_particles(
            zenith=zenith,
            azimuth=azimuth,
            vertex=vertex,
            vertex_time=vertex_time,
            primary_energy=primary_energy,
            daughter_energy=daughter_energy,
            hadron_energy=hadron_energy,
            interaction_type=interaction_type,
            flavor=flavor,
        )

        # -------------------------
        # get sub-cascade particles
        # -------------------------
        sub_primaries = []
        sub_daughters = []
        sub_hadrons = []

        vertices, times = self._get_sub_cascade_vertices(
            vertex=vertex,
            vertex_time=vertex_time,
            zenith=zenith,
            azimuth=azimuth,
        )
        for vertex_j, vertex_time_j in zip(vertices, times):
            primary_energy_j, hadron_energy_j, daughter_energy_j = \
                self._get_energy()
            flavor_j, interaction_type_j = self._get_flavor_and_int_type()
            primary_j, daughter_j, hadrons_j = self._create_particles(
                zenith=zenith,
                azimuth=azimuth,
                vertex=vertex_j,
                vertex_time=vertex_time_j,
                primary_energy=primary_energy_j,
                daughter_energy=daughter_energy_j,
                hadron_energy=hadron_energy_j,
                interaction_type=interaction_type_j,
                flavor=flavor_j,
            )
            sub_primaries.append(primary_j)
            sub_daughters.append(daughter_j)
            sub_hadrons.append(hadrons_j)
        # -------------------------

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

            # ----------------------------
            # now add additional cascades:
            # ----------------------------
            for primary_j, daughter_j, hadrons_j in zip(
                    sub_primaries, sub_daughters, sub_hadrons):
                primary_copy_j = dataclasses.I3Particle(primary_j)
                mctree.add_primary(primary_copy_j)
                mctree.append_child(
                    primary_copy_j, dataclasses.I3Particle(daughter_j))
                mctree.append_child(
                    primary_copy_j, dataclasses.I3Particle(hadrons_j))
            # ----------------------------

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
