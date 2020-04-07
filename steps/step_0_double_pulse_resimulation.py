#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
#METAPROJECT /home/mmeier/combo_test/build
# use self-build combo until the I3CrossSection pybindings are included
# in a release
from __future__ import division
import click
import yaml
import copy

import numpy as np

from icecube.simprod import segments

from I3Tray import I3Tray, I3Units
from icecube import icetray, dataclasses
from icecube import sim_services, MuonGun

from utils import create_random_services, get_run_folder

import healpy
import os
from scipy import interpolate
from collections import Iterable


def logsumexp(a, axis=None, b=None):
    a = np.asarray(a)
    if axis is None:
        a = a.ravel()
    else:
        a = np.rollaxis(a, axis)
    a_max = a.max(axis=0)
    if b is not None:
        b = np.asarray(b)
        if axis is None:
            b = b.ravel()
        else:
            b = np.rollaxis(b, axis)
        out = np.log(np.sum(b * np.exp(a - a_max), axis=0))
    else:
        out = np.log(np.sum(np.exp(a - a_max), axis=0))
    out += a_max
    return out


def getSmearedMap(filename, sigmaInDeg=30., eventName=None):
    data = np.load(filename)
    logls = -data['logl']
    energies = data['energy']
    posX = data['x']
    posY = data['y']
    posZ = data['z']
    time = data['t']
    del data

    log_normalization = logsumexp(logls)
    log_probs = logls - log_normalization
    del logls

    probs = np.exp(log_probs)

    probs_smooth = healpy.smoothing(probs, sigma=sigmaInDeg*3.14/180.)
    probs_smooth = np.where(probs_smooth >= 1e-100, probs_smooth, 1e-100)
    log_probs_smooth = np.log(probs_smooth)

    probs_smooth = np.exp(log_probs_smooth - logsumexp(log_probs_smooth))

    if eventName is None:
        name = filename
    else:
        name = eventName

    return dict(
        probs_smooth=probs_smooth,
        energies=energies,
        posX=posX,
        posY=posY,
        posZ=posZ,
        time=time,
        filename=name)


def sampleFromMap(mapDict, random_state,
                  ptype='nutau', pos_sigma=5.,
                  **kwargs):
    if not isinstance(random_state, np.random.RandomState):
        random_state = np.random.RandomState(random_state)

    nside = healpy.npix2nside(len(mapDict["probs_smooth"]))

    sampled_index = random_state.choice(
        np.arange(len(mapDict['probs_smooth'])),
        p=mapDict['probs_smooth'],
        size=1)[0]
    dir_zen, dir_azi = healpy.pix2ang(nside, sampled_index)

    energy = mapDict["energies"][sampled_index]
    posX = mapDict["posX"][sampled_index]
    posY = mapDict["posY"][sampled_index]
    posZ = mapDict["posZ"][sampled_index]
    time = mapDict["time"][sampled_index]

    if ptype == 'nutau':
        e_min = energy
        e_max = e_min + 10e3

        energy = random_state.uniform(e_min, e_max)

    elif ptype == 'numu':
        e_min = energy
        e_max = kwargs.get('e_max', 1e6)

        energy = random_state.uniform(e_min, e_max)

    else:
        raise ValueError(('ptype {} either unknown or not' +
                         'implemented').format(ptype))

    vshift = random_state.normal(loc=0, scale=pos_sigma, size=3)

    return dict(
        zenith=dir_zen,
        azimuth=dir_azi,
        energy=energy,
        posX=posX + vshift[0],
        posY=posY + vshift[1],
        posZ=posZ + vshift[2],
        time=time)


class energy_loss(object):
    def __init__(self, energy):
        from icecube.icetray import I3Units

        self.a = 2.03 + 0.604 * np.log(energy/I3Units.GeV)
        self.b = 0.633
        self.lrad = (35.8*I3Units.cm/0.9216)

    def __call__(self, d):
        """Return the *unnormalize* energy loss d meters from the vertex"""
        x = d/self.lrad
        return (x**(self.a-1))*np.exp(-self.b*x)

    @property
    def max(self):
        return ((self.a-1.)/self.b)*self.lrad


class InterpolatedCrossSection():
    '''
    Load cross section tables and interpolate them.
    '''
    def __init__(self, xsec_path, interp_type):
        if not os.path.isfile(xsec_path):
            raise IOError('File not found at {}!'.format(
                xsec_path))
        self._xsec_path = xsec_path
        self._energy = np.logspace(1, 12, 111)
        self._y, self._xsec = self.__load_file__(xsec_path)

        if interp_type not in ['linear', 'spline']:
            raise ValueError('`interp_type` has to be linear or spline.')
        if interp_type == 'spline':
            raise NotImplementedError
        elif interp_type == 'linear':
            self._interp = self.__build_lin_interp__()

    def __load_file__(self, xsec_path):
        xsec_f = np.loadtxt(xsec_path)
        y_values = xsec_f[:, 0]
        xsec_values = xsec_f[:, 1]
        return y_values, xsec_values

    def __build_lin_interp__(self):
        '''
        Build the interpolator object on the energy and y grid.
        Interpolate in log scale to make sure that the interpolator
        doesnt get in trouble.
        '''
        energy_grid_v = np.repeat(np.log10(self._energy), 100)
        y_grid_v = np.log10(self._y)
        xsec_values = np.log10(self._xsec)
        interp = interpolate.LinearNDInterpolator((energy_grid_v, y_grid_v),
                                                  xsec_values,
                                                  fill_value=0)
        return interp

    def __call__(self, e_log, y_log):
        '''
        Call the interpolator with a logarithmic energy and logarithmic y.
        '''
        if isinstance(e_log, Iterable) and isinstance(y_log, Iterable):
            assert len(e_log) == len(y_log), \
                'shape of flattened `e_log` and `y_log` should be the same!'
        elif isinstance(e_log, Iterable) or isinstance(y_log, Iterable):
            if isinstance(e_log, Iterable):
                y_log = np.repeat(y_log, len(e_log))
            elif isinstance(y_log, Iterable):
                e_log = np.repeat(e_log, len(y_log))
        return 10**self._interp(e_log, y_log)

    def sample_y(self, e_log, y_log=None, n_samples=1, random_state=None):
        if not isinstance(random_state, np.random.RandomState):
            random_state = np.random_state.RandomState(random_state)

        if y_log is None:
            e_nu = 10**e_log
            proton_mass = 0.938  # in GeV
            # y_min = Q^2_min / s with s = 2*mp*Enu
            # Q^2_min is taken to be 1 GeV^2, below this threshold
            # perturbative QCD no longer applies.
            y_min = 1 / (2 * proton_mass * e_nu)
            y_log = np.logspace(np.log10(y_min), np.log10(1), 101)

        xsec = self.__call__(e_log, y_log)
        xsec_max = np.max(xsec)

        rand_uni = random_state.uniform(size=(n_samples, 2))
        xsec_at_u = self.__call__(e_log, np.log10(rand_uni[:, 0]))
        failed_mask = rand_uni[:, 1] > (xsec_at_u / xsec_max)
        n_failed = np.sum(failed_mask)
        while n_failed != 0:
            new_rand_uni = random_state.uniform(size=(n_failed, 2))
            rand_uni[failed_mask] = new_rand_uni
            xsec_at_u[failed_mask] = self.__call__(
                e_log,
                np.log10(new_rand_uni[:, 0]))
            failed_mask = rand_uni[:, 1] > (xsec_at_u / xsec_max)
            n_failed = np.sum(failed_mask)

        if n_samples == 1:
            return rand_uni[0, 0]
        else:
            return rand_uni[:, 0]


class ParticleFactory(icetray.I3ConditionalModule):
    def __init__(self, context):
        icetray.I3ConditionalModule.__init__(self, context)
        self.AddOutBox('OutBox')
        self.AddParameter('particle_type', '', None)
        self.AddParameter('map_filename', '', None)
        self.AddParameter('smearing_angle', '', 30.*I3Units.deg)
        self.AddParameter('event_name', '', None)
        self.AddParameter('num_events', '', 1)
        self.AddParameter('smearing_pos', '', 5.)
        self.AddParameter('random_state', '', 1337)
        self.AddParameter('random_service', '', None)

    def Configure(self):
        self.particle_type = self.GetParameter('particle_type')
        self.map_filename = self.GetParameter('map_filename')
        self.smearing_angle = self.GetParameter('smearing_angle')
        self.event_name = self.GetParameter('event_name')
        self.num_events = self.GetParameter('num_events')
        self.pos_sigma = self.GetParameter('smearing_pos')
        self.random_state = self.GetParameter('random_state')
        self.random_service = self.GetParameter('random_service')
        if not isinstance(self.random_state, np.random.RandomState):
            self.random_state = np.random.RandomState(self.random_state)
        self.events_done = 0

        self.the_map = getSmearedMap(
            filename=self.map_filename,
            sigmaInDeg=self.smearing_angle/I3Units.deg,
            eventName=self.event_name
            )

    def DAQ(self, frame):
        sample = sampleFromMap(self.the_map,
                               self.random_state,
                               ptype=self.particle_type,
                               pos_sigma=self.pos_sigma)
        primary = dataclasses.I3Particle()
        daughter = dataclasses.I3Particle()

        primary.time = sample['time'] * I3Units.ns
        primary.dir = dataclasses.I3Direction(sample['zenith'],
                                              sample['azimuth'])
        primary.energy = sample['energy'] * I3Units.GeV
        primary.pos = dataclasses.I3Position(sample['posX'] * I3Units.m,
                                             sample['posY'] * I3Units.m,
                                             sample['posZ'] * I3Units.m)
        primary.speed = dataclasses.I3Constants.c
        primary.location_type = dataclasses.I3Particle.LocationType.Anywhere

        if self.particle_type == 'numu':
            primary.type = dataclasses.I3Particle.ParticleType.NuMu
            daughter.type = dataclasses.I3Particle.ParticleType.MuMinus
        elif self.particle_type == 'nutau':
            primary.type = dataclasses.I3Particle.ParticleType.NuTau
            daughter.type = dataclasses.I3Particle.ParticleType.TauMinus
        elif self.particle_type == 'nue':
            primary.type = dataclasses.I3Particle.ParticleType.NuE
            daughter.type = dataclasses.I3Particle.ParticleType.EMinus
        else:
            raise ValueError(('particle_type {} not known or not ' +
                              'implemented'.format(self.particle_type)))

        # Load xsec tables
        # Draw muon energy from xsec
        base_path = os.path.join('$I3_DATA',
                                 'neutrino-generator',
                                 'cross_section_data',
                                 'csms_differential_v1.0')
        base_path = os.path.expandvars(base_path)

        cross_section = sim_services.I3CrossSection(
            os.path.join(base_path, 'dsdxdy_nu_CC_iso.fits'),
            os.path.join(base_path, 'sigma_nu_CC_iso.fits'))

        final_state_record = cross_section.sample_final_state(
            primary.energy,
            primary.type,
            self.random_service)

        daughter.energy = primary.energy * (1 - final_state_record.y)

        daughter.time = primary.time
        daughter.dir = primary.dir
        daughter.speed = primary.speed
        daughter.pos = dataclasses.I3Position(
            primary.pos.x,
            primary.pos.y,
            primary.pos.z)

        daughter.location_type = dataclasses.I3Particle.LocationType.InIce
        daughter.shape = dataclasses.I3Particle.InfiniteTrack

        # ################################################
        # Add hadrons to the mctree with E_h = E_nu * y  #
        # ################################################
        hadrons = dataclasses.I3Particle()
        hadrons.energy = primary.energy - daughter.energy
        hadrons.pos = dataclasses.I3Position(
            primary.pos.x,
            primary.pos.y,
            primary.pos.z)
        hadrons.time = daughter.time
        hadrons.dir = daughter.dir
        hadrons.speed = daughter.speed
        hadrons.type = dataclasses.I3Particle.ParticleType.Hadrons
        hadrons.location_type = daughter.location_type
        hadrons.shape = dataclasses.I3Particle.Cascade

        # Fill primary and daughter particles into a MCTree
        mctree = dataclasses.I3MCTree()
        mctree.add_primary(primary)
        mctree.append_child(primary, daughter)
        mctree.append_child(primary, hadrons)

        frame["I3MCTree"] = mctree
        self.PushFrame(frame)

        self.events_done += 1
        if self.events_done >= self.num_events:
            self.RequestSuspension()


@click.command()
@click.argument('cfg', type=click.Path(exists=True))
@click.argument('run_number', type=int)
@click.option('--scratch/--no-scratch', default=True)
def main(cfg, run_number, scratch):
    with open(cfg, 'r') as stream:
        if int(yaml.__version__[0]) < 5:
            # backwards compatibility for yaml versions before version 5
            cfg = yaml.load(stream)
        else:
            cfg = yaml.full_load(stream)
    cfg['run_number'] = run_number
    cfg['run_folder'] = get_run_folder(run_number)
    if scratch:
        outfile = cfg['scratchfile_pattern'].format(**cfg)
    else:
        outfile = cfg['outfile_pattern'].format(**cfg)
    outfile = outfile.replace(' ', '0')

    click.echo('Run: {}'.format(run_number))
    click.echo('ParticleType: {}'.format(cfg['particle_type']))
    click.echo('Outfile: {}'.format(outfile))
    click.echo('n_events_per_run: {}'.format(cfg['n_events_per_run']))
    click.echo('smearing_angle: {}'.format(cfg['smearing_angle']))
    click.echo('skymap_path: {}'.format(cfg['skymap_path']))

    tray = I3Tray()
    random_services, _ = create_random_services(
        dataset_number=cfg['dataset_number'],
        run_number=cfg['run_number'],
        seed=cfg['seed'],
        n_services=2)

    tray.AddModule('I3InfiniteSource', 'source',
                   # Prefix=gcdfile,
                   Stream=icetray.I3Frame.DAQ)

    tray.AddModule(ParticleFactory,
                   'make_particles',
                   particle_type=cfg['particle_type'],
                   map_filename=cfg['skymap_path'],
                   num_events=cfg['n_events_per_run'],
                   smearing_angle=cfg['smearing_angle'] * I3Units.deg,
                   random_state=cfg['seed'],
                   random_service=random_services[0])

    tray.AddSegment(segments.PropagateMuons,
                    'propagate_muons',
                    RandomService=random_services[1],
                    InputMCTreeName='I3MCTree',
                    **cfg['muon_propagation_config'])

    tray.AddModule('I3Writer', 'write',
                   Filename=outfile,
                   Streams=[icetray.I3Frame.DAQ, icetray.I3Frame.Stream('M')])
    tray.AddModule('TrashCan', 'trash')
    tray.Execute()
    tray.Finish()
    del tray

if __name__ == '__main__':
    print('AJAJAJ')
    main()
