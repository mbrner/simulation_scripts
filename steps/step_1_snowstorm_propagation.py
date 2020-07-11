#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py3-v4.1.0/icetray-start
#METAPROJECT /mnt/lfs7/user/mhuennefeld/software/icecube/py3-v4.1.0/combo_V01-00-00/build

""" Run Snowstorm Propagation

Adopted from:
        https://code.icecube.wisc.edu/projects/icecube/browser/IceCube/
        meta-projects/combo/stable/simprod-scripts/resources/scripts/
        SnowSuite/3-Snowstorm.py

# Copyright (c) 2019
# Jakob van Santen <jakob.van.santen@desy.de>
# and the IceCube Collaboration <http://www.icecube.wisc.edu>
#
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
# SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION
# OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
# CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
"""

import os
import sys
import click
import yaml
import numpy as np
import time
import copy
import itertools
import tempfile

from I3Tray import I3Tray
from icecube import icetray, dataclasses, dataio, phys_services, clsim
from icecube.clsim.traysegments.common import \
    setupPropagators, setupDetector, configureOpenCLDevices
from icecube.clsim.traysegments.I3CLSimMakePhotons import \
    I3CLSimMakePhotonsWithServer
from icecube.ice_models import icewave
from icecube.ice_models import angsens_unified
from icecube.snowstorm import \
    Perturber, MultivariateNormal, DeltaDistribution, UniformDistribution
from icecube.snowstorm import all_parametrizations

from utils import create_random_services, get_run_folder
from resources import snowstorm_perturbers


# ----------------
# Helper Functions
# ----------------
class FrameSequenceReader(icetray.I3Module):
    """
    Emit frames from an externally supplied dataio.I3FrameSequence, effectively
    making a persistent I3Reader.
    """
    def __init__(self, ctx):
        super(FrameSequenceReader, self).__init__(ctx)
        self.AddParameter("Sequence", "Iterable of frames to emit", None)

    def Configure(self):
        self._frames = self.GetParameter("Sequence")

    def Process(self):
        # this can run into issues if it's the last one
        try:
            frame = next(self._frames)
            if frame is not None:
                self.PushFrame(frame)
            else:
                self.RequestSuspension()
        except StopIteration:
            self.RequestSuspension()


class Bumper(icetray.I3Module):
    """
    Stop the tray after N Q-frames
    """
    def __init__(self, ctx):
        super(Bumper, self).__init__(ctx)
        self.AddParameter("NumFrames", "", 100)

    def Configure(self):
        self._numframes = self.GetParameter("NumFrames")
        self._count = 0

    def DAQ(self, frame):
        self._count += 1
        if self._count >= self._numframes:
            self.PushFrame(frame)
            self.RequestSuspension()
        else:
            self.PushFrame(frame)


class EnsureSFrame(icetray.I3Module):
    """
    Inject an S frame if none present, and ensure that M frames come after S
    """
    def __init__(self, ctx):
        super(EnsureSFrame, self).__init__(ctx)
        self.AddParameter("Enable", "", True)

    def Configure(self):
        self._disabled = not self.GetParameter("Enable")
        self._mframes = []

    def Process(self):
        frame = self.PopFrame()
        if self._disabled:
            self.PushFrame(frame)
            return
        elif frame.Stop.id == 'S':
            # got an existing S frame, emit buffered M frames
            self._disabled = True
            self.PushFrame(frame)
            for m in self._mframes:
                self.PushFrame(m)
            del self._mframes[:]
        elif frame.Stop.id == 'M':
            self._mframes.append(frame)
        elif frame.Stop.id == 'Q':
            # no S frame seen, emit SMQ
            self._disabled = True
            self.PushFrame(icetray.I3Frame('S'))
            for m in self._mframes:
                self.PushFrame(m)
            del self._mframes[:]
            self.PushFrame(frame)
        else:
            self.PushFrame(frame)


class GatherStatistics(icetray.I3Module):
    """Mimick the summary stage of I3CLSimModule::Finish()"""
    def Finish(self):
        if 'I3SummaryService' not in self.context:
            return
        summary = self.context['I3SummaryService']
        server = self.context['CLSimServer']
        if "TotalNumGeneratedHits" not in summary.keys():
            summary["TotalNumGeneratedHits"] = 0
        for k, v in summary.items():
            if (k.startswith("I3PhotonToMCPEConverter") and
                    k.endswith("NumGeneratedHits")):
                summary["TotalNumGeneratedHits"] += v
                summary.pop(k)
        for k, v in server.GetStatistics().items():
            if k in summary and (k.startswith('Total') or
                                 k.startswith('Num')):
                summary[k] += v
            else:
                summary[k] = v


def run_snowstorm_propagation(cfg, infile, outfile):
    """Run SnowStorm Propagation.

    Adopted from:
        https://code.icecube.wisc.edu/projects/icecube/browser/IceCube/
        meta-projects/combo/stable/simprod-scripts/resources/scripts/
        SnowSuite/3-Snowstorm.py


    Parameters
    ----------
    cfg : dict
        Dictionary with configuration settings.
    infile : str
        Path to input file.
    outfile : str
        Path to output file.
    """

    start_time = time.time()

    # --------
    # Settings
    # --------
    default_args = {

        # required
        'NumEventsPerModel': 100,
        'DOMOversizeFactor': 1.,
        'UseI3PropagatorService': True,

        # optional
        'UseGPUs': True,
        'SummaryFile': 'summary_snowstorm.yaml',

        'UseOnlyDeviceNumber': None,
        'MCTreeName': 'I3MCTree',
        'OutputMCTreeName': None,
        'FlasherInfoVectName': None,
        'FlasherPulseSeriesName': None,
        'PhotonSeriesName': None,
        'MCPESeriesName': "I3MCPESeriesMap",
        'DisableTilt': False,
        'UnWeightedPhotons': False,
        'UnWeightedPhotonsScalingFactor': None,
        'UseGeant4': False,
        'ParticleHistory': True,
        'ParticleHistoryGranularity': 1*icetray.I3Units.m,
        'CrossoverEnergyEM': None,
        'CrossoverEnergyHadron': None,
        'UseCascadeExtension': True,
        'StopDetectedPhotons': True,
        'PhotonHistoryEntries': 0,
        'DoNotParallelize': False,
        'UnshadowedFraction': 1.0,
        'WavelengthAcceptance': None,
        'DOMRadius': 0.16510*icetray.I3Units.m,
        'CableOrientation': None,
        'OverrideApproximateNumberOfWorkItems': None,
        'IgnoreSubdetectors': ["IceTop"],
        'ExtraArgumentsToI3CLSimClientModule': dict(),
    }

    # overwrite default settings
    default_args.update(cfg)
    cfg = default_args

    snowstorm_config = cfg['snowstorm_config']
    if cfg['SummaryFile'] is not None:
        cfg['SummaryFile'] = cfg['SummaryFile'].format(**cfg)
    ice_model_location = \
        os.path.expandvars(snowstorm_config["IceModelLocation"])
    hole_ice_parameterization = \
        os.path.expandvars(snowstorm_config["HoleIceParameterization"])

    # set units to meter
    cfg['ParticleHistoryGranularity'] *= icetray.I3Units.m
    cfg['DOMRadius'] *= icetray.I3Units.m

    # Print out most important settings
    click.echo('\n---------------')
    click.echo('Script Settigns')
    click.echo('---------------')
    click.echo('\tInput: {}'.format(infile))
    click.echo('\tGCDFile: {}'.format(cfg['gcd']))
    click.echo('\tOutput: {}'.format(outfile))
    for key in ['DOMOversizeFactor', 'UseI3PropagatorService', 'UseGPUs',
                'SummaryFile']:
        click.echo('\t{}: {}'.format(key, cfg[key]))
    click.echo('---------------\n')

    # get random service
    random_services, _ = create_random_services(
        dataset_number=cfg['dataset_number'],
        run_number=cfg['run_number'],
        seed=cfg['seed'],
        n_services=1,
        use_gslrng=cfg['random_service_use_gslrng'])

    random_service = random_services[0]

    """
    Setup and run Snowstorm (aka MultiSim) by running a series of short
    trays, each with a different ice model. This works by front-loading as much
    of the expensive initialization (reading the GCD file, setting up
    PROPOSAL/Geant4, etc) as possible, so that only the propagation kernel
    needs to be recompiled for every tray.
    """
    # instantiate baseline detector setup.
    # this will help construct the baseline characteristics before applying
    # the perturbers
    print("Setting up detector... ", end="")
    clsimParams = setupDetector(
        GCDFile=cfg['gcd'],
        SimulateFlashers=bool(cfg['FlasherInfoVectName'] or
                              cfg['FlasherPulseSeriesName']),
        IceModelLocation=ice_model_location,
        DisableTilt=cfg['DisableTilt'],
        UnWeightedPhotons=cfg['UnWeightedPhotons'],
        UnWeightedPhotonsScalingFactor=cfg['UnWeightedPhotonsScalingFactor'],
        UseI3PropagatorService=cfg['UseI3PropagatorService'],
        UseGeant4=cfg['UseGeant4'],
        CrossoverEnergyEM=cfg['CrossoverEnergyEM'],
        CrossoverEnergyHadron=cfg['CrossoverEnergyHadron'],
        UseCascadeExtension=cfg['UseCascadeExtension'],
        StopDetectedPhotons=cfg['StopDetectedPhotons'],
        DOMOversizeFactor=cfg['DOMOversizeFactor'],
        UnshadowedFraction=cfg['UnshadowedFraction'],
        HoleIceParameterization=hole_ice_parameterization,
        WavelengthAcceptance=cfg['WavelengthAcceptance'],
        DOMRadius=cfg['DOMRadius'],
        CableOrientation=cfg['CableOrientation'],
        IgnoreSubdetectors=cfg['IgnoreSubdetectors'],
    )
    print("done")
    print("Setting up OpenCLDevices... ", end="")
    openCLDevices = configureOpenCLDevices(
        UseGPUs=cfg['UseGPUs'],
        UseCPUs=not cfg['UseGPUs'],
        OverrideApproximateNumberOfWorkItems=cfg[
                                    'OverrideApproximateNumberOfWorkItems'],
        DoNotParallelize=cfg['DoNotParallelize'],
        UseOnlyDeviceNumber=cfg['UseOnlyDeviceNumber'])
    print("done")

    # -------------------
    # Setup perturbations
    # -------------------
    # create empty "perturber" object
    perturber = Perturber()
    # get perturbation_cfg dict to simplify calls
    perturbation_cfg = snowstorm_config["Perturbations"]
    # loop over all perturbations in the perturbation_cfg
    print("Setting up perturbers... ")
    for name, params in perturbation_cfg.items():
        # catch special case of IceWavePlusModes
        if name == "IceWavePlusModes":
            if not params["apply"]:
                continue
            if params["type"] == "default":
                print("-> adding {} of type {}".format(name, params["type"]))
                perturber.add('IceWavePlusModes',
                              *icewave.get_default_perturbation())
                continue

            elif hasattr(snowstorm_perturbers, params["type"]):
                print("-> adding {} of type {}".format(name, params["type"]))
                get_perturber = getattr(snowstorm_perturbers, params["type"])
                perturber.add('IceWavePlusModes',
                              *get_perturber(**params['settings']))
                continue
            else:
                msg = "IceWavePlusModes of type '{}' are not implemented(yet)."
                raise NotImplementedError(msg.format(params["type"]))
        # all other cases
        if params["type"] == "delta":
            print("-> adding {} of type {}".format(name, params["type"]))
            params = params["delta"]
            perturber.add(name, all_parametrizations[name],
                          DeltaDistribution(params["x0"]))
        elif params["type"] == "gauss":
            print("-> adding {} of type {}".format(name, params["type"]))
            params = params["gauss"]
            # Caution: MultivariateNormal expect the covariance matrix as
            # first argument, so we need to use sigma**2
            perturber.add(name, all_parametrizations[name],
                          MultivariateNormal(
                            dataclasses.I3Matrix(np.diag(params["sigma"])**2),
                            params["mu"]))
        elif params["type"] == "uniform":
            print("-> adding {} of type {}".format(name, params["type"]))
            params = params["uniform"]
            perturber.add(name, all_parametrizations[name],
                          UniformDistribution(
                            [dataclasses.make_pair(*limits)
                             for limits in params["limits"]]))
        else:
            msg = "Perturbation '{}' of type '{}' not implemented."
            raise NotImplementedError(msg.format(name, params["type"]))
    print("done")

    # Setting up some other things
    gcdFrames = list(dataio.I3File(cfg['gcd']))
    inputStream = dataio.I3FrameSequence([infile])
    summary = dataclasses.I3MapStringDouble()
    intermediateOutputFiles = []

    # --------------
    # Run PhotonProp
    # --------------

    # start a model counter
    model_counter = 0

    # Execute photon propagation
    print("Executing photon propagation...", end="")
    while inputStream.more():
        # measure CLSimInit time
        time_CLSimInit_start = time.time()

        tray = I3Tray()
        tray.context['I3RandomService'] = random_service
        tray.context['I3SummaryService'] = summary
        # make a mutable copy of the config dict
        config = dict(clsimParams)
        # populate the M frame with I3FrameObjects from clsimParams
        model = icetray.I3Frame('M')
        for k, v in config.items():
            if isinstance(v, icetray.I3FrameObject):
                model[k] = v
        # apply perturbations in the order they were configured
        perturber.perturb(random_service, model)
        # check for items in the M-frame that were changed/added
        # by the perturbers
        for k in model.keys():
            if k.startswith('Snowstorm'):
                # keep all Snowstorm keys
                continue
            if k not in config:
                msg = "\n {} was put in the M frame, but does not appear in "
                msg += "the CLSim configuration dict"
                raise KeyError(msg.format(k))

            if config[k] != model[k]:
                # if an items was changed, copy it back to clsimParams
                config[k] = model[k]
            else:
                # remove unmodified items from the M frame
                del model[k]

        # add "persistent" I3Reader
        tray.Add(FrameSequenceReader,
                 Sequence=itertools.chain(gcdFrames, [model], inputStream))

        # inject an S frame if it doesn't exist
        tray.Add(EnsureSFrame, Enable=len(intermediateOutputFiles) == 0)

        # write pertubations to frame
        def populate_s_frame(frame):
            perturber.to_frame(frame)
        tray.Add(populate_s_frame, Streams=[icetray.I3Frame.Stream('S')])

        # Add Bumper to stop the tray after NumEventsPerModel Q-frames
        tray.Add(Bumper, NumFrames=cfg['NumEventsPerModel'])

        # initialize CLSim server and setup the propagators
        server_location = tempfile.mkstemp(prefix='clsim-server-')[1]
        address = 'ipc://'+server_location
        converters = setupPropagators(
            random_service, config,
            UseGPUs=cfg['UseGPUs'],
            UseCPUs=not cfg['UseGPUs'],
            OverrideApproximateNumberOfWorkItems=cfg[
                                    'OverrideApproximateNumberOfWorkItems'],
            DoNotParallelize=cfg['DoNotParallelize'],
            UseOnlyDeviceNumber=cfg['UseOnlyDeviceNumber']
        )
        server = clsim.I3CLSimServer(
            address, clsim.I3CLSimStepToPhotonConverterSeries(converters))

        # stash server instance in the context to keep it alive
        tray.context['CLSimServer'] = server

        # recycle StepGenerator to prevent repeated, expensive initialization
        if 'StepGenerator' in cfg['ExtraArgumentsToI3CLSimClientModule']:
            stepGenerator = \
                cfg['ExtraArgumentsToI3CLSimClientModule']['StepGenerator']
            stepGenerator.SetMediumProperties(config['MediumProperties'])
            stepGenerator.SetWlenBias(config['WavelengthGenerationBias'])

        # add CLSim server to tray
        module_config = \
            tray.Add(
                I3CLSimMakePhotonsWithServer,
                ServerAddress=address,
                DetectorSettings=config,
                MCTreeName=cfg['MCTreeName'],
                OutputMCTreeName=cfg['OutputMCTreeName'],
                FlasherInfoVectName=cfg['FlasherInfoVectName'],
                FlasherPulseSeriesName=cfg['FlasherPulseSeriesName'],
                PhotonSeriesName=cfg['PhotonSeriesName'],
                MCPESeriesName=cfg['MCPESeriesName'],
                RandomService=random_service,
                ParticleHistory=cfg['ParticleHistory'],
                ParticleHistoryGranularity=cfg['ParticleHistoryGranularity'],
                ExtraArgumentsToI3CLSimClientModule=cfg[
                                        'ExtraArgumentsToI3CLSimClientModule'],
            )

        # recycle StepGenerator to prevent repeated, expensive initialization
        cfg['ExtraArgumentsToI3CLSimClientModule']['StepGenerator'] = \
            module_config['StepGenerator']

        # write to temporary output file
        intermediateOutputFiles.append(
            tempfile.mkstemp(suffix=(outfile.split("/"))[-1])[1])
        tray.Add("I3Writer",
                 Filename=intermediateOutputFiles[-1],
                 DropOrphanStreams=[icetray.I3Frame.TrayInfo],
                 Streams=[icetray.I3Frame.TrayInfo,
                          icetray.I3Frame.Simulation,
                          icetray.I3Frame.Stream('M'),
                          icetray.I3Frame.DAQ,
                          icetray.I3Frame.Physics])

        # gather statistics in the "I3SummaryService"
        tray.Add(GatherStatistics)

        # measure CLSimInit time
        time_CLSimInit = time.time() - time_CLSimInit_start
        summary["CLSimInitTime_{:03d}".format(model_counter)] = time_CLSimInit
        if "TotalCLSimInitTime" not in summary:
            summary["TotalCLSimInitTime"] = time_CLSimInit
        else:
            summary["TotalCLSimInitTime"] += time_CLSimInit
        # measure CLSimTray time
        time_CLSimTray_start = time.time()

        # Execute Tray
        tray.Execute()

        # measure CLSimTray time
        time_CLSimTray = time.time() - time_CLSimTray_start
        summary["CLSimTrayTime_{:03d}".format(model_counter)] = time_CLSimTray
        if "TotalCLSimTrayTime" not in summary:
            summary["TotalCLSimTrayTime"] = time_CLSimTray
        else:
            summary["TotalCLSimTrayTime"] += time_CLSimTray
        # remove the temp file made by the server location thingy
        os.unlink(server_location)

        # increase model counter
        model_counter += 1

    print("done")

    # Add number of models to summary
    summary["TotalNumberOfModels"] = model_counter

    # Concatenate intermediate files
    print("Concatenating temporary files... ", end='')
    tray = I3Tray()
    tray.Add(dataio.I3Reader, "I3Reader", FilenameList=intermediateOutputFiles)
    tray.Add("I3Writer", Filename=outfile,
             DropOrphanStreams=[icetray.I3Frame.TrayInfo],
             Streams=[icetray.I3Frame.TrayInfo,
                      icetray.I3Frame.Simulation,
                      icetray.I3Frame.Stream('M'),
                      icetray.I3Frame.DAQ,
                      icetray.I3Frame.Physics])
    tray.Execute()
    tray.Finish()
    print("done")

    print("Cleaning up Temporary files... ")
    for fname in intermediateOutputFiles:
        os.unlink(fname)
    print("done")

    # Recalculate averages
    print("Writing summary file... ", end='')
    if cfg['UseGPUs']:
        if summary['TotalHostTime'] > 0.0:
            summary['DeviceUtilization'] = \
                summary['TotalDeviceTime']/summary['TotalHostTime']
        if summary['TotalNumPhotonsGenerated'] > 0.0:
            summary['AverageDeviceTimePerPhoton'] = \
                summary['TotalDeviceTime']/summary['TotalNumPhotonsGenerated']
        if summary['TotalNumPhotonsGenerated'] > 0.0:
            summary['AverageHostTimePerPhoton'] = \
                summary['TotalHostTime']/summary['TotalNumPhotonsGenerated']
    if cfg['SummaryFile']:
        with open(cfg['SummaryFile'], 'w') as f:
            yaml.dump(dict(summary), f)
    print("done")
    print('--------')
    print('Summary:')
    print('--------')
    for key, value in summary.items():
        print('\t{}: {}'.format(key, value))
    print('--------\n')

    # Hurray!
    print("All finished!")
    # say something about the runtime
    end_time = time.time()
    print("That took "+str(end_time - start_time)+" seconds.")


@click.command()
@click.argument('cfg', type=click.Path(exists=True))
@click.argument('run_number', type=int)
@click.option('--scratch/--no-scratch', default=True)
def main(cfg, run_number, scratch):
    with open(cfg, 'r') as stream:
        cfg = yaml.full_load(stream)
    cfg['run_number'] = run_number
    cfg['run_folder'] = get_run_folder(run_number)

    infile = cfg['infile_pattern'].format(**cfg)
    infile = infile.replace(' ', '0')

    if scratch:
        outfile = cfg['scratchfile_pattern'].format(**cfg)
    else:
        outfile = cfg['outfile_pattern'].format(**cfg)
    outfile = outfile.replace(' ', '0')

    if cfg.get('distance_splits', False):
        raise NotImplementedError('Distance splits are not supported!')
    else:
        run_snowstorm_propagation(cfg, infile, outfile)


if __name__ == '__main__':
    main()
