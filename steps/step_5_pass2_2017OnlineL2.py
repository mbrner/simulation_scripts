#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
#METAPROJECT http://icecube:skua@convey.icecube.wisc.edu/data/user/tkintscher/software/icerec.V05-01-07
#xxMETAPROJECT /scratch/tkintscher/icerec-trunk/build
import argparse
import glob
import os
import os.path
import sys
import numpy as np
# from I3Tray import *
from I3Tray import I3Tray
from icecube import icetray, dataclasses, dataio
from icecube import phys_services
from icecube import linefit, lilliput
from icecube import MuonGun
import icecube.lilliput.segments
from icecube.filterscripts import filter_globals
from icecube.filterscripts.muonfilter import MuonFilter
from icecube.IC86_2017_GFU.onlinel2filter import OnlineL2Filter
from icecube.IC86_2017_GFU.gfufilter import GammaFollowUp
from icecube.IC86_2017_GFU.slowreco import CustomSplineMPE
from icecube.phys_services.which_split import which_split
from icecube.weighting import get_weighted_primary
from icecube.toolbox.modules import AddMuon, AddMuonIntersection, AddDepositedEnergy
from icecube.toolbox.processing_time import TimerStart, TimerStop

import click
import yaml
import subprocess

from icecube.icetray import I3PacketModule, I3Units


from utils import get_run_folder


PHOTONICS_DIR = '/cvmfs/icecube.opensciencegrid.org/data/photon-tables'


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

    click.echo('Keep all OnlineL2: {}'.format(cfg['OnlineL2_keep_all_L2']))
    click.echo('Keep time residuals: {}'.format(cfg['OnlineL2_keep_time_residuals']))


    infile = cfg['infile_pattern'].format(**cfg)
    infile = infile.replace(' ', '0')
    infile = infile.replace('Level0.{}'.format(cfg['previous_step']),
                            'Level2')
    infile = infile.replace('Level0.{}'.format(cfg['previous_step']),
                            'Level0.{}'.format(cfg['previous_step'] % 10))
    infile = infile.replace('2012_pass2', 'pass2')

    if scratch:
        outfile = cfg['scratchfile_pattern'].format(**cfg)
    else:
        outfile = cfg['outfile_pattern'].format(**cfg)
    outfile = outfile.replace('Level0.{}'.format(cfg['step']),
                            '2017OnlineL2')
    outfile = outfile.replace(' ', '0')
    outfile = outfile.replace('2012_pass2', 'pass2')
    print('Outfile != $FINAL_OUT clean up for crashed scripts not possible!')



    # build tray
    tray = I3Tray()
    tray.context['I3FileStager'] = dataio.get_stagers()
    tray.Add('I3Reader', FilenameList=[ cfg['gcd_pass2'], infile ],
             SkipKeys=[ 'I3MCTree' ] if 'corsika' in infile.lower() else [])

    # drop exisiting P-Frames (will do our own splitting later)
    tray.Add(lambda f: False, Streams=[ icetray.I3Frame.Physics ])

    ############################################################################
    # the following modules repeat what is done in the base processing at Pole #
    ############################################################################

    # resplit Q frame
    icetray.load('trigger-splitter', False)
    tray.AddModule('I3TriggerSplitter', filter_globals.InIceSplitter,
                   TrigHierName='DSTTriggers',
                   TriggerConfigIDs = [ filter_globals.deepcoreconfigid,
                                        filter_globals.inicesmtconfigid,
                                        filter_globals.inicestringconfigid,
                                        filter_globals.volumetriggerconfigid
                   ],
                   SubEventStreamName=filter_globals.InIceSplitter,
                   InputResponses=[ 'InIceDSTPulses' ],
                   OutputResponses=[ filter_globals.SplitUncleanedInIcePulses ],
                   WriteTimeWindow=True)

    # evaluate TriggerHierarchy
    tray.AddModule("TriggerCheck_13", "BaseProc_Trigchecker",
                   I3TriggerHierarchy=filter_globals.triggerhierarchy,
                   InIceSMTFlag=filter_globals.inicesmttriggered,
                   IceTopSMTFlag=filter_globals.icetopsmttriggered,
                   InIceStringFlag=filter_globals.inicestringtriggered,
                   DeepCoreSMTFlag=filter_globals.deepcoresmttriggered,
                   DeepCoreSMTConfigID=filter_globals.deepcoreconfigid,
                   VolumeTriggerFlag=filter_globals.volumetrigtriggered,
                   SlowParticleFlag=filter_globals.slowparticletriggered,
                   FixedRateTriggerFlag=filter_globals.fixedratetriggered,
                   )

    # run SRT and TW Cleaning from the Base Processing
    from icecube.STTools.seededRT.configuration_services import I3DOMLinkSeededRTConfigurationService
    seededRTConfig = I3DOMLinkSeededRTConfigurationService(
                         ic_ic_RTRadius              = 150.0*I3Units.m,
                         ic_ic_RTTime                = 1000.0*I3Units.ns,
                         treat_string_36_as_deepcore = False,
                         useDustlayerCorrection      = False,
                         allowSelfCoincidence        = True
                     )

    tray.AddModule('I3SeededRTCleaning_RecoPulseMask_Module', 'BaseProc_seededrt',
                   InputHitSeriesMapName  = filter_globals.SplitUncleanedInIcePulses,
                   OutputHitSeriesMapName = filter_globals.SplitRTCleanedInIcePulses,
                   STConfigService        = seededRTConfig,
                   SeedProcedure          = 'HLCCoreHits',
                   NHitsThreshold         = 2,
                   MaxNIterations         = 3,
                   Streams                = [icetray.I3Frame.Physics],
                   If = which_split(split_name=filter_globals.InIceSplitter)
                  )

    tray.AddModule("I3TimeWindowCleaning<I3RecoPulse>", "TimeWindowCleaning",
                   InputResponse = filter_globals.SplitRTCleanedInIcePulses,
                   OutputResponse = filter_globals.CleanedMuonPulses,
                   TimeWindow = 6000*I3Units.ns,
                   If = which_split(split_name=filter_globals.InIceSplitter)
                  )

    tray.AddSegment(linefit.simple, "BaseProc_imprv_LF",
                    inputResponse = filter_globals.CleanedMuonPulses,
                    fitName = filter_globals.muon_linefit,
                    If = which_split(split_name=filter_globals.InIceSplitter)
                   )

    # Muon LLH SimpleFitter from GulliverSuite with LineFit seed.
    tray.AddSegment(lilliput.segments.I3SinglePandelFitter, filter_globals.muon_llhfit,
                    seeds = [filter_globals.muon_linefit],
                    pulses = filter_globals.CleanedMuonPulses,
                    If = which_split(split_name=filter_globals.InIceSplitter)
                   )

    # run MuonFilter
    tray.Add(MuonFilter, 'MuonFilter',
             pulses = filter_globals.CleanedMuonPulses,
             If = which_split(split_name=filter_globals.InIceSplitter)
            )
    tray.AddModule("I3FirstPulsifier", "BaseProc_first-pulsify",
                   InputPulseSeriesMapName = filter_globals.CleanedMuonPulses,
                   OutputPulseSeriesMapName = 'FirstPulseMuonPulses',
                   KeepOnlyFirstCharge = False,   # default
                   UseMask = False,               # default
                   If = which_split(split_name=filter_globals.InIceSplitter)
                  )

    # discard events not passing the MuonFilter
    tray.Add(lambda f: f.Has(filter_globals.MuonFilter) and f[filter_globals.MuonFilter].value)

    # run OnlineL2 filter
    tray.Add(TimerStart, timerName='OnlineL2',
             If = which_split(split_name=filter_globals.InIceSplitter))
    tray.AddSegment(OnlineL2Filter, "OnlineL2",
                    If = which_split(split_name=filter_globals.InIceSplitter) )
    tray.Add(TimerStop, timerName='OnlineL2')

    # discard events not passing the OnlineL2 filter
    tray.Add(lambda f: f.Has(filter_globals.OnlineL2Filter) and f[filter_globals.OnlineL2Filter].value)

    # run GFU filter
    tray.Add(TimerStart, timerName='GFU')
    tray.AddSegment(GammaFollowUp, "GFU",
                    OnlineL2SegmentName = "OnlineL2",
                    KeepDetails         = cfg['OnlineL2_keep_time_residuals'],
                    angular_error       = True)
    tray.Add(TimerStop, timerName='GFU')

    # discard events not passing the GFU filter
    if not cfg['OnlineL2_keep_all_L2']:
        tray.Add(lambda f: f.Has(filter_globals.GFUFilter) and f[filter_globals.GFUFilter].value)

        # in this case, also run splineMPE with maximum settings for comparison
        TEestis = [ 'OnlineL2_SplineMPE_TruncatedEnergy_AllDOMS_Muon',
                    'OnlineL2_SplineMPE_TruncatedEnergy_DOMS_Muon',
                    'OnlineL2_SplineMPE_TruncatedEnergy_AllBINS_Muon',
                    'OnlineL2_SplineMPE_TruncatedEnergy_BINS_Muon',
                    'OnlineL2_SplineMPE_TruncatedEnergy_ORIG_Muon' ]
        tray.Add(CustomSplineMPE, 'SplineMPEmax',
                 configuration = 'max',
                 pulses        = 'OnlineL2_CleanedMuonPulses',
                 trackSeeds    = [ 'OnlineL2_SplineMPE' ],
                 enEstis       = TEestis,
                 paraboloid    = True)

    # For MC weighting, keep the neutrino primary.
    if 'corsika' not in infile.lower():
        # Some CORSIKA files have I3MCTree objects much larger than 100 MB.
        # Loading them takes too long... instead use CorsikaWeightMap.PrimaryEnergy / PrimaryType for weighting.
        tray.AddModule(get_weighted_primary, 'get_weighted_primary', MCPrimary='I3MCPrimary')

    # For MC studies, store information about the muon from CC interaction
    if 'neutrino-generator' in infile.lower():
        # store muon intersection points
        tray.Add(AddMuon)
        tray.Add(AddMuonIntersection)
        # store deposited energy in detector
        tray.Add(AddDepositedEnergy)


    tray.AddModule("I3Writer", "EventWriter",
                   filename=outfile,
                   Streams=[icetray.I3Frame.DAQ,
                            icetray.I3Frame.Physics,
                            icetray.I3Frame.TrayInfo,
                            icetray.I3Frame.Simulation,
                            icetray.I3Frame.Stream('M')],
                   DropOrphanStreams=[icetray.I3Frame.DAQ])
    tray.AddModule("TrashCan", "the can")
    tray.Execute()
    del tray


if __name__ == '__main__':
    main()
