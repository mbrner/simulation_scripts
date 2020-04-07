#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v1/icetray-start
#METAPROJECT /home/mboerner/software/i3/IC2012-L2_V13-01-00_IceSim04-01-10compat/build
import click
import yaml

from utils import get_run_folder

from I3Tray import I3Tray
from icecube import icetray, dataclasses, dataio
from icecube.icetray import I3PacketModule

from icecube.filter_2012 import Globals
from icecube.filter_2012.Globals import (which_split, deepcore_wg,
    icetop_wg_coic_inice, muon_wg, wimp_wg, cascade_wg,
    fss_wg, fss_wg_finiteReco, ehe_wg, ehe_wg_Qstream)
#from icecube.filter_2012.Offline_Base import RepeatBaseProc
from icecube.filter_2012.level2_IceTop_CalibrateAndExtractPulses import CalibrateAndExtractIceTop
from icecube.filter_2012.level2_EHE_Calibration import EHECalibration
from icecube.filter_2012.level2_HitCleaning_IceTop import IceTopCoincTWCleaning
from icecube.filter_2012.level2_HitCleaning_DeepCore import DeepCoreHitCleaning
from icecube.filter_2012.level2_HitCleaning_WIMP import WimpHitCleaning
from icecube.filter_2012.level2_HitCleaning_Cascade import CascadeHitCleaning
from icecube.filter_2012.PhotonTables import InstallTables
from icecube.filter_2012.level2_Reconstruction_Muon import OfflineMuonReco
from icecube.filter_2012.level2_HitCleaning_EHE import HitCleaningEHE
from icecube.filter_2012.level2_Reconstruction_IceTop import ReconstructIceTop
from icecube.filter_2012.level2_Reconstruction_DeepCore import OfflineDeepCoreReco
from icecube.filter_2012.level2_Reconstruction_WIMP import WimpReco
from icecube.filter_2012.Rehydration import Rehydration
#from icecube.filter_2012.level2_Reconstruction_FSS import OfflineFSSReco
from icecube.filter_2012.level2_Reconstruction_Cascade import OfflineCascadeReco
from icecube.filter_2012.level2_Reconstruction_SLOP import SLOPLevel2
from icecube.filter_2012.level2_Reconstruction_EHE import ReconstructionEHE
from icecube.filter_2012 import SpecialWriter
icetray.load("SeededRTCleaning", False)

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
    if 'dictitems' in cfg.keys():
        cfg = cfg['dictitems']
    cfg['run_number'] = run_number
    cfg['run_folder'] = get_run_folder(run_number)
    infile = cfg['infile_pattern'].format(**cfg)
    infile = infile.replace(' ', '0')
    infile = infile.replace('Level0.{}'.format(cfg['previous_step']),
                            'Level0.{}'.format(cfg['previous_step'] % 10))
    infile = infile.replace('2012_pass2', '2012')

    if scratch:
        outfile = cfg['scratchfile_pattern'].format(**cfg)
    else:
        outfile = cfg['outfile_pattern'].format(**cfg)
    outfile = outfile.replace('Level0.{}'.format(cfg['step']),
                            'Level2')
    outfile = outfile.replace(' ', '0')
    outfile = outfile.replace('2012_pass2', '2012')
    print('Outfile != $FINAL_OUT clean up for crashed scripts not possible!')

    tray = I3Tray()
    tray.AddModule('I3Reader',
                   'i3 reader',
                   FilenameList=[cfg['gcd'], infile])


    class EmptyIceTopBadLists(icetray.I3ConditionalModule):
        def __init__(self, context):
            icetray.I3ConditionalModule.__init__(self, context)

        def Configure(self):
            self.Register(icetray.I3Frame.DetectorStatus, self.Detector)

        def Detector(self, frame):
            frame['IceTopBadDOMs'] = dataclasses.I3VectorOMKey()
            frame['IceTopBadTanks'] = dataclasses.TankKey.I3VectorTankKey()
            self.PushFrame(frame)

    ##################################################################
    #########  Level 1                                     ###########
    #########  IF SIM, do L1 that was done on PnF          ###########
    #########  IF DATA, Rehydrate, recalibrate             ###########
    #########  FOR BOTH,  recal, resplit IT                ###########
    ##################################################################

    tray.AddSegment(Rehydration, 'rehydrator',
        dstfile=None,
        mc=True)

    ## relic of redoing pole fits. That got taken out.
    ## but need to keep doing SRT cleaning for all the filters
    tray.AddModule("I3SeededRTHitMaskingModule",  'North_seededrt',
        MaxIterations = 3,
        Seeds = 'HLCcore',
        InputResponse = 'SplitInIcePulses',
        OutputResponse = 'SRTInIcePulses',
        If = lambda f: (which_split(f, split_name='InIceSplit') and
                        (deepcore_wg(f) or wimp_wg(f) or
                         muon_wg(f) or cascade_wg(f) or
                         ehe_wg(f) or fss_wg(f) or icetop_wg_coic_inice(f)))
    )

    ## Counter to keep track of the differences between PnF and offline split
    #tray.AddModule("I3PQEventCounter", "countme")(
    #	("Substreams", ["InIceSplit"]),
    #	("Bools",["NFramesIsDifferent"]),
    #)

    ## IceTop pules calibration
    tray.AddSegment(CalibrateAndExtractIceTop, 'CalibrateAndExtractIceTop',
        Pulses='IceTopPulses'
    )

    ## EHE Calibration
    tray.AddSegment(EHECalibration, 'ehecalib',
        inPulses='CleanInIceRawData',
        outATWD='EHECalibratedATWD_Wave',
        outFADC='EHECalibratedFADC_Wave',
        If=lambda f: ehe_wg_Qstream(f)
    )

    ###################################################################
    ########### HIT CLEANING    #######################################
    ###################################################################
    # icetop hitcleaning & splitting #
    tray.AddSegment(IceTopCoincTWCleaning, 'IceTopCoincTWCleaning',
        VEMPulses = 'CleanedHLCTankPulses',
        OfflinePulses = 'InIcePulses'
    )

    # deepcore hitcleaning #
    tray.AddSegment(DeepCoreHitCleaning,'DCHitCleaning',
        If=lambda f: (which_split(f,split_name='InIceSplit') and
                      deepcore_wg(f))
    )

    # wimp & FSS hitcleaning #
    tray.AddSegment(WimpHitCleaning, "WIMPstuff",
        If=lambda f: (which_split(f, split_name='InIceSplit') and
                      (wimp_wg(f) or fss_wg_finiteReco(f))),
        suffix='_WIMP',
    )

    # cascade hit cleaning #
    tray.AddSegment(CascadeHitCleaning,'CascadeHitCleaning',
        If=lambda f: (which_split(f, split_name='InIceSplit') and
                      cascade_wg(f)),
    )

    # ehe hit cleaning #
    tray.AddSegment(HitCleaningEHE, 'eheclean',
        inATWD='EHECalibratedATWD_Wave', inFADC = 'EHECalibratedFADC_Wave',
        If=lambda f: which_split(f, split_name='InIceSplit') and ehe_wg(f)
    )

    ###################################################################
    ########### RECONSTRUCTIONS/CALCULATIONS ##########################
    ###################################################################
    # load tables #
    tray.AddSegment(InstallTables, 'InstallPhotonTables',
        PhotonicsDir=PHOTONICS_DIR
    )

    # muon, cascade, wimp, fss #
    tray.AddSegment(OfflineMuonReco, 'OfflineMuonRecoSLC',
        Pulses = "SRTInIcePulses",
	If = lambda f: ((muon_wg(f) or icetop_wg_coic_inice(f) or cascade_wg(f) or wimp_wg(f) or fss_wg(f)) and which_split(f, split_name='InIceSplit')),
        suffix = "", #null? copied from level2_globals supplied
        #photonics_service_mu_spice1 = Globals.photonics_service_mu_spice1,
        #photonics_service_mu_spicemie = Globals.photonics_service_mu_spicemie
    )

    # icetop #
    tray.AddSegment(ReconstructIceTop, 'ReconstructIceTop',
        Pulses      = 'CleanedHLCTankPulses',
        CoincPulses = 'CleanedCoincOfflinePulses',
        If = lambda f:which_split(f, split_name='ice_top')
    )

    # deepcore #
    tray.AddSegment(OfflineDeepCoreReco,'DeepCoreL2Reco',
        pulses='SRTTWOfflinePulsesDC',
        If=lambda f: (which_split(f, split_name='InIceSplit') and
                      deepcore_wg(f)),
        suffix='_DC')

    # wimp, fss #
    tray.AddSegment(WimpReco, "WIMPreco",
        If=lambda f: (which_split(f, split_name='InIceSplit') and
                      (wimp_wg(f) or fss_wg_finiteReco(f))),
        suffix='_WIMP',
    )

    tray.AddSegment(OfflineCascadeReco,'CascadeL2Reco',
        Pulses='TWOfflinePulsesHLC',
        TopoPulses = 'OfflinePulsesHLC',
        PhotonicsServiceName = Globals.photonics_service_cscd,
        If=lambda f: (which_split(f, split_name='InIceSplit') and
                      cascade_wg(f)),
        suffix='_L2'
    )

    # slop #
    tray.AddSegment(SLOPLevel2, "slop_me",
        If = lambda f:which_split(f, split_name='SLOPSplit')
    )

    # ehe #
    tray.AddSegment(ReconstructionEHE, 'ehereco',
        Pulses='EHETWCInIcePulsesSRT',
        suffix='EHE', LineFit = 'LineFit',
        SPEFitSingle='SPEFitSingle', SPEFit = 'SPEFit12',
        N_iter=12,
        If=lambda f: which_split(f, split_name='InIceSplit') and ehe_wg(f)
    )

    tray.AddModule("I3Writer", "EventWriter",
                   filename=outfile,
                   Streams=[icetray.I3Frame.DAQ,
                            icetray.I3Frame.Physics,
                            icetray.I3Frame.TrayInfo,
                            icetray.I3Frame.Stream('S')],
                   DropOrphanStreams=[icetray.I3Frame.DAQ])
    tray.AddModule("TrashCan", "the can")
    tray.Execute()
    tray.Finish()


if __name__ == '__main__':
    main()

