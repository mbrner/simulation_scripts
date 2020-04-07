#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v1/icetray-start
#METAPROJECT /home/mboerner/software/i3/IC2012-L2_V13-01-00_IceSim04-01-10compat/build
import os

import click
import yaml

from utils import get_run_folder, muongun_keys, create_random_services

from I3Tray import I3Tray
from icecube import icetray, dataclasses, dataio, jeb_filter_2012
from icecube import filter_tools, trigger_sim
from icecube import phys_services
from icecube.jeb_filter_2012 import filter_globals
from icecube.jeb_filter_2012.filter_globals import which_split


PHOTONICS_DIR = '/cvmfs/icecube.opensciencegrid.org/data/photon-tables'


@click.command()
@click.argument('config_file', type=click.Path(exists=True))
@click.argument('run_number', type=int)
@click.option('--scratch/--no-scratch', default=True)
def main(config_file, run_number, scratch):
    with open(config_file, 'r') as stream:
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
                            'Level0.{}'.format(cfg['step'] % 10))
    outfile = outfile.replace(' ', '0')
    outfile = outfile.replace('2012_pass2', '2012')
    print('Outfile != $FINAL_OUT clean up for crashed scripts not possible!')

    _, seed = create_random_services(
        dataset_number=cfg['dataset_number'],
        run_number=cfg['run_number'],
        seed=cfg['seed'],
        n_services=0)

    tray = I3Tray()
    tray.AddModule('I3Reader',
                   'reader',
                   FilenameList=[cfg['gcd_2012'], infile],
                   SkipKeys=['I3DST11',
                             'I3SuperDST',
                             'I3VEMCalData',
                             'PoleMuonLlhFit',
                             'PoleMuonLlhFitCutsFirstPulseCuts',
                             'PoleMuonLlhFitFitParams',
                             'CramerRaoPoleL2IpdfGConvolute_2itParams',
                             'CramerRaoPoleL2MPEFitParams',
                             'PoleL2IpdfGConvolute_2it',
                             'PoleL2IpdfGConvolute_2itFitParams',
                             'PoleL2MPEFit',
                             'PoleL2MPEFitCuts',
                             'PoleL2MPEFitFitParams',
                             'PoleL2MPEFitMuE',
                             ])

    class SkipSFrames(icetray.I3ConditionalModule):
        S_stream = icetray.I3Frame.Stream('S')

        def __init__(self, context):
            icetray.I3ConditionalModule.__init__(self, context)

        def Configure(self):
            self.Register(self.S_stream, self.SFrame)

        def SFrame(self, frame):
            pass

#    tray.AddModule(SkipSFrames,
#                   "Skip I Frames")

    def check_driving_time(frame):
        if 'DrivingTime' not in frame:
            frame['DrivingTime'] = dataclasses.I3Time(
                frame['I3EventHeader'].start_time)
        return True

    tray.AddModule(check_driving_time,
                   'DrivingTimeCheck',
                   Streams=[icetray.I3Frame.DAQ])

    # move that old filterMask out of the way
    tray.AddModule("Rename",
                   "filtermaskmover",
                   Keys=["FilterMask", "OrigFilterMask"])

    if cfg['L1_2012_qify']:
        tray.AddModule("QConverter", "qify", WritePFrame=False)

    def MissingITCheck(frame):
        #print "Fixing IceTop RO"
        if "IceTopRawData" not in frame:
            itrd = dataclasses.I3DOMLaunchSeriesMap()
            frame["IceTopRawData"] = itrd
    tray.AddModule(MissingITCheck,
                   'L1_AddIceTopPulses',
                   Streams=[icetray.I3Frame.DAQ])

    if cfg['L1_2012_retrigger']:
        # some cleanup first
        tray.AddModule("Delete",
                       "delete_triggerHierarchy",
                       Keys=["I3TriggerHierarchy", "TimeShift"])
        gcd_file = dataio.I3File(cfg['gcd_2012'])
        tray.AddSegment(trigger_sim.TriggerSim,
                        "trig",
                        gcd_file=gcd_file)

    tray.AddSegment(jeb_filter_2012.BaseProcessing,
                    "BaseProc",
                    pulses=filter_globals.CleanedMuonPulses,
                    decode=False,
                    simulation=True,
                    DomLauncher=(not cfg['L1_2012_dom_simulator']))

    tray.AddSegment(jeb_filter_2012.MuonFilter,
                    "MuonFilter",
                    pulses=filter_globals.CleanedMuonPulses,
                    If=which_split(split_name=filter_globals.InIceSplitter))

    tray.AddSegment(jeb_filter_2012.CascadeFilter,
                    "CascadeFilter",
                    pulses=filter_globals.CleanedMuonPulses,
                    muon_llhfit_name=filter_globals.muon_llhfit,
                    If=which_split(split_name=filter_globals.InIceSplitter))

    tray.AddSegment(jeb_filter_2012.FSSFilter,
                    "FSSFilter",
                    pulses=filter_globals.SplitUncleanedInIcePulses,
                    If=which_split(split_name=filter_globals.InIceSplitter))

    tray.AddSegment(jeb_filter_2012.LowUpFilter,
                    "LowUpFilter",
                    If=which_split(split_name=filter_globals.InIceSplitter))

    tray.AddSegment(jeb_filter_2012.ShadowFilter,
                    "ShawdowFilters",
                    If=which_split(split_name=filter_globals.InIceSplitter))

    # use the PID as a seed. Good enough?


    tray.AddSegment(jeb_filter_2012.GCFilter,
                    "GCFilter",
                    mcseed=seed,
                    If = which_split(split_name=filter_globals.InIceSplitter))

    tray.AddSegment(jeb_filter_2012.VEFFilter, "VEFFilter",
                    pulses = filter_globals.CleanedMuonPulses,
                    If = which_split(split_name=filter_globals.InIceSplitter))

    if PHOTONICS_DIR is not None:
        photonicstabledirmu = os.path.join(PHOTONICS_DIR,'SPICE1')
        photonicsdriverfilemu = os.path.join('driverfiles','mu_photorec.list')
    else:
        photonicstabledirmu = None
        photonicsdriverfilemu = None

    tray.AddSegment(jeb_filter_2012.OnlineL2Filter,
                    "OnlineL2",
                    pulses=filter_globals.CleanedMuonPulses,
                    llhfit_name=filter_globals.muon_llhfit,
                    improved_linefit=True,
                    paraboloid=False,
                    PhotonicsTabledirMu=photonicstabledirmu,
                    PhotonicsDriverfileMu_Spice1=photonicsdriverfilemu,
                    If = which_split(split_name=filter_globals.InIceSplitter))

    tray.AddSegment(jeb_filter_2012.DeepCoreFilter,
                    "DeepCoreFilter",
                    pulses = filter_globals.SplitUncleanedInIcePulses,
                    If = which_split(split_name=filter_globals.InIceSplitter))

    tray.AddSegment(jeb_filter_2012.EHEFilter,
                    "EHEFilter",
                    If = which_split(split_name=filter_globals.NullSplitter))

    tray.AddSegment(jeb_filter_2012.MinBiasFilters,
                    "MinBias",
                    If = which_split(split_name=filter_globals.NullSplitter))

    tray.AddSegment(jeb_filter_2012.SlopFilters,
                    "SLOP",
                    If = which_split(split_name=filter_globals.NullSplitter))

    tray.AddSegment(jeb_filter_2012.FixedRateTrigFilter,
                    "FixedRate",
                    If = which_split(split_name=filter_globals.NullSplitter))

    tray.AddSegment(jeb_filter_2012.CosmicRayFilter,
                    "CosmicRayFilter",
                    pulseMask = filter_globals.SplitUncleanedITPulses,
                    If = which_split(split_name=filter_globals.IceTopSplitter))

    tray.AddSegment(jeb_filter_2012.DST,
                    "DSTFilter",
                    dstname="I3DST12",
                    pulses=filter_globals.CleanedMuonPulses,
                    If = which_split(split_name=filter_globals.InIceSplitter))

    # make random service
    filter_mask_randoms = phys_services.I3GSLRandomService(seed)

    # override MinBias Prescale
    filterconfigs = filter_globals.filter_pairs + filter_globals.sdst_pairs
    if cfg['L1_min_bias_prescale'] is not None:
        for i,filtertuple in enumerate(filterconfigs):
            if filtertuple[0] == filter_globals.FilterMinBias:
                del filterconfigs[i]
                filterconfigs.append((filtertuple[0],
                                      cfg['L1_min_bias_prescale']))
                break
    click.echo(filterconfigs)

    # Generate filter Masks for all P frames
    tray.AddModule(filter_tools.FilterMaskMaker,
                   "MakeFilterMasks",
                   OutputMaskName=filter_globals.filter_mask,
                   FilterConfigs=filterconfigs,
                   RandomService=filter_mask_randoms)

    # Merge the FilterMasks
    tray.AddModule("OrPframeFilterMasks",
                   "make_q_filtermask",
                   InputName=filter_globals.filter_mask,
                   OutputName=filter_globals.qfilter_mask)


    #Q+P frame specific keep module needs to go first, as KeepFromSubstram
    #will rename things, let's rename post keep.
    def is_Q(frame):
        return frame.Stop==frame.DAQ

    tray.AddModule("Keep",
                   "keep_before_merge",
                   keys = filter_globals.q_frame_keeps + [
                            'InIceDSTPulses', # keep DST pulse masks
                            'IceTopDSTPulses',
                            'CalibratedWaveformRange', # keep calibration info
                            'UncleanedInIcePulsesTimeRange',
                            'SplitUncleanedInIcePulsesTimeRange',
                            'SplitUncleanedInIceDSTPulsesTimeRange',
                            'CalibrationErrata',
                            'SaturationWindows',
                            'InIceRawData', # keep raw data for now
                            'IceTopRawData',
                            'CorsikaWeightMap', # sim keys
                            'I3MCWeightDict',
                            'MCHitSeriesMap',
                            'MMCTrackList',
                            'I3MCTree',
                            'I3LinearizedMCTree',
                            'MCPrimary',
                            'MCPrimaryInfo',
                            'TimeShift', # the time shift amount
                            'WIMP_params', # Wimp-sim
                            'SimTrimmer', # for SimTrimmer flag
                            'I3MCPESeriesMap',
                            'I3MCPulseSeriesMap',
                            'I3MCPulseSeriesMapParticleIDMap',
                          ] + muongun_keys,
                   If=is_Q
                   )

    tray.AddModule("I3IcePickModule<FilterMaskFilter>","filterMaskCheckAll",
                   FilterNameList = filter_globals.filter_streams,
                   FilterResultName = filter_globals.qfilter_mask,
                   DecisionName = "PassedAnyFilter",
                   DiscardEvents = False,
                   Streams = [icetray.I3Frame.DAQ]
                   )
    def do_save_just_superdst(frame):
        if frame.Has("PassedAnyFilter"):
            if not frame["PassedAnyFilter"].value:
                return True    #  <- Event failed to pass any filter.
            else:
                return False # <- Event passed some filter

        else:
            print "Failed to find key frame Bool!!"
            return False

    tray.AddModule("Keep", "KeepOnlySuperDSTs",
                   keys = filter_globals.keep_nofilterpass+[
                             'PassedAnyFilter',
                             'InIceDSTPulses',
                             'IceTopDSTPulses',
                             'SplitUncleanedInIcePulses',
                             'SplitUncleanedInIcePulsesTimeRange',
                             'SplitUncleanedInIceDSTPulsesTimeRange',
                             'CorsikaWeightMap', # sim keys
                             'I3MCWeightDict',
                             'MCHitSeriesMap',
                             'MMCTrackList',
                             'I3MCTree',
                             'I3LinearizedMCTree',
                             'MCPrimary',
                             'MCPrimaryInfo',
                             'TimeShift', # the time shift amount
                             'WIMP_params', # Wimp-sim
                             'I3MCPESeriesMap',
                             'I3MCPulseSeriesMap',
                             'I3MCPulseSeriesMapParticleIDMap',
                             ] + muongun_keys,
                   If = do_save_just_superdst
                   )

    ## Now clean up the events that not even the SuperDST filters passed on.
    tray.AddModule("I3IcePickModule<FilterMaskFilter>","filterMaskCheckSDST",
                   FilterNameList = filter_globals.sdst_streams,
                   FilterResultName = filter_globals.qfilter_mask,
                   DecisionName = "PassedKeepSuperDSTOnly",
                   DiscardEvents = False,
                   Streams = [icetray.I3Frame.DAQ]
                   )

    def dont_save_superdst(frame):
        if frame.Has("PassedKeepSuperDSTOnly") and frame.Has("PassedAnyFilter"):
            if frame["PassedAnyFilter"].value:
                return False  #  <- these passed a regular filter, keeper
            elif not frame["PassedKeepSuperDSTOnly"].value:
                return True    #  <- Event failed to pass SDST filter.
            else:
                return False # <- Event passed some  SDST filter

        else:
            print "Failed to find key frame Bool!!"
            return False

    # backward compatibility
    if 'L1_keep_untriggered' in cfg and cfg['L1_keep_untriggered']:
        discard_substream_and_keys = False
    else:
        discard_substream_and_keys = True

    if discard_substream_and_keys:
        tray.AddModule("Keep", "KeepOnlyDSTs",
                       keys = filter_globals.keep_dst_only
                              + ["PassedAnyFilter","PassedKeepSuperDSTOnly",
                                 filter_globals.eventheader] + muongun_keys,
                              If = dont_save_superdst
                       )


        ## Frames should now contain only what is needed.  now flatten, write/send to server
        ## Squish P frames back to single Q frame, one for each split:
        tray.AddModule("KeepFromSubstream","null_stream",
                       StreamName = filter_globals.NullSplitter,
                       KeepKeys = filter_globals.null_split_keeps,
                       )

    # Keep the P frames for InIce intact
    #tray.AddModule("KeepFromSubstream","inice_split_stream",
    #               StreamName = filter_globals.InIceSplitter,
    #               KeepKeys = filter_globals.inice_split_keeps + filter_globals.onlinel2filter_keeps,
    #               )
    #
    in_ice_keeps = filter_globals.inice_split_keeps + \
        filter_globals.onlinel2filter_keeps
    in_ice_keeps = in_ice_keeps + ['I3EventHeader',
                                   'SplitUncleanedInIcePulses',
                                   'SplitUncleanedInIcePulsesTimeRange',
                                   'SplitUncleanedInIceDSTPulsesTimeRange',
                                   'I3TriggerHierarchy',
                                   'GCFilter_GCFilterMJD']
    tray.AddModule("Keep", "inice_keeps",
                   keys = in_ice_keeps + muongun_keys,
                   If = which_split(split_name=filter_globals.InIceSplitter),
                   )


    tray.AddModule("KeepFromSubstream","icetop_split_stream",
                   StreamName = filter_globals.IceTopSplitter,
                   KeepKeys = filter_globals.icetop_split_keeps,
                   )


    tray.AddModule("I3IcePickModule<FilterMaskFilter>","filterMaskCheck",
                   FilterNameList = filter_globals.filters_keeping_allraw,
                   FilterResultName = filter_globals.qfilter_mask,
                   DecisionName = "PassedConventional",
                   DiscardEvents = False,
                   Streams = [icetray.I3Frame.DAQ]
                   )

    ## Clean out the Raw Data when not passing conventional filter
    def I3RawDataCleaner(frame):
        if not (('PassedConventional' in frame and
                 frame['PassedConventional'].value == True) or
                ('SimTrimmer' in frame and
                 frame['SimTrimmer'].value == True)
               ):
            frame.Delete('InIceRawData')
            frame.Delete('IceTopRawData')

    tray.AddModule(I3RawDataCleaner,"CleanErrataForConventional",
                   Streams=[icetray.I3Frame.DAQ])

    tray.AddModule("I3Writer", "EventWriter",
                   filename=outfile,
                   Streams=[icetray.I3Frame.DAQ,
                            icetray.I3Frame.Physics,
                            icetray.I3Frame.TrayInfo,
                            icetray.I3Frame.Stream('S')])
    tray.AddModule("TrashCan", "the can")
    tray.Execute()
    tray.Finish()


if __name__ == '__main__':
    main()
