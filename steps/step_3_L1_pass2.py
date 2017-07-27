#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
#METAPROJECT icerec/V05-01-06
import os

import click
import yaml

from icecube.filterscripts import filter_globals
from icecube.filterscripts.all_filters import OnlineFilter
from icecube.phys_services.which_split import which_split
from I3Tray import I3Tray
from icecube import icetray, dataclasses, dataio, phys_services
from icecube import filter_tools, trigger_sim
from utils import get_run_folder


SPLINE_TABLES = '/cvmfs/icecube.opensciencegrid.org/data/photon-tables/splines'


@click.command()
@click.argument('cfg', click.Path(exists=True))
@click.argument('run_number', type=int)
@click.option('--scratch/--no-scratch', default=True)
def main(cfg, run_number, scratch):
    with open(cfg, 'r') as stream:
        cfg = yaml.load(stream)
    cfg['run_number'] = run_number
    cfg['run_folder'] = get_run_folder(run_number)

    infile = cfg['infile_pattern'].format(**cfg)
    infile = infile.replace(' ', '0')

    tray = I3Tray()

    tray.context['I3FileStager'] = dataio.get_stagers()

    tray.AddSegment(
        OnlineFilter,
        "OnlineFilter",
        decode=False,
        simulation=True,
        vemcal_enabled=False,
        SplineRecoAmplitudeTable=os.path.join(SPLINE_TABLES,
                                              'InfBareMu_mie_abs_z20a10.fits'),
        SplineRecoTimingTable=os.path.join(SPLINE_TABLES,
                                           'InfBareMu_mie_prob_z20a10.fits'),
        hese_followup_base_GCD_filename=cfg['gcd_pass2'],
        gfu_enabled=cfg['l1_run_gfu'])

    filter_mask_randoms = phys_services.I3GSLRandomService(
        cfg['seed'] + run_number)
    # override MinBias Prescale
    filterconfigs = filter_globals.filter_pairs + filter_globals.sdst_pairs
    if cfg['l1_min_bias_prescale']:
        for i, filtertuple in enumerate(filterconfigs):
            if filtertuple[0] == filter_globals.FilterMinBias:
                del filterconfigs[i]
                filterconfigs.append((filtertuple[0],
                                      cfg['l1_min_bias_prescale']))
                break
    click.echo('filter_configs: {}'.format(filterconfigs))

    # Generate filter Masks for all P frames
    tray.AddModule(filter_tools.FilterMaskMaker,
                   "MakeFilterMasks",
                   OutputMaskName=filter_globals.filter_mask,
                   FilterConfigs=filterconfigs,
                   RandomService=filter_mask_randoms)

    tray.AddModule("OrPframeFilterMasks",
                   "make_q_filtermask",
                   InputName=filter_globals.filter_mask,
                   OutputName=filter_globals.qfilter_mask)

    #Q+P frame specific keep module needs to go first, as KeepFromSubstram
    #will rename things, let's rename post keep.
    def is_Q(frame):
        return frame.Stop == frame.DAQ

    simulation_keeps = [
        'BackgroundI3MCTree',
        'BackgroundI3MCTreePEcounts',
        'BackgroundI3MCPESeriesMap',
        'BackgroundI3MCTree_preMuonProp',
        'BackgroundMMCTrackList',
        'BeaconLaunches',
        'CorsikaInteractionHeight',
        'CorsikaWeightMap',
        'EventProperties',
        'GenerationSpec',
        'I3LinearizedMCTree',
        'I3MCTree',
        'I3MCTreePEcounts',
        'I3MCTree_preMuonProp',
        'I3MCPESeriesMap',
        'I3MCPulseSeriesMap',
        'I3MCPulseSeriesMapParticleIDMap',
        'I3MCWeightDict',
        'LeptonInjectorProperties',
        'MCHitSeriesMap',
        'MCPrimary',
        'MCPrimaryInfo',
        'MMCTrackList',
        'PolyplopiaInfo',
        'PolyplopiaPrimary',
        'RNGState',
        'SignalI3MCPEs',
        'SimTrimmer', # for SimTrimmer flag
        'TimeShift', # the time shift amount
        'WIMP_params']

    keep_before_merge = filter_globals.q_frame_keeps + [
                            'InIceDSTPulses', # keep DST pulse masks
                            'IceTopDSTPulses',
                            'CalibratedWaveformRange', # keep calibration info
                            'UncleanedInIcePulsesTimeRange',
                            'SplitUncleanedInIcePulses',
                            'SplitUncleanedInIcePulsesTimeRange',
                            'SplitUncleanedInIceDSTPulsesTimeRange',
                            'CalibrationErrata',
                            'SaturationWindows',
                            'InIceRawData', # keep raw data for now
                            'IceTopRawData',
                           ] + simulation_keeps

    tray.AddModule("Keep", "keep_before_merge",
                   keys = keep_before_merge,
                   If=is_Q
                   )

    ## second set of prekeeps, conditional on filter content, based on newly created Qfiltermask
    #Determine if we should apply harsh keep for events that failed to pass any filter
    ##  Note: excluding the sdst_streams entries

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
            print("Failed to find key frame Bool!!")
            return False

    keep_only_superdsts = filter_globals.keep_nofilterpass+[
                             'PassedAnyFilter',
                             'InIceDSTPulses',
                             'IceTopDSTPulses',
                             'SplitUncleanedInIcePulses',
                             'SplitUncleanedInIcePulsesTimeRange',
                             'SplitUncleanedInIceDSTPulsesTimeRange',
                             'RNGState',
                             ] + simulation_keeps
    tray.AddModule("Keep", "KeepOnlySuperDSTs",
                   keys = keep_only_superdsts,
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
            print("Failed to find key frame Bool!!")
            return False

    tray.AddModule("Keep", "KeepOnlyDSTs",
                   keys = filter_globals.keep_dst_only
                          + ["PassedAnyFilter","PassedKeepSuperDSTOnly",
                             filter_globals.eventheader],
                   If = dont_save_superdst
                   )

    ## Frames should now contain only what is needed.  now flatten,
    ## write/send to server
    ## Squish P frames back to single Q frame, one for each split:
    tray.AddModule("KeepFromSubstream","null_stream",
                   StreamName = filter_globals.NullSplitter,
                   KeepKeys = filter_globals.null_split_keeps,
                   )

    in_ice_keeps = filter_globals.inice_split_keeps + filter_globals.onlinel2filter_keeps
    in_ice_keeps = in_ice_keeps + ['I3EventHeader',
                                   'SplitUncleanedInIcePulses',
                                   'TriggerSplitterLaunchWindow',
                                   'I3TriggerHierarchy',
                                   'GCFilter_GCFilterMJD']
    tray.AddModule("Keep", "inice_keeps",
                   keys = in_ice_keeps,
                   If = which_split(split_name=filter_globals.InIceSplitter),
                   )


    tray.AddModule("KeepFromSubstream","icetop_split_stream",
                   StreamName = filter_globals.IceTopSplitter,
                   KeepKeys = filter_globals.icetop_split_keeps,
                   )

    # Apply small keep list (SuperDST/SmallTrig/DST/FilterMask for non-filter passers
    # Remove I3DAQData object for events not passing one of the 'filters_keeping_allraw'
    tray.AddModule("I3IcePickModule<FilterMaskFilter>","filterMaskCheck",
                   FilterNameList = filter_globals.filters_keeping_allraw,
                   FilterResultName = filter_globals.qfilter_mask,
                   DecisionName = "PassedConventional",
                   DiscardEvents = False,
                   Streams = [icetray.I3Frame.DAQ]
                   )


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

    if scratch:
        outfile = cfg['scratchfile_pattern'].format(**cfg)
    else:
        outfile = cfg['outfile_pattern'].format(**cfg)
    outfile = outfile.replace(' ', '0')
    tray.AddModule("I3Writer", "EventWriter",
                   filename=outfile,
                   Streams=[icetray.I3Frame.DAQ,
                            icetray.I3Frame.Physics,
                            icetray.I3Frame.TrayInfo,
                            icetray.I3Frame.Simulation])
    tray.AddModule("TrashCan", "the can")
    tray.Execute()
    tray.Finish()


if __name__ == '__main__':
    main()
