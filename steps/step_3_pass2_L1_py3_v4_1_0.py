#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py3-v4.1.0/icetray-start
#METAPROJECT /mnt/lfs7/user/mhuennefeld/software/icecube/py3-v4.1.0/combo_V01-00-00-RC0/build
import os

import click
import yaml

from I3Tray import I3Tray, I3Units
from icecube import icetray, dataclasses, dataio, filter_tools, trigger_sim
from icecube import phys_services
from icecube.filterscripts import filter_globals
from icecube.filterscripts.all_filters import OnlineFilter
from icecube.phys_services.which_split import which_split
import os
import sys
import time

import subprocess
from math import log10, cos, radians
from optparse import OptionParser
from os.path import expandvars


from utils import get_run_folder, muongun_keys, create_random_services


SPLINE_TABLES = '/cvmfs/icecube.opensciencegrid.org/data/photon-tables/splines'


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

    infile = cfg['infile_pattern'].format(**cfg)
    infile = infile.replace(' ', '0')
    infile = infile.replace('Level0.{}'.format(cfg['previous_step']),
                            'Level0.{}'.format(cfg['previous_step'] % 10))
    infile = infile.replace('2012_pass2', 'pass2')

    if scratch:
        outfile = cfg['scratchfile_pattern'].format(**cfg)
    else:
        outfile = cfg['outfile_pattern'].format(**cfg)
    outfile = outfile.replace('Level0.{}'.format(cfg['step']),
                              'Level0.{}'.format(cfg['step'] % 10))
    outfile = outfile.replace(' ', '0')
    outfile = outfile.replace('2012_pass2', 'pass2')
    print('Outfile != $FINAL_OUT clean up for crashed scripts not possible!')

    tray = I3Tray()
    """The main L1 script"""
    tray.AddModule('I3Reader',
                   'i3 reader',
                   FilenameList=[cfg['gcd_pass2'], infile])

    # run online filters
    online_kwargs = {}
    if SPLINE_TABLES:
        online_kwargs.update({
            'SplineRecoAmplitudeTable': os.path.join(
                SPLINE_TABLES, 'InfBareMu_mie_abs_z20a10.fits'),
            'SplineRecoTimingTable': os.path.join(
                SPLINE_TABLES, 'InfBareMu_mie_prob_z20a10.fits'),
            # 'alert_followup_base_GCD_filename': cfg['gcd_pass2'],
        })
    if cfg['L1_pass2_run_gfu'] is not None:
        online_kwargs['gfu_enabled'] = cfg['L1_pass2_run_gfu']
    if 'L1_needs_wavedeform_spe_corr' not in cfg:
        cfg['L1_needs_wavedeform_spe_corr'] = False
    tray.AddSegment(OnlineFilter, "OnlineFilter",
                    decode=False, simulation=True,
                    vemcal_enabled=False,
                    alert_followup=False,
                    needs_wavedeform_spe_corr=cfg[
                        'L1_needs_wavedeform_spe_corr'],
                    **online_kwargs
                    )

    # make random service
    _, seed = create_random_services(
        dataset_number=cfg['dataset_number'],
        run_number=cfg['run_number'],
        seed=cfg['seed'],
        n_services=0)
    filter_mask_randoms = phys_services.I3GSLRandomService(seed)

    # override MinBias Prescale
    filterconfigs = filter_globals.filter_pairs + filter_globals.sdst_pairs
    print(cfg['L1_min_bias_prescale'])
    if cfg['L1_min_bias_prescale']:
        for i, filtertuple in enumerate(filterconfigs):
            if filtertuple[0] == filter_globals.FilterMinBias:
                del filterconfigs[i]
                filterconfigs.append((filtertuple[0],
                                      cfg['L1_min_bias_prescale']))
                break
    print(filterconfigs)

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

    # Q+P frame specific keep module needs to go first, as KeepFromSubstram
    # will rename things, let's rename post keep.
    def is_Q(frame):
        return frame.Stop == frame.DAQ

    simulation_keeps = [
        'BackgroundI3MCTree',
        'BackgroundI3MCTreePEcounts',
        'BackgroundI3MCPESeriesMap',
        'BackgroundI3MCTree_preMuonProp',
        'BackgroundI3MCTree_preMuonProp_RNGState',
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
        'I3MCTree_preMuonProp_RNGState',
        'I3MCPESeriesMap',
        'I3MCPESeriesMapWithoutNoise',
        'I3MCPESeriesMapParticleIDMap',
        'I3MCPulseSeriesMap',
        'I3MCPulseSeriesMapParticleIDMap',
        'I3MCPulseSeriesMapPrimaryIDMap',
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
        'SimTrimmer',  # for SimTrimmer flag
        'TimeShift',  # the time shift amount
        'WIMP_params',  # Wimp-sim
        'noise_weight',  # weights for noise-only vuvuzela simulations
        'I3GENIEResultDict'  # weight informaition for GENIE simulations
    ] + muongun_keys

    keep_before_merge = filter_globals.q_frame_keeps + [
        'InIceDSTPulses',  # keep DST pulse masks
        'IceTopDSTPulses',
        'CalibratedWaveformRange',  # keep calibration info
        'UncleanedInIcePulsesTimeRange',
        'SplitUncleanedInIcePulses',
        'SplitUncleanedInIcePulsesTimeRange',
        'SplitUncleanedInIceDSTPulsesTimeRange',
        'CalibrationErrata',
        'SaturationWindows',
        'InIceRawData',  # keep raw data for now
        'IceTopRawData',
    ] + simulation_keeps

    tray.AddModule("Keep", "keep_before_merge",
                   keys=keep_before_merge,
                   If=is_Q)

    # second set of prekeeps, conditional on filter content, based on newly
    # created Qfiltermask
    # Determine if we should apply harsh keep for events that failed to pass
    # any filter
    # Note: excluding the sdst_streams entries
    tray.AddModule("I3IcePickModule<FilterMaskFilter>", "filterMaskCheckAll",
                   FilterNameList=filter_globals.filter_streams,
                   FilterResultName=filter_globals.qfilter_mask,
                   DecisionName="PassedAnyFilter",
                   DiscardEvents=False,
                   Streams=[icetray.I3Frame.DAQ])

    def do_save_just_superdst(frame):
        if frame.Has("PassedAnyFilter"):
            if not frame["PassedAnyFilter"].value:
                return True  # <- Event failed to pass any filter.
            else:
                return False  # <- Event passed some filter
        else:
            icetray.logging.log_error("Failed to find key frame Bool!!")
            return False

    keep_only_superdsts = filter_globals.keep_nofilterpass + [
        'PassedAnyFilter',
        'InIceDSTPulses',
        'IceTopDSTPulses',
        'SplitUncleanedInIcePulses',
        'SplitUncleanedInIcePulsesTimeRange',
        'SplitUncleanedInIceDSTPulsesTimeRange',
        'RNGState'] + simulation_keeps
    tray.AddModule("Keep", "KeepOnlySuperDSTs",
                   keys=keep_only_superdsts,
                   If=do_save_just_superdst)

    # Now clean up the events that not even the SuperDST filters passed on
    tray.AddModule("I3IcePickModule<FilterMaskFilter>", "filterMaskCheckSDST",
                   FilterNameList=filter_globals.sdst_streams,
                   FilterResultName=filter_globals.qfilter_mask,
                   DecisionName="PassedKeepSuperDSTOnly",
                   DiscardEvents=False,
                   Streams=[icetray.I3Frame.DAQ])

    def dont_save_superdst(frame):
        if frame.Has("PassedKeepSuperDSTOnly") and \
                frame.Has("PassedAnyFilter"):
            if frame["PassedAnyFilter"].value:
                return False  # <- these passed a regular filter, keeper
            elif not frame["PassedKeepSuperDSTOnly"].value:
                return True  # <- Event failed to pass SDST filter.
            else:
                return False  # <- Event passed some  SDST filter
        else:
            icetray.logging.log_error("Failed to find key frame Bool!!")
            return False

    # backward compatibility
    if 'L1_keep_untriggered' in cfg and cfg['L1_keep_untriggered']:
        discard_substream_and_keys = False
    else:
        discard_substream_and_keys = True

    if discard_substream_and_keys:
        tray.AddModule("Keep", "KeepOnlyDSTs",
                       keys=filter_globals.keep_dst_only + [
                           "PassedAnyFilter",
                           "PassedKeepSuperDSTOnly",
                           filter_globals.eventheader] + muongun_keys,
                       If=dont_save_superdst)

        # Frames should now contain only what is needed.  now flatten,
        # write/send to server
        # Squish P frames back to single Q frame, one for each split:
        tray.AddModule("KeepFromSubstream", "null_stream",
                       StreamName=filter_globals.NullSplitter,
                       KeepKeys=filter_globals.null_split_keeps)

    in_ice_keeps = filter_globals.inice_split_keeps + \
        filter_globals.onlinel2filter_keeps
    in_ice_keeps = in_ice_keeps + ['I3EventHeader',
                                   'SplitUncleanedInIcePulses',
                                   'SplitUncleanedInIcePulsesTimeRange',
                                   'TriggerSplitterLaunchWindow',
                                   'I3TriggerHierarchy',
                                   'GCFilter_GCFilterMJD'] + muongun_keys
    tray.AddModule("Keep", "inice_keeps",
                   keys=in_ice_keeps,
                   If=which_split(split_name=filter_globals.InIceSplitter),)

    tray.AddModule("KeepFromSubstream", "icetop_split_stream",
                   StreamName=filter_globals.IceTopSplitter,
                   KeepKeys=filter_globals.icetop_split_keeps)

    # Apply small keep list (SuperDST/SmallTrig/DST/FilterMask for non-filter
    # passers
    # Remove I3DAQData object for events not passing one of the
    # 'filters_keeping_allraw'
    tray.AddModule("I3IcePickModule<FilterMaskFilter>", "filterMaskCheck",
                   FilterNameList=filter_globals.filters_keeping_allraw,
                   FilterResultName=filter_globals.qfilter_mask,
                   DecisionName="PassedConventional",
                   DiscardEvents=False,
                   Streams=[icetray.I3Frame.DAQ])

    # Clean out the Raw Data when not passing conventional filter
    def I3RawDataCleaner(frame):
        if not (('PassedConventional' in frame and
                 frame['PassedConventional'].value == True) or
                ('SimTrimmer' in frame and
                 frame['SimTrimmer'].value == True)):
            frame.Delete('InIceRawData')
            frame.Delete('IceTopRawData')

    tray.AddModule(I3RawDataCleaner,
                   "CleanErrataForConventional",
                   Streams=[icetray.I3Frame.DAQ])

    tray.AddModule("I3Writer", "EventWriter",
                   filename=outfile,
                   Streams=[icetray.I3Frame.DAQ,
                            icetray.I3Frame.Physics,
                            icetray.I3Frame.TrayInfo,
                            icetray.I3Frame.Simulation,
                            icetray.I3Frame.Stream('M')])
    tray.AddModule("TrashCan", "the can")
    tray.Execute()
    tray.Finish()


if __name__ == '__main__':
    main()
