#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
#METAPROJECT icerec/V05-01-06
import os

import click
import yaml

from I3Tray import I3Tray, I3Units
from icecube import icetray, dataclasses, dataio, filter_tools, trigger_sim
from icecube import phys_services
from icecube.filterscripts import filter_globals
from icecube.filterscripts.all_filters import OnlineFilter
from icecube.phys_services.which_split import which_split
import os, sys, time

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
        cfg = yaml.load(stream)
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
            'hese_followup_base_GCD_filename': cfg['gcd_pass2'],
        })
    if cfg['L1_pass2_run_gfu'] is not None:
        online_kwargs['gfu_enabled'] = cfg['L1_pass2_run_gfu']
    tray.AddSegment(OnlineFilter, "OnlineFilter",
                    decode=False, simulation=True,
                    vemcal_enabled=False,
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
        'SimTrimmer',
        'TimeShift',
        'WIMP_params'] + muongun_keys

    keep_before_merge = filter_globals.q_frame_keeps + [
        'InIceDSTPulses',
        'IceTopDSTPulses',
        'CalibratedWaveformRange',
        'UncleanedInIcePulsesTimeRange',
        'SplitUncleanedInIcePulses',
        'SplitUncleanedInIcePulsesTimeRange',
        'SplitUncleanedInIceDSTPulsesTimeRange',
        'CalibrationErrata',
        'SaturationWindows',
        'InIceRawData',
        'IceTopRawData'] + simulation_keeps

    tray.AddModule("Keep", "keep_before_merge",
                   keys=keep_before_merge,
                   If=is_Q)

    tray.AddModule("I3IcePickModule<FilterMaskFilter>", "filterMaskCheckAll",
                   FilterNameList=filter_globals.filter_streams,
                   FilterResultName=filter_globals.qfilter_mask,
                   DecisionName="PassedAnyFilter",
                   DiscardEvents=False,
                   Streams=[icetray.I3Frame.DAQ])

    def do_save_just_superdst(frame):
        if frame.Has("PassedAnyFilter"):
            if not frame["PassedAnyFilter"].value:
                return True
            else:
                return False
        else:
            print("Failed to find key frame Bool!!")
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
                return False
            elif not frame["PassedKeepSuperDSTOnly"].value:
                return True
            else:
                return False
        else:
            print("Failed to find key frame Bool!!")
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

        tray.AddModule("KeepFromSubstream", "null_stream",
                       StreamName=filter_globals.NullSplitter,
                       KeepKeys=filter_globals.null_split_keeps)

    in_ice_keeps = filter_globals.inice_split_keeps + \
        filter_globals.onlinel2filter_keeps
    in_ice_keeps = in_ice_keeps + ['I3EventHeader',
                                   'SplitUncleanedInIcePulses',
                                   'TriggerSplitterLaunchWindow',
                                   'I3TriggerHierarchy',
                                   'GCFilter_GCFilterMJD'] + muongun_keys
    tray.AddModule("Keep", "inice_keeps",
                   keys=in_ice_keeps,
                   If=which_split(split_name=filter_globals.InIceSplitter),)

    tray.AddModule("KeepFromSubstream", "icetop_split_stream",
                   StreamName=filter_globals.IceTopSplitter,
                   KeepKeys=filter_globals.icetop_split_keeps)

    tray.AddModule("I3IcePickModule<FilterMaskFilter>", "filterMaskCheck",
                   FilterNameList=filter_globals.filters_keeping_allraw,
                   FilterResultName=filter_globals.qfilter_mask,
                   DecisionName="PassedConventional",
                   DiscardEvents=False,
                   Streams=[icetray.I3Frame.DAQ])

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
                            icetray.I3Frame.Simulation])
    tray.AddModule("TrashCan", "the can")
    tray.Execute()
    tray.Finish()


if __name__ == '__main__':
    main()
