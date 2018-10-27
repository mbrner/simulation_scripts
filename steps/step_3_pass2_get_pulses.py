#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
#METAPROJECT icerec/V05-01-06
import os
import sys
import time

import click
import yaml

from I3Tray import I3Tray, I3Units
from icecube import icetray, dataclasses
from icecube.filterscripts import filter_globals
from icecube.filterscripts.baseproc import BaseProcessing
from icecube.STTools.seededRT.configuration_services import \
    I3DOMLinkSeededRTConfigurationService

from utils import get_run_folder


@icetray.traysegment
def GetPulses(tray, name,
              simulation=False,
              decode=False,
              sdstarchive=False,
              slop_split_enabled=True,
              needs_wavedeform_spe_corr=False,
              If=lambda f: True,
              ):
    '''
    Relevant part of OnlineL2 tray segment that creates pulses from
    InIceRawData. Taken from:
        https://code.icecube.wisc.edu/projects/icecube/browser/IceCube/
        projects/filterscripts/trunk/python/all_filters.py
    '''

    # Create a SeededRT configuration object with the standard RT settings.
    # This object will be used by all the different SeededRT modules, i.e. the
    # modules use the same causial space and time conditions, but can use
    # different seed algorithms.
    seededRTConfig = I3DOMLinkSeededRTConfigurationService(
                         ic_ic_RTRadius              = 150.0*I3Units.m,
                         ic_ic_RTTime                = 1000.0*I3Units.ns,
                         treat_string_36_as_deepcore = False,
                         useDustlayerCorrection      = False,
                         allowSelfCoincidence        = True
                     )
    # base processing requires:  GCD and frames being fed in by reader or Inlet
    # base processing include:
    #     decoding, TriggerCheck, Bad Dom cleaning, calibrators,
    #     Feature extraction, pulse cleaning (seeded RT, and Time Window),
    #     PoleMuonLineit, PoleMuonLlh, Cuts module and Mue on PoleMuonLlh
    if sdstarchive:
        tray.AddSegment(BaseProcessing, "BaseProc",
                        pulses=filter_globals.CleanedMuonPulses,
                        decode=decode,
                        simulation=False,
                        needs_calibration=False, needs_superdst=False,
                        do_slop=slop_split_enabled,
                        needs_trimmer=False, seededRTConfig=seededRTConfig
                        )
    else:
        tray.AddSegment(BaseProcessing, "BaseProc",
                        pulses=filter_globals.CleanedMuonPulses,
                        decode=decode,
                        simulation=simulation,
                        do_slop=slop_split_enabled,
                        seededRTConfig=seededRTConfig,
                        needs_wavedeform_spe_corr=needs_wavedeform_spe_corr
                        )


class MergeOversampledEvents(icetray.I3ConditionalModule):

    def __init__(self, context):
        icetray.I3ConditionalModule.__init__(self, context)
        self.AddParameter('OversamplingFactor', 'Oversampling factor.', None)
        self.AddParameter('KeepKeys', 'Keys to keep.', None)

    def Configure(self):
        self.oversampling_factor = self.GetParameter('OversamplingFactor')
        self.keep_keys = self.GetParameter('KeepKeys')
        if self.keep_keys is None:
            self.keep_keys = []
        self.current_event_counter = None
        self.current_aggregation_frame = None
        self.oversampling_counter = None
        self.pushed_frame_already = False

    def Physics(self, frame):
        if 'oversampling' in frame:
            oversampling = frame['oversampling']

            # Find out if a new event started
            if (oversampling['event_num_in_run']
                    != self.current_event_counter):
                # new event started:
                # push aggregated frame if it hasn't been yet
                if (self.current_aggregation_frame is not None and
                        self.pushed_frame_already is False):
                    self.current_aggregation_frame['AggregatedPulses'] = \
                        self.merged_pulse_series
                    self.current_aggregation_frame['oversampling'].update(
                        {'num_aggregated_pulses': self.oversampling_counter})
                    self.PushFrame(self.current_aggregation_frame)

                # reset values for new event
                self.current_aggregation_frame = frame
                self.current_event_counter = oversampling['event_num_in_run']
                self.merged_pulse_series = frame['InIceDSTPulses'].apply(frame)
                self.oversampling_counter = 1
                self.pushed_frame_already = False

            else:
                # same event, keep aggregating pulses
                new_pulses = dataclasses.I3RecoPulseSeriesMap(
                                    frame['InIceDSTPulses'].apply(frame))
                self.merged_pulse_series.update(new_pulses)
                self.oversampling_counter += 1

            # Find out if event ended
            if (self.oversampling_factor
                    == 1 + oversampling['oversampling_num']):

                if self.current_aggregation_frame is not None:
                    self.current_aggregation_frame['AggregatedPulses'] = \
                        self.merged_pulse_series
                    self.current_aggregation_frame['oversampling'].update(
                        {'num_aggregated_pulses': self.oversampling_counter})
                    self.PushFrame(self.current_aggregation_frame)
                    self.pushed_frame_already = True

            # create copy of frame:
        else:
            self.PushFrame(frame)


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
    tray.AddModule('I3Reader',
                   'i3 reader',
                   FilenameList=[cfg['gcd_pass2'], infile])

    # get pulses
    tray.AddSegment(GetPulses, "GetPulses",
                    decode=False,
                    simulation=True,
                    )

    # merge oversampled events: calculate average hits
    if cfg['oversampling_factor'] is not None:
        tray.AddModule(MergeOversampledEvents, 'MergeOversampledEvents',
                       OversamplingFactor=cfg['oversampling_factor'],
                       KeepKeys=cfg['oversampling_keep_keys'])

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
