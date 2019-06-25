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
from icecube import filter_tools

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
        self.AddParameter('PulseKey', 'The pulse key over which to aggregate.',
                          'InIceDSTPulses')
        self.AddParameter('MinPulseTimeSeparation',
                          'The minimum time in ns a merged pulse may be '
                          'separated from a previous one. If it is less, then '
                          'the two pulses will be merged into a single one',
                          1.)

    def Configure(self):
        self.oversampling_factor = self.GetParameter('OversamplingFactor')
        self.pulse_key = self.GetParameter('PulseKey')
        self.min_separation = self.GetParameter('MinPulseTimeSeparation')
        self.current_time_shift = None
        self.current_event_counter = None
        self.current_aggregation_frame = None
        self.current_daq_frame = None
        self.oversampling_counter = None
        self.pushed_frame_already = False

    def push_aggregated_frame(self):

        # adjust charges of pulses
        for om_key in self.merged_pulse_series.keys():
            for pulse in self.merged_pulse_series[om_key]:
                pulse.charge /= self.oversampling_counter

        self.current_aggregation_frame['AggregatedPulses'] = \
            self.merged_pulse_series

        # update oversampling dictionary
        dic = dict(self.current_aggregation_frame['oversampling'])
        del self.current_aggregation_frame['oversampling']
        dic['num_aggregated_pulses'] = self.oversampling_counter
        dic['time_shift'] = self.current_time_shift
        self.current_aggregation_frame['oversampling'] = \
            dataclasses.I3MapStringDouble(dic)

        self.PushFrame(self.current_daq_frame)
        self.PushFrame(self.current_aggregation_frame)
        self.current_daq_frame = None
        self.pushed_frame_already = True

    def merge_pulse_series(self, pulse_series, new_pulses, time_shift):
        """Merge two pulse series.

        Assumes that new_pulses are to be merged into existing pulse_series,
        e.g. new_pulses are smaller than pulse_series

        Parameters
        ----------
        pulse_series : dataclasses.I3RecoPulseSeriesMap
            Pulse series map to which the new pulses will be added
        new_pulses : dataclasses.I3RecoPulseSeriesMap
            New pulse series that will be merged into the existing pulse
            series.
        time_shift : float
            The time shift of the new pulses.

        Returns
        -------
        dataclasses.I3RecoPulseSeriesMap
            Merged pulse series map.
        """
        pulse_series = dataclasses.I3RecoPulseSeriesMap(pulse_series)

        # calculate relative time difference to first oversampling frame
        delta_t = time_shift - self.current_time_shift

        # Go through every key in new pulses:
        for key, new_hits in new_pulses:
            if key not in pulse_series:
                # DOM has not been previously hit: just merge all hits
                pulse_series[key] = new_hits

                # correct times:
                for new_hit in pulse_series[key]:
                    new_hit.time += delta_t
            else:
                # DOM already has hits:
                #   now need to merge new pulses in existing series
                # Loop through existing pulses and sort them in
                merged_hits = list(pulse_series[key])
                len_merged_hits = len(merged_hits)
                index = 0
                for new_hit in new_hits:
                    pulse_is_merged = False
                    combine_pulses = False

                    # correct for relative time shift difference
                    new_hit.time += delta_t

                    # sort the pulse into existing list
                    while not pulse_is_merged:
                        if (index >= len(merged_hits) or
                                new_hit.time < merged_hits[index].time):

                            time_diff = abs(
                                new_hit.time - merged_hits[index - 1].time)
                            combine_pulses = time_diff < self.min_separation
                            if combine_pulses:
                                # the pulses are close in time: merge
                                merged_hits[index - 1].charge += new_hit.charge
                            else:
                                # insert pulse
                                merged_hits.insert(index, new_hit)
                                len_merged_hits += 1

                            pulse_is_merged = True

                        # only increase index if we did not combine 2 pulses
                        if not combine_pulses:
                            index += 1

                # overwrite old pulse series
                pulse_series[key] = merged_hits

                # # sanity check
                # assert len(pulse_series[key]) == len_merged_hits

            # # sanity checks
            # t_previous = pulse_series[key][0].time
            # for p in pulse_series[key][1:]:
            #     assert p.time >= t_previous
            #     t_previous = p.time

        return pulse_series

    def _get_pulses(self, frame):
        """Get the I3RecoPulseSeriesMap from the frame.

        Parameters
        ----------
        frame : I3Frame
            The current I3Frame.
        """
        pulses = frame[self.pulse_key]
        if isinstance(pulses, dataclasses.I3RecoPulseSeriesMapMask) or \
                isinstance(pulses, dataclasses.I3RecoPulseSeriesMapUnion):
            pulses = pulses.apply(frame)
        return dataclasses.I3RecoPulseSeriesMap(pulses)

    def DAQ(self, frame):
        if self.current_daq_frame is None:
            self.current_daq_frame = frame

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
                    self.push_aggregated_frame()

                # reset values for new event
                self.current_time_shift = frame['TimeShift'].value
                self.current_aggregation_frame = frame
                self.current_event_counter = oversampling['event_num_in_run']
                self.merged_pulse_series = self._get_pulses(frame)
                self.oversampling_counter = 1
                self.pushed_frame_already = False

            else:
                # same event, keep aggregating pulses
                new_pulses = self._get_pulses(frame)
                self.merged_pulse_series = self.merge_pulse_series(
                                        self.merged_pulse_series,
                                        new_pulses,
                                        frame['TimeShift'].value)
                self.oversampling_counter += 1

            # Find out if event ended
            if (self.oversampling_factor
                    == 1 + oversampling['oversampling_num']):

                if self.current_aggregation_frame is not None:
                    self.push_aggregated_frame()

        else:
            self.PushFrame(frame)


@click.command()
@click.argument('cfg', click.Path(exists=True))
@click.argument('run_number', type=int)
@click.option('--scratch/--no-scratch', default=True)
@click.argument('do_merging_if_necessary', type=bool, default=True)
def main(cfg, run_number, scratch, do_merging_if_necessary):
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

    # Throw out unneeded streams and keys
    if 'oversampling_keep_keys' not in cfg:
        oversampling_keep_keys = []
    elif cfg['oversampling_keep_keys'] is None:
        oversampling_keep_keys = []

    if cfg['L1_keep_untriggered']:
        stream_name = filter_globals.InIceSplitter
    else:
        stream_name = filter_globals.NullSplitter
    tray.AddModule("KeepFromSubstream", "DeleteSubstream",
                   StreamName=stream_name,
                   KeepKeys=['do_not_keep_anything'])

    # merge oversampled events: calculate average hits
    if cfg['oversampling_factor'] is not None and do_merging_if_necessary:
        if 'oversampling_merge_events' in cfg:
            merge_events = cfg['oversampling_merge_events']
        else:
            # backward compability
            merge_events = True

        if merge_events:
            tray.AddModule(MergeOversampledEvents, 'MergeOversampledEvents',
                           OversamplingFactor=cfg['oversampling_factor'])
    keys_to_keep = [
        'TimeShift',
        'I3MCTree_preMuonProp',
        'I3MCTree',
        'MMCTrackList',
        'I3EventHeader',
        'I3SuperDST',
        'RNGState',
        'oversampling',
        'AggregatedPulses',
        'InIceDSTPulses',
        'CalibrationErrata',
        'SaturationWindows',
        'SplitUncleanedInIcePulses',
        'SplitUncleanedInIcePulsesTimeRange',
        'SplitUncleanedInIceDSTPulsesTimeRange',
        'I3TriggerHierarchy',
        'GCFilter_GCFilterMJD',
        ]
    keys_to_keep += filter_globals.inice_split_keeps + \
        filter_globals.onlinel2filter_keeps

    tray.AddModule("Keep", "keep_before_merge",
                   keys=keys_to_keep + cfg['oversampling_keep_keys'])

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
