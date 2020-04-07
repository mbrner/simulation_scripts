#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
#METAPROJECT icerec/V05-01-06
import os
import sys
import time

import click
import yaml

from I3Tray import I3Tray, I3Units
from icecube import icetray, dataclasses, simclasses
from icecube.filterscripts import filter_globals
from icecube.filterscripts.baseproc import BaseProcessing
from icecube.STTools.seededRT.configuration_services import \
    I3DOMLinkSeededRTConfigurationService
from icecube import filter_tools

from utils import get_run_folder
from step_3_pass2_get_pulses import MergeOversampledEvents


class GetMCPulses(icetray.I3ConditionalModule):

    """Creates I3RecoPulseSeriesMap from I3MCPESeriesMap and optionally
    creates and inserts new Physics-frames.
    """

    def __init__(self, context):
        icetray.I3ConditionalModule.__init__(self, context)
        self.AddParameter('I3MCPESeriesMap', 'I3MCPESeriesMap to use.',
                          'I3MCPESeriesMapWithoutNoise')
        self.AddParameter('OutputKey',
                          'Output key to which the MC hits will be stored.',
                          'MCPulses')
        self.AddParameter('CreatePFrames', 'Create P frames from q frames?.',
                          True)

    def Configure(self):
        """Configure the module.
        """
        self._mcpe_series = self.GetParameter('I3MCPESeriesMap')
        self._output_key = self.GetParameter('OutputKey')
        self._create_p_frames = self.GetParameter('CreatePFrames')

        assert isinstance(self._create_p_frames, bool), \
            'Expected CreatePFrames to be a boolean, but got {!r}'.format(
                self._create_p_frames)

    def DAQ(self, frame):
        """Create P-frames and add MC pulses.

        Parameters
        ----------
        frame : I3Frame
            The current I3Frame.
        """
        self.PushFrame(frame)

        if self._create_p_frames:
            p_frame = icetray.I3Frame(icetray.I3Frame.Physics)

            # add MC reco pulses from I3MCPESeriesMap
            self._add_mc_pulses(p_frame, frame[self._mcpe_series])

            # Detector simulation creates trigger and shifts times relative
            # to this trigger. If detector simulation is skipped,
            # we must manually add the TimeShift key to the frame.
            if 'TimeShift' not in frame:
                p_frame['TimeShift'] = dataclasses.I3Double(0.)
            self.PushFrame(p_frame)

    def Physics(self, frame):
        """Add MC pulses to P-frame

        Parameters
        ----------
        frame : I3Frame
            The current I3Frame.
        """
        if not self._create_p_frames:

            # add MC reco pulses from I3MCPESeriesMap
            self._add_mc_pulses(frame, frame[self._mcpe_series])

        # push frame on to next modules
        self.PushFrame(frame)

    def _add_mc_pulses(self, frame, mcpe_series_map):
        '''Create MC reco pulses from I3MCPESeriesMap

        This is a dirty hack, so that other modules can be used without
        changing them. However, this will use up unecessary space, because
        I3RecoPulses have more data fields, which are not required by an
        MC hit (width, ATWD, ...) .

        Parameters
        ----------
        frame : I3Frame
            The I3Frame to which the MC Pulses will be added to.
        mcpe_series_map : I3MCPESeriesMap
            The I3MCPESeriesMap which will be converted.
        '''
        mc_pulse_map = dataclasses.I3RecoPulseSeriesMap()
        for omkey, mcpe_series in mcpe_series_map.items():

            mc_pulses = []
            for mcpe in mcpe_series:

                # create I3RecoPulse with corresponding time and 'charge'
                # The charge is set to the number of photo electrons (npe)
                mc_pulse = dataclasses.I3RecoPulse()
                mc_pulse.time = mcpe.time
                mc_pulse.charge = mcpe.npe

                # append pulse
                mc_pulses.append(mc_pulse)

            mc_pulse_map[omkey] = dataclasses.vector_I3RecoPulse(mc_pulses)

        # write to frame
        frame[self._output_key] = mc_pulse_map


@click.command()
@click.argument('cfg', type=click.Path(exists=True))
@click.argument('run_number', type=int)
@click.option('--scratch/--no-scratch', default=True)
@click.argument('do_merging_if_necessary', type=bool, default=True)
def main(cfg, run_number, scratch, do_merging_if_necessary):
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
    tray.AddModule('I3Reader',
                   'i3 reader',
                   FilenameList=[cfg['gcd_pass2'], infile])

    # get MC pulses
    tray.AddModule(GetMCPulses, "GetMCPulses",
                   I3MCPESeriesMap='I3MCPESeriesMapWithoutNoise',
                   OutputKey='MCPulses',
                   CreatePFrames=True)

    # merge oversampled events: calculate average hits
    if cfg['oversampling_factor'] is not None and do_merging_if_necessary:
        if 'oversampling_merge_events' in cfg:
            merge_events = cfg['oversampling_merge_events']
        else:
            # backward compability
            merge_events = True

        if merge_events:
            tray.AddModule(MergeOversampledEvents, 'MergeOversampledEvents',
                           OversamplingFactor=cfg['oversampling_factor'],
                           PulseKey='MCPulses')

    # Make space and delete uneeded keys
    keys_to_delete = [
        'I3MCPESeriesMap',
        'I3MCPulseSeriesMap',
        'I3MCPESeriesMapWithoutNoise',
        'I3MCPulseSeriesMapParticleIDMap',
        'I3MCPulseSeriesMapPrimaryIDMap',
        'InIceRawData',
        'IceTopRawData',
        ]
    tray.AddModule('Delete', 'DeleteKeys',
                   keys=keys_to_delete)

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
