#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
#METAPROJECT simulation/V05-01-01
import os

import click
import yaml

from icecube.simprod import segments
from I3Tray import I3Tray
from icecube import icetray, dataclasses, dataio, phys_services
from utils import create_random_services, get_run_folder


MCPE_SERIES_MAP = 'I3MCPESeriesMap'
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

    random_service, _, run_id = create_random_services(
        dataset_number=cfg['run_number'],
        run_number=cfg['dataset_number'],
        seed=cfg['seed'])

    tray.context['I3RandomService'] = random_service

    tray.Add('I3Reader', FilenameList=[cfg['gcd'], infile])
    tray.AddSegment(segments.DetectorSim, "DetectorSim",
        RandomService='I3RandomService',
        RunID=run_id,
        GCDFile=cfg['gcd'],
        KeepMCHits=cfg['det_keep_mc_hits'],
        KeepPropagatedMCTree=cfg['det_keep_propagated_mc_tree'],
        KeepMCPulses=cfg['det_keep_mc_pulses'],
        SkipNoiseGenerator=cfg['det_skip_noise_generation'],
        LowMem=cfg['det_low_mem'],
        InputPESeriesMapName=MCPE_SERIES_MAP,
        BeaconLaunches=cfg['det_add_beacon_launches'],
        FilterTrigger=cfg['det_filter_trigger'])

    if scratch:
        outfile = cfg['scratchfile_pattern'].format(**cfg)
    else:
        outfile = cfg['outfile_pattern'].format(**cfg)
    outfile = outfile.replace(' ', '0')

    print(outfile)
    print(cfg['outfile_pattern'])
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
