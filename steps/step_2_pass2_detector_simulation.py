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

    tray.context['I3FileStager'] = dataio.get_stagers()

    random_services, run_id = create_random_services(
        dataset_number=cfg['dataset_number'],
        run_number=cfg['run_number'],
        seed=cfg['seed'],
        n_services=1)
    random_service = random_services[0]
    tray.context['I3RandomService'] = random_service

    tray.Add('I3Reader', FilenameList=[cfg['gcd_pass2'], infile])

    if run_number < cfg['det_pass2_keep_all_upto']:
        cfg['det_keep_mc_hits'] = True
        cfg['det_keep_propagated_mc_tree'] = True
        cfg['det_keep_mc_pulses'] = True

    tray.AddSegment(segments.DetectorSim, "Detector5Sim",
        RandomService='I3RandomService',
        RunID=run_id,
        GCDFile=cfg['gcd_pass2'],
        KeepMCHits=cfg['det_keep_mc_hits'],
        KeepPropagatedMCTree=cfg['det_keep_propagated_mc_tree'],
        KeepMCPulses=cfg['det_keep_mc_pulses'],
        SkipNoiseGenerator=cfg['det_skip_noise_generation'],
        LowMem=cfg['det_low_mem'],
        InputPESeriesMapName=MCPE_SERIES_MAP,
        BeaconLaunches=cfg['det_add_beacon_launches'],
        FilterTrigger=cfg['det_filter_trigger'])
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
