#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
#METAPROJECT simulation/V05-01-01

import click
import yaml

from icecube.simprod import segments

from I3Tray import I3Tray
from icecube import icetray, dataclasses, dataio, phys_services
from utils import create_random_services


MCPE_SERIES_MAP = 'I3MCPESeriesMap'


@click.command()
@click.argument('cfg', click.Path(exists=True))
@click.argument('run_number', type=int)
@click.option('--scratch/--no-scratch', default=True)
def main(cfg, run_number, scratch):
    with open(cfg, 'r') as stream:
        cfg = yaml.load(stream)
    cfg['run_number'] = run_number

    infile = cfg['infile_pattern'].format(run_number=run_number)
    infile = infile.replace(' ', '0')

    tray = I3Tray()

    tray.context['I3FileStager'] = dataio.get_stagers()

    random_service, _, run_number = create_random_services(
        dataset_number=cfg['run_number'],
        run_number=cfg['dataset_number'],
        seed=cfg['seed'])

    tray.context['I3RandomService'] = random_service

    tray.Add('I3Reader', FilenameList=[cfg['gcd'], infile])

    tray.AddSegment(segments.DetectorSim, "DetectorSim",
        RandomService='I3RandomService',
        RunID=run_number,
        GCDFile=cfg['gcd'],
        InputPESeriesMapName=MCPE_SERIES_MAP,
        KeepMCHits=False,
        KeepPropagatedMCTree=True,
        SkipNoiseGenerator=False)

    if scratch:
        outfile = cfg['scratchfile_pattern'].format(run_number=run_number)
    else:
        outfile = cfg['outfile_pattern'].format(run_number=run_number)
    outfile = outfile.replace(' ', '0')
    tray.AddModule("I3Writer","writer",
        Filename=outfile,
        Streams=[icetray.I3Frame.DAQ,
                 icetray.I3Frame.Physics])

if __name__ == '__main__':
    main()
