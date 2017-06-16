#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
#METAPROJECT XXXXX

import click
import yaml

from icecube.simprod import segments

from I3Tray import I3Tray
from icecube import icetray, dataclasses, dataio, phys_services
from utils import create_random_services, create_filename


MCPE_SERIES_MAP = 'I3MCPESeriesMap'


@click.command()
@click.argument('config_file', click.Path(exists=True))
@click.argument('run_number')
def main(cfg, run_number):
    with open(cfg, 'r') as stream:
        cfg = yaml.load(stream)
    cfg['run_number'] = run_number

    infile = create_filename(cfg, previous=True)

    tray = I3Tray()

    tray.context['I3FileStager'] = dataio.get_stagers()

    random_service, _ = create_random_services(
        dataset_number=cfg['run_number'],
        run_number=cfg['dataset_number'],
        seed=cfg['seed'])

    tray.context['I3RandomService'] = random_service

    tray.Add('I3Reader', FilenameList=[cfg['gcd'], infile])

    tray.AddSegment(segments.DetectorSim, "DetectorSim",
        RandomService='I3RandomService',
        RunID=cfg['run_number'],
        GCDFile=cfg['gcd'],
        InputPESeriesMapName=MCPE_SERIES_MAP,
        KeepMCHits=False,
        KeepPropagatedMCTree=True,
        SkipNoiseGenerator=False)

    outfile = create_filename(cfg)
    tray.AddModule("I3Writer","writer",
        Filename=outfile,
        Streams=[icetray.I3Frame.DAQ,
                 icetray.I3Frame.Physics])

if __name__ == '__main__':
    main()
