#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
#METAPROJECT XXXXX

import click
import yaml

from icecube.simprod import segments

from I3Tray import I3Tray
from icecube import icetray, dataclasses
from utils import create_random_services, create_filename


@click.command()
@click.argument('config_file', click.Path(exists=True))
@click.argument('run_number')
def main(cfg, run_number):
    with open(cfg, 'r') as stream:
        cfg = yaml.load(stream)
    cfg['run_number'] = run_number

    tray = I3Tray()

    random_service, random_service_prop = create_random_services(
        dataset_number=cfg['run_number'],
        run_number=cfg['dataset_number'],
        seed=cfg['seed'])

    if cfg['generator'].lower() == "nugen":
        tray.AddModule("I3InfiniteSource","TheSource",
                       Prefix=cfg['gcd'],
                       Stream=icetray.I3Frame.DAQ)
        tray.AddSegment(
            segments.GenerateNeutrinos,
            "GenerateNeutrinos",
            RandomService=random_service,
            NumEvents=cfg['n_events_per_run'],
            Flavor=cfg['nugen_flavor'],
            AutoExtendMuonVolume=cfg['nugen_autoextend'],
            GammaIndex=cfg['gamma'],
            FromEnergy=cfg['e_min'] * icetray.I3Units.GeV,
            ToEnergy=cfg['e_max'] * icetray.I3Units.GeV,)
    elif cfg['generator'].lower() == "muongun":
        tray.AddSegment(
            segments.GenerateCosmicRayMuons,
            "GenerateCosmicRayMuons",
            NumEvents=cfg['n_events_per_run'],
            FromEnergy=cfg['e_min'] * icetray.I3Units.GeV,
            ToEnergy=cfg['e_max'] * icetray.I3Units.GeV,
            BreakEnergy=cfg['muongun_e_break'] * icetray.I3Units.GeV,
            GammaIndex=cfg['gamma'],
            ZenithRange=[0., 180. * icetray.I3Units.deg])
    else:
        raise ValueError('This script only supports "muongun" and "nugen" '
                         'as generators.')

    tray.AddSegment(
        segments.PropagateMuons,
        "PropagateMuons",
        RandomService = random_service_prop)

    outfile = create_filename(cfg)
    tray.AddModule("I3Writer","writer",
        Filename=outfile,
        Streams=[icetray.I3Frame.DAQ,
                 icetray.I3Frame.Physics,
                 icetray.I3Frame.Stream('S'),
                 icetray.I3Frame.Stream('M')],
        )

if __name__ == '__main__':
    main()
