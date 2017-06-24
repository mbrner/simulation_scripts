#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
#METAPROJECT simulation/V05-01-01
import click
import yaml

from icecube.simprod import segments

from I3Tray import I3Tray
from icecube import icetray, dataclasses
from icecube import sim_services, MuonGun

from utils import create_random_services
from dom_distance_cut import low_oversize_stream, high_oversize_stream
from dom_distance_cut import OversizeSplitter


@click.command()
@click.argument('cfg', click.Path(exists=True))
@click.argument('run_number', type=int)
@click.option('--scratch/--no-scratch', default=True)
def main(cfg, run_number, scratch):
    with open(cfg, 'r') as stream:
        cfg = yaml.load(stream)
    cfg['run_number'] = run_number

    if scratch:
        outfile = cfg['scratchfile_pattern'].format(run_number=run_number)
    else:
        outfile = cfg['outfile_pattern'].format(run_number=run_number)

    click.echo('SplittingDistance: {}'.format(
               cfg['photon_propagation_dist_threshold']))
    click.echo('NEvents: {}'.format(cfg['n_events_per_run']))
    click.echo('EMin: {}'.format(cfg['e_min']))
    click.echo('EMax: {}'.format(cfg['e_max']))
    click.echo('EBreak: {}'.format(cfg['muongun_e_break']))
    click.echo('Gamma: {}'.format(cfg['gamma']))
    click.echo('ZenithMin: {}'.format(cfg['zenith_min']))
    click.echo('ZenithMax: {}'.format(cfg['zenith_max']))

    tray = I3Tray()

    random_service, random_service_prop, _ = create_random_services(
        dataset_number=run_number,
        run_number=cfg['dataset_number'],
        seed=cfg['seed'])

    tray.context['I3RandomService'] = random_service

    tray.AddModule("I3InfiniteSource",
                   "TheSource",
                   Prefix=cfg['gcd'],
                   Stream=icetray.I3Frame.DAQ)

    tray.AddSegment(
        segments.GenerateSingleMuons,
        "GenerateCosmicRayMuons",
        NumEvents=cfg['n_events_per_run'],
        FromEnergy=cfg['e_min'] * icetray.I3Units.GeV,
        ToEnergy=cfg['e_max'] * icetray.I3Units.GeV,
        BreakEnergy=cfg['muongun_e_break'] * icetray.I3Units.GeV,
        GammaIndex=cfg['gamma'],
        ZenithRange=[cfg['zenith_min'] * icetray.I3Units.deg,
                     cfg['zenith_max'] * icetray.I3Units.deg])

    tray.AddSegment(
        segments.PropagateMuons,
        "PropagateMuons",
        RandomService=random_service_prop)
    if scratch:
        outfile = cfg['scratchfile_pattern'].format(run_number=run_number)
    else:
        outfile = cfg['outfile_pattern'].format(run_number=run_number)
    outfile = outfile.replace(' ', '0')
    if cfg['photon_propagation_dist_threshold'] is not None:
        click.echo('SplittingDistance: {}'.format(
            cfg['photon_propagation_dist_threshold']))
        tray.AddModule(OversizeSplitter,
                       "OversizeSplitter",
                       threshold=cfg['photon_propagation_dist_threshold'])
        outfile_low_oversize = outfile.replace('i3.bz2',
                                               'low_oversize.i3.bz2')
        outfile_high_oversize = outfile.replace('i3.bz2',
                                                'high_oversize.i3.bz2')
        tray.AddModule("I3Writer", "writer_low_oversize",
                       Filename=outfile_low_oversize,
                       Streams=[icetray.I3Frame.DAQ,
                                icetray.I3Frame.Physics,
                                icetray.I3Frame.Stream('S'),
                                icetray.I3Frame.Stream('M')],
                       If=high_oversize_stream)
        tray.AddModule("I3Writer", "writer_high_oversize",
                       Filename=outfile_high_oversize,
                       Streams=[icetray.I3Frame.DAQ,
                                icetray.I3Frame.Physics,
                                icetray.I3Frame.Stream('S'),
                                icetray.I3Frame.Stream('M')],
                       If=low_oversize_stream)
        click.echo('Output1: {}'.format(outfile_low_oversize))
        click.echo('Output2: {}'.format(outfile_high_oversize))
    else:
        click.echo('Output: {}'.format(outfile))
        tray.AddModule("I3Writer", "writer",
                       Filename=outfile,
                       Streams=[icetray.I3Frame.DAQ,
                                icetray.I3Frame.Physics,
                                icetray.I3Frame.Stream('S'),
                                icetray.I3Frame.Stream('M')])
    click.echo('Scratch: {}'.format(scratch))
    tray.AddModule("TrashCan", "the can")
    tray.Execute()
    tray.Finish()


if __name__ == '__main__':
    main()
