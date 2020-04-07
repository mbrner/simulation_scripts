#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
#METAPROJECT simulation/V05-01-01
import click
import yaml

from icecube.simprod import segments

from I3Tray import I3Tray
from icecube import icetray, dataclasses
from icecube import sim_services, MuonGun
import os
import sys
file_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(file_dir + '/..')
from utils import create_random_services, get_run_folder


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

    tray = I3Tray()

    random_services, _ = create_random_services(
        dataset_number=cfg['dataset_number'],
        run_number=cfg['run_number'],
        seed=cfg['seed'],
        n_services=2)

    random_service, random_service_prop = random_services

    tray.context['I3RandomService'] = random_service

    model = MuonGun.load_model(cfg['muongun_model'])
    model.flux.min_multiplicity = cfg['muongun_min_multiplicity']
    model.flux.max_multiplicity = cfg['muongun_max_multiplicity']
    spectrum = MuonGun.OffsetPowerLaw(cfg['gamma'],
                                      cfg['e_min']*icetray.I3Units.GeV,
                                      cfg['e_min']*icetray.I3Units.GeV,
                                      cfg['e_max']*icetray.I3Units.GeV)
    surface = MuonGun.Cylinder(1600, 800,
                               dataclasses.I3Position(31.25, 19.64, 0))

    if cfg['muongun_generator'] == 'energy':
        scale = MuonGun.BasicSurfaceScalingFunction()
        scale.SetSideScaling(4., 17266, 3.41, 1.74)
        scale.SetCapScaling(4., 23710, 3.40, 1.88)
        generator = MuonGun.EnergyDependentSurfaceInjector(surface,
                                                           model.flux,
                                                           spectrum,
                                                           model.radius,
                                                           scale)
    elif cfg['muongun_generator'] == 'static':
        generator = MuonGun.StaticSurfaceInjector(surface,
                                                  model.flux,
                                                  spectrum,
                                                  model.radius)
    elif cfg['muongun_generator'] == 'floodlight':
        generator = MuonGun.Floodlight(
                                surface=surface,
                                energyGenerator=spectrum,
                                cosMin=cfg['muongun_floodlight_min_cos'],
                                cosMax=cfg['muongun_floodlight_max_cos'],
                                )
    else:
        err_msg = 'MuonGun generator {} is not known.'
        err_msg += " Must be 'energy','static' or 'floodlight"
        raise ValueError(err_msg.format(cfg['muongun_generator']))

    tray.Add(MuonGun.segments.GenerateBundles, 'MuonGenerator',
             Generator=generator,
             NEvents=cfg['n_events_per_run'],
             GCDFile=cfg['gcd'])

    tray.Add("Rename", keys=["I3MCTree", "I3MCTree_preMuonProp"])

    tray.AddSegment(
        segments.PropagateMuons,
        "PropagateMuons",
        RandomService=random_service_prop,
        **cfg['muon_propagation_config'])
    if scratch:
        outfile = cfg['scratchfile_pattern'].format(**cfg)
    else:
        outfile = cfg['outfile_pattern'].format(**cfg)
    outfile = outfile.replace(' ', '0')
    if cfg['distance_splits'] is not None:
        click.echo('SplittingDistance: {}'.format(
            cfg['distance_splits']))
        distance_splits = np.atleast_1d(cfg['distance_splits'])
        dom_limits = np.atleast_1d(cfg['threshold_doms'])
        if len(dom_limits) == 1:
            dom_limits = np.ones_like(distance_splits) * cfg['threshold_doms']
        oversize_factors = np.atleast_1d(cfg['oversize_factors'])
        order = np.argsort(distance_splits)

        distance_splits = distance_splits[order]
        dom_limits = dom_limits[order]
        oversize_factors = oversize_factors[order]

        stream_objects = generate_stream_object(distance_splits,
                                                dom_limits,
                                                oversize_factors)
        tray.AddModule(OversizeSplitterNSplits,
                       "OversizeSplitterNSplits",
                       thresholds=distance_splits,
                       thresholds_doms=dom_limits,
                       oversize_factors=oversize_factors)
        for stream_i in stream_objects:
            outfile_i = stream_i.transform_filepath(outfile)
            tray.AddModule("I3Writer",
                           "writer_{}".format(stream_i.stream_name),
                           Filename=outfile_i,
                           Streams=[icetray.I3Frame.DAQ,
                                    icetray.I3Frame.Physics,
                                    icetray.I3Frame.Stream('S'),
                                    icetray.I3Frame.Stream('M')],
                           If=stream_i)
            click.echo('Output ({}): {}'.format(stream_i.stream_name,
                                                outfile_i))
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
