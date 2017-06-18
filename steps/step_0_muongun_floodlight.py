#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
#METAPROJECT simulation/trunk
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
from utils import create_random_services


@click.command()
@click.argument('cfg', click.Path(exists=True))
@click.argument('run_number', type=int)
@click.option('--scratch/--no-scratch', default=True)
def main(cfg, run_number, scratch):
    with open(cfg, 'r') as stream:
        cfg = yaml.load(stream)
    cfg['run_number'] = run_number

    tray = I3Tray()

    random_service, random_service_prop, _ = create_random_services(
        dataset_number=run_number,
        run_number=cfg['dataset_number'],
        seed=cfg['seed'])

    tray.context['I3RandomService'] = random_service

    if cfg['generator'].lower() == "nugen":
        tray.AddModule("I3InfiniteSource",
                       "TheSource",
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
        # tray.AddSegment(
        #     segments.GenerateSingleMuons,
        #     "GenerateCosmicRayMuons",
        #     NumEvents=cfg['n_events_per_run'],
        #     FromEnergy=cfg['e_min'] * icetray.I3Units.GeV,
        #     ToEnergy=cfg['e_max'] * icetray.I3Units.GeV,
        #     BreakEnergy=cfg['muongun_e_break'] * icetray.I3Units.GeV,
        #     GammaIndex=cfg['gamma'],
        #     ZenithRange=[0., 180. * icetray.I3Units.deg])

        model = MuonGun.load_model(cfg['muongun_model'])
        model.flux.min_multiplicity = cfg['muongun_min_multiplicity']
        model.flux.max_multiplicity = cfg['muongun_max_multiplicity']
        spectrum = MuonGun.OffsetPowerLaw(  cfg['gamma'], 
                                            cfg['e_min']*icetray.I3Units.TeV, 
                                            cfg['e_min']*icetray.I3Units.TeV, 
                                            cfg['e_max']*icetray.I3Units.TeV)
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
        elif cfg['muongun_generator'] =='floodlight':
            generator = MuonGun.Floodlight(surface = surface, 
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

        def renameMCTree(frame):
            mctree = frame["I3MCTree"]
            del frame["I3MCTree"]
            frame["I3MCTree_preMuonProp"] = mctree
        tray.AddModule(renameMCTree, "RenameMCTree", Streams=[icetray.I3Frame.DAQ])

    else:
        raise ValueError('This script only supports "muongun" and "nugen" '
                         'as generators.')

    tray.AddSegment(
        segments.PropagateMuons,
        "PropagateMuons",
        RandomService=random_service_prop)
    if scratch:
        outfile = cfg['scratchfile_pattern'].format(run_number=run_number)
    else:
        outfile = cfg['outfile_pattern'].format(run_number=run_number)
    outfile = outfile.replace(' ', '0')
    outfile = outfile.replace('.bz2', '')
    tray.AddModule("I3Writer", "writer",
                   Filename=outfile,
                   Streams=[icetray.I3Frame.DAQ,
                            icetray.I3Frame.Physics,
                            icetray.I3Frame.Stream('S'),
                            icetray.I3Frame.Stream('M')])

    tray.AddModule("TrashCan", "the can")
    tray.Execute()
    tray.Finish()


if __name__ == '__main__':
    main()
