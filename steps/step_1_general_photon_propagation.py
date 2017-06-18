#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
#METAPROJECT simulation/V05-01-01

import click
import yaml

from icecube.simprod import segments

from I3Tray import I3Tray
from icecube import icetray, dataclasses, dataio, phys_services
from utils import create_random_services


MAX_PARALLEL_EVENTS = 1


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

    random_service, _, _ = create_random_services(
        dataset_number=cfg['run_number'],
        run_number=cfg['dataset_number'],
        seed=cfg['seed'])

    tray.context['I3RandomService'] = random_service

    tray.Add('I3Reader', FilenameList=[cfg['gcd'], infile])



    hybrid_mode = (cfg['clsim_hybrid_mode'] and
                   cfg['icemodel'].lower() != 'spicelea')
    ignore_muon_light = (cfg['clsim_ignore_muon_light'] and
                         cfg['clsim_hybrid_mode'])
    click.echo('HybridMode: {}'.format(hybrid_mode))
    click.echo('IgnoreMuonLight: {}'.format(ignore_muon_light))

    tray.AddSegment(
        segments.PropagatePhotons,
        "PropagatePhotons",
        RandomService=random_service,
        MaxParallelEvents=MAX_PARALLEL_EVENTS,
        HybridMode=cfg['icemodel'].lower() in ['spcie1', 'spicemie'],
        KeepIndividualMaps=cfg['clsim_keep_mcpe'],
        IceModel=cfg['icemodel'],
        UnshadowedFraction=cfg['clsim_unshadowed_fraction'],
        IgnoreMuons=ignore_muon_light,
        HybridMode=hybrid_mode,
        UseGPUs=cfg['clsim_usegpus'],
        DOMOversizeFactor=cfg['clsim_dom_oversize'])

    if scratch:
        outfile = cfg['scratchfile_pattern'].format(run_number=run_number)
    else:
        outfile = cfg['outfile_pattern'].format(run_number=run_number)
    outfile = outfile.replace(' ', '0')
    tray.AddModule("I3Writer","writer",
        Filename=outfile,
        Streams=[icetray.I3Frame.DAQ,
                 icetray.I3Frame.Physics,
                 icetray.I3Frame.Stream('S'),
                 icetray.I3Frame.Stream('M')],
        )
    tray.AddModule("TrashCan", "the can")
    tray.Execute()
    tray.Finish()


if __name__ == '__main__':
    main()
