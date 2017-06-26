#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
#METAPROJECT simulation/V05-01-01

import click
import yaml

from icecube.simprod import segments

from I3Tray import I3Tray
from icecube import icetray, dataclasses, dataio, phys_services
from utils import create_random_services


MAX_PARALLEL_EVENTS = 100
SPLINE_TABLES = '/cvmfs/icecube.opensciencegrid.org/data/photon-tables/splines'


@click.command()
@click.argument('cfg', click.Path(exists=True))
@click.argument('run_number', type=int)
@click.option('--scratch/--no-scratch', default=True)
@click.option('--low_oversize/--no-low_oversize', default=False)
@click.option('--high_oversize/--no-high_oversize', default=False)
def main(cfg, run_number, scratch, low_oversize, high_oversize):
    with open(cfg, 'r') as stream:
        cfg = yaml.load(stream)
    cfg['run_number'] = run_number
    infile = cfg['infile_pattern'].format(run_number=run_number)
    infile = infile.replace(' ', '0')

    if scratch:
        outfile = cfg['scratchfile_pattern'].format(run_number=run_number)
    else:
        outfile = cfg['outfile_pattern'].format(run_number=run_number)
    outfile = outfile.replace(' ', '0')
    if low_oversize:
        dom_oversize = cfg['clsim_low_dom_oversize']
        infile = infile.replace('i3.bz2', 'low_oversize.i3.bz2')
        outfile = outfile.replace('i3.bz2', 'low_oversize.i3.bz2')
    elif high_oversize:
        dom_oversize = cfg['clsim_high_dom_oversize']
        infile = infile.replace('i3.bz2', 'high_oversize.i3.bz2')
        outfile = outfile.replace('i3.bz2', 'high_oversize.i3.bz2')
    else:
        dom_oversize = cfg['clsim_dom_oversize']

    click.echo('Input: {}'.format(infile))

    hybrid_mode = (cfg['clsim_hybrid_mode'] and
                   cfg['icemodel'].lower() != 'spicelea')
    ignore_muon_light = (cfg['clsim_ignore_muon_light'] and
                         cfg['clsim_hybrid_mode'])
    click.echo('UseGPUs: {}'.format(cfg['clsim_usegpus']))
    click.echo('IceModel: {}'.format(cfg['icemodel']))
    click.echo('DomOversize {}'.format(dom_oversize))
    click.echo('LowOversize: {}'.format(low_oversize))
    click.echo('HighOversize: {}'.format(high_oversize))
    click.echo('UnshadowedFraction: {0:.2f}'.format(
        cfg['clsim_unshadowed_fraction']))
    click.echo('HybridMode: {}'.format(hybrid_mode))
    click.echo('IgnoreMuonLight: {}'.format(ignore_muon_light))
    click.echo('KeepMCPE: {}'.format(cfg['clsim_keep_mcpe']))
    click.echo('Output: {}'.format(outfile))
    click.echo('Scratch: {}'.format(scratch))
    tray = I3Tray()

    tray.context['I3FileStager'] = dataio.get_stagers()

    random_service, _, _ = create_random_services(
        dataset_number=cfg['run_number'],
        run_number=cfg['dataset_number'],
        seed=cfg['seed'])

    tray.context['I3RandomService'] = random_service

    tray.Add('I3Reader', FilenameList=[cfg['gcd'], infile])

    if hybrid_mode:
        cascade_tables = segments.LoadCascadeTables(IceModel=cfg['icemodel'],
                                                    TablePath=SPLINE_TABLES)
    else:
        cascade_tables = None

    if cfg['clsim_usegpus']:
        use_gpus = True
        use_cpus = False
    else:
        use_gpus = True
        use_cpus = False

    tray.AddSegment(
        segments.PropagatePhotons,
        "PropagatePhotons",
        RandomService=random_service,
        MaxParallelEvents=MAX_PARALLEL_EVENTS,
        KeepIndividualMaps=cfg['clsim_keep_mcpe'],
        IceModel=cfg['icemodel'],
        UnshadowedFraction=cfg['clsim_unshadowed_fraction'],
        IgnoreMuons=ignore_muon_light,
        HybridMode=hybrid_mode,
        UseGPUs=use_gpus,
        UseCPUs=use_cpus,
        DOMOversizeFactor=dom_oversize,
        CascadeService=cascade_tables)

    outfile = outfile.replace(' ', '0')
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
