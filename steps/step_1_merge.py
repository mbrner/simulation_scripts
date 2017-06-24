#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
#METAPROJECT simulation/V05-01-01

import click
import yaml

from icecube.simprod import segments

from I3Tray import I3Tray
from icecube import icetray, dataclasses, dataio, phys_services
from utils import create_random_services


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
    infile_no_oversize = infile.replace('i3.gz2', 'no_oversize.i3.gz2')
    infile_oversize = infile.replace('i3.gz2', 'oversize.i3.gz2')
    tray = I3Tray()

    tray.context['I3FileStager'] = dataio.get_stagers()

    tray.Add('I3Reader', FilenameList=[infile_no_oversize,
                                       infile_oversize])

    if scratch:
        outfile = cfg['scratchfile_pattern'].format(run_number=run_number)
    else:
        outfile = cfg['outfile_pattern'].format(run_number=run_number)
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
