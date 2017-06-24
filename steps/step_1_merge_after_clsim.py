#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
#METAPROJECT simulation/V05-01-01

import click
import yaml

from icecube.simprod import segments

from I3Tray import I3Tray
from icecube import icetray, dataclasses, dataio, phys_services
from utils import create_random_services


def filter_S_frame(frame):
    if not filter_S_frame.already_added:
        filter_S_frame.already_added = True
        return True
    else:
        return False

filter_S_frame.already_added = False


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
    outfile = outfile.replace(' ', '0')
    infile_low_oversize = outfile.replace('i3.bz2', 'low_oversize.i3.bz2')
    infile_high_oversize = outfile.replace('i3.bz2', 'high_oversize.i3.bz2')
    tray = I3Tray()

    tray.context['I3FileStager'] = dataio.get_stagers()

    tray.Add('I3Reader', FilenameList=[infile_low_oversize,
                                       infile_high_oversize])
    outfile = outfile.replace(' ', '0')
    tray.AddModule(filter_S_frame,
                   'S Frame Filter',
                   Streams=[icetray.I3Frame.Stream('S')])

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
