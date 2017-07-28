#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v1/icetray-start
#METAPROJECT /home/mboerner/software/i3/IC2012-L2_V13-01-00_IceSim04-01-10compat/build
import click
import yaml

from I3Tray import I3Tray
from icecube import icetray, dataclasses, dataio
from icecube.icetray import I3PacketModule

import os
import sys
import subprocess

from I3Tray import I3Tray
from icecube import icetray, dataclasses, dataio
from icecube.icetray import I3PacketModule, I3Units

from icecube.filterscripts.offlineL2.level2_all_filters import OfflineFilter
from icecube.filterscripts.offlineL2 import SpecialWriter

from utils import get_run_folder


PHOTONICS_DIR = '/cvmfs/icecube.opensciencegrid.org/data/photon-tables'


@click.command()
@click.argument('cfg', click.Path(exists=True))
@click.argument('run_number', type=int)
@click.option('--scratch/--no-scratch', default=True)
def main(cfg, run_number, scratch):
    with open(cfg, 'r') as stream:
        cfg = yaml.load(stream)
    cfg['run_number'] = run_number
    cfg['run_folder'] = get_run_folder(run_number)

    infile = cfg['infile_pattern'].format(**cfg)
    infile = infile.replace(' ', '0')

    tray = I3Tray()
    """The main L1 script"""
    tray.AddModule('I3Reader',
                   'i3 reader',
                   FilenameList=[cfg['gcd_pass2'], infile])

    tray.AddSegment(OfflineFilter, "OfflineFilter",
        dstfile=None,
        mc=True,
        doNotQify=True,
        photonicsdir=PHOTONICS_DIR
        )

    if scratch:
        outfile = cfg['scratchfile_pattern'].format(**cfg)
    else:
        outfile = cfg['outfile_pattern'].format(**cfg)
    outfile = outfile.replace(' ', '0')
    tray.AddModule("I3Writer", "EventWriter",
                   filename=outfile,
                   Streams=[icetray.I3Frame.DAQ,
                            icetray.I3Frame.Physics,
                            icetray.I3Frame.TrayInfo,
                            icetray.I3Frame.Simulation],
                   DropOrphanStreams = [icetray.I3Frame.DAQ])
    tray.AddModule("TrashCan", "the can")
    tray.Execute()
    tray.Finish()
    del tray


if __name__ == '__main__':
    main()
