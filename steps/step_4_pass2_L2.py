#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
#METAPROJECT icerec/V05-01-06
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
@click.argument('cfg', type=click.Path(exists=True))
@click.argument('run_number', type=int)
@click.option('--scratch/--no-scratch', default=True)
def main(cfg, run_number, scratch):
    with open(cfg, 'r') as stream:
        cfg = yaml.safe_load(stream)
    cfg['run_number'] = run_number
    cfg['run_folder'] = get_run_folder(run_number)

    infile = cfg['infile_pattern'].format(**cfg)
    infile = infile.replace(' ', '0')
    infile = infile.replace('Level0.{}'.format(cfg['previous_step']),
                            'Level0.{}'.format(cfg['previous_step'] % 10))
    infile = infile.replace('2012_pass2', 'pass2')

    if scratch:
        outfile = cfg['scratchfile_pattern'].format(**cfg)
    else:
        outfile = cfg['outfile_pattern'].format(**cfg)
    outfile = outfile.replace('Level0.{}'.format(cfg['step']),
                            'Level2')
    outfile = outfile.replace(' ', '0')
    outfile = outfile.replace('2012_pass2', 'pass2')
    print('Outfile != $FINAL_OUT clean up for crashed scripts not possible!')

    tray = I3Tray()
    """The main L1 script"""
    tray.AddModule('I3Reader',
                   'i3 reader',
                   FilenameList=[cfg['gcd_pass2'], infile])

    tray.AddSegment(OfflineFilter,
                    "OfflineFilter",
                    dstfile=None,
                    mc=True,
                    doNotQify=True,
                    photonicsdir=PHOTONICS_DIR)

    tray.AddModule("I3Writer", "EventWriter",
                   filename=outfile,
                   Streams=[icetray.I3Frame.DAQ,
                            icetray.I3Frame.Physics,
                            icetray.I3Frame.TrayInfo,
                            icetray.I3Frame.Simulation],
                   DropOrphanStreams=[icetray.I3Frame.DAQ])
    tray.AddModule("TrashCan", "the can")
    tray.Execute()
    tray.Finish()
    del tray


if __name__ == '__main__':
    main()
