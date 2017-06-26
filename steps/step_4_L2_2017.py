#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
#METAPROJECT icerec/V05-01-06
import click
import yaml

from I3Tray import I3Tray
from icecube import icetray, dataclasses, dataio
from icecube.icetray import I3PacketModule, I3Units

from icecube.filterscripts.offlineL2.level2_all_filters import OfflineFilter

PHOTONICS_DIR = '/cvmfs/icecube.opensciencegrid.org/data/photon-tables'

@click.command()
@click.argument('cfg', click.Path(exists=True))
@click.argument('run_number', type=int)
@click.option('--scratch/--no-scratch', default=True)
def main(cfg, run_number, scratch):
    with open(cfg, 'r') as stream:
        cfg = yaml.load(stream)
    if 'dictitems' in cfg.keys():
        cfg = cfg['dictitems']
    cfg['run_number'] = run_number

    infile = cfg['infile_pattern'].format(run_number=run_number)
    infile = infile.replace(' ', '0')

    tray = I3Tray()
    tray.AddModule('I3Reader',
                   'i3 reader',
                   FilenameList=[cfg['gcd'], infile])


    tray.AddSegment(OfflineFilter, "OfflineFilter",
        dstfile=cfg['dstfile'],
        mc=True,#cfg['L2_mc'],
        doNotQify=True,#cfg['L2_mc'],
        photonicsdir=PHOTONICS_DIR
        )

    if scratch:
        outfile = cfg['scratchfile_pattern'].format(run_number=run_number)
    else:
        outfile = cfg['outfile_pattern'].format(run_number=run_number)
    outfile = outfile.replace(' ', '0')

    tray.AddModule("I3Writer", "EventWriter",
                   filename=outfile,
                   Streams=[icetray.I3Frame.DAQ,
                            icetray.I3Frame.Physics,
                            icetray.I3Frame.TrayInfo],
                   DropOrphanStreams=[icetray.I3Frame.DAQ])
    tray.AddModule("TrashCan", "the can")
    tray.Execute()
    tray.Finish()


if __name__ == '__main__':
    main()

