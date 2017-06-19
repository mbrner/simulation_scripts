#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
#METAPROJECT simulation/V05-01-01

import click
import yaml

from I3Tray import I3Tray
import os
from icecube import icetray, dataio, dataclasses, hdfwriter, phys_services, lilliput, gulliver, gulliver_modules, improvedLinefit, rootwriter

from icecube.level3_filter_muon.MuonL3TraySegment import MuonL3

SPLINE_TABLES = '/cvmfs/icecube.opensciencegrid.org/data/photon-tables/splines'
PHOTON_TABLES = '/cvmfs/icecube.opensciencegrid.org/data/photon-tables'
DRIVER_FILE = 'mu_photorec.list'


@click.command()
@click.argument('cfg', click.Path(exists=True))
@click.argument('run_number', type=int)
@click.option('--scratch/--no-scratch', default=True)
def main(cfg, run_number, scratch):
    with open(cfg, 'r') as stream:
        cfg = yaml.load(stream)
    icetray.logging.set_level("WARN")

    infile = cfg['infile_pattern'].format(run_number=run_number)
    infile = infile.replace(' ', '0')

    if scratch:
        outfile = cfg['scratchfile_pattern'].format(run_number=run_number)
    else:
        outfile = cfg['outfile_pattern'].format(run_number=run_number)
    outfile = outfile.replace(' ', '0')

    tray = I3Tray()

    photonics_dir = os.path.join(PHOTON_TABLES, 'SPICEMie')
    photonics_driver_dir = os.path.join(photonics_dir, 'driverfiles')

    tray.AddSegment(
        MuonL3,
        gcdfile=cfg['gcd'],
        infiles=infile,
        output_i3=outfile,
        output_hd5=None,
        output_root=None,
        photonicsdir=photonics_dir,
        photonicsdriverdir=photonics_driver_dir,
        photonicsdriverfile=DRIVER_FILE,
        infmuonampsplinepath=os.path.join(SPLINE_TABLES,
                                          'InfBareMu_mie_abs_z20a10.fits'),
        infmuonprobsplinepath=os.path.join(SPLINE_TABLES,
                                           'InfBareMu_mie_prob_z20a10.fits'),
        cascadeampsplinepath=os.path.join(SPLINE_TABLES,
                                          'ems_mie_z20_a10.abs.fits'),
        cascadeprobsplinepath=os.path.join(SPLINE_TABLES,
                                           'ems_mie_z20_a10.prob.fits'))

    tray.AddModule("TrashCan", "Bye")

    tray.Finish()

if __name__ == '__main__':
    main()
