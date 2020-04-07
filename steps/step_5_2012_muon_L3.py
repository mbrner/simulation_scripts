#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v1/icetray-start
#METAPROJECT /home/mboerner/software/i3/IC2012-L3_Muon_V3/build
import os

import click
import yaml

from utils import get_run_folder

from I3Tray import I3Tray
from icecube import icetray, dataio, dataclasses, hdfwriter, phys_services

from icecube.level3_filter_muon.MuonL3TraySegment import MuonL3

SPLINE_TABLES = '/cvmfs/icecube.opensciencegrid.org/data/photon-tables/splines'
PHOTON_TABLES = '/cvmfs/icecube.opensciencegrid.org/data/photon-tables'
DRIVER_FILE = 'mu_photorec.list'


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
    icetray.logging.set_level("WARN")
    cfg['run_number'] = run_number
    cfg['run_folder'] = get_run_folder(run_number)
    infile = cfg['infile_pattern'].format(**cfg)
    infile = infile.replace(' ', '0')
    infile = infile.replace('Level0.{}'.format(cfg['previous_step']),
                            'Level2')
    infile = infile.replace('2012_pass2', '2012')

    if scratch:
        outfile = cfg['scratchfile_pattern'].format(**cfg)
    else:
        outfile = cfg['outfile_pattern'].format(**cfg)
    outfile = outfile.replace('Level0.{}'.format(cfg['step']),
                            'Level3')
    outfile = outfile.replace(' ', '0')
    outfile = outfile.replace('2012_pass2', '2012')
    print('Outfile != $FINAL_OUT clean up for crashed scripts not possible!')

    outfile = outfile.replace(' ', '0')
    outfile = outfile.replace('2012_pass2', '2012')

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
    tray.Execute()
    tray.Finish()

if __name__ == '__main__':
    main()
