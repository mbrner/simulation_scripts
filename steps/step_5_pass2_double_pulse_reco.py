#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v1/icetray-start
#METAPROJECT /data/user/mmeier/tests/musner/icerec.trunk.rev142791.extended.2016-03-04.RHEL_6_x86_64
import os

import click
import yaml

from utils import get_run_folder

from I3Tray import I3Tray
from icecube import icetray, dataio, dataclasses, hdfwriter, phys_services
from icecube import lilliput, gulliver, gulliver_modules
from icecube import improvedLinefit, rootwriter

from icecube.photonics_service import I3PhotoSplineService
from icecube.millipede import HighEnergyExclusions
from modules.taupede import TaupedeWrapper
from icecube.level3_filter_muon.level3_Reconstruct import DoSplineReco
from icecube.level3_filter_muon.level3_SplitHiveSplitter import SplitAndRecoHiveSplitter
from icecube import mue
from icecube.level3_filter_cascade.CascadeL3TraySegment import maskify
from icecube.level3_filter_cascade.L3_monopod import L3_Monopod
from icecube.level3_filter_cascade.level3_Recos import CascadeLlhVertexFit

SPLINE_TABLES = '/cvmfs/icecube.opensciencegrid.org/data/photon-tables/splines'
PHOTON_TABLES = '/cvmfs/icecube.opensciencegrid.org/data/photon-tables'
DRIVER_FILE = 'mu_photorec.list'


@icetray.traysegment
def taupede_segment(tray, name, cfg, pulses='SplitInIcePulses'):
    cascade_service = I3PhotoSplineService(
        amplitudetable=os.path.join(SPLINE_TABLES, 'ems_mie_z20_a10.abs.fits'),
        timingtable=os.path.join(SPLINE_TABLES, 'ems_mie_z20_a10.prob.fits'),
        timingSigma=0)

    # add DOM exclusions
    tray.Add('Delete', keys=['BrightDOMs', 'DeepCoreDOMs', 'SaturatedDOMs'])
    excludedDOMs = tray.Add(HighEnergyExclusions,
                            Pulses='SplitInIcePulses',
                            BadDomsList='BadDomsList',
                            CalibrationErrata='CalibrationErrata',
                            ExcludeBrightDOMs='BrightDOMs',
                            ExcludeDeepCore='DeepCoreDOMs',
                            ExcludeSaturatedDOMs='SaturatedDOMs',
                            SaturationWindows='SaturationTimes')

    millipede_params = {
        'Pulses': 'SplitInIcePulses',
        'CascadePhotonicsService': cascade_service,
        'ExcludedDOMs': excludedDOMs,
        'DOMEfficiency': 0.99,
        'ReadoutWindow': pulses + 'TimeRange',
        'PartialExclusion': True,
        'UseUnhitDOMs': True}

    gcdfile = dataio.I3File(cfg['gcd'])
    frame = gcdfile.pop_frame()
    while 'I3Geometry' not in frame:
        frame = gcdfile.pop_frame()
    omgeo = frame['I3Geometry'].omgeo

    tray.AddSegment(TaupedeWrapper, 'TaupedeFit',
                    omgeo=omgeo,
                    Seed='L3MonopodFit4',
                    Iterations=4,
                    PhotonsPerBin=5,
                    **millipede_params)


@icetray.traysegment
def mu_millipede_segment(tray, name, cfg, pulses='InIcePulses'):
    # Run HiveSplitter and TimeWindow cleaning for TWSRTHVInIcePulses
    suffix = 'HV'
    tray.AddSegment(SplitAndRecoHiveSplitter, 'HiveSplitterSegment',
                    Suffix=suffix)
    # Run MuEX as a seed for spline MPE
    # muex - iterative angular
    tray.AddModule("muex", "muex_angular4",
                   Pulses="TWSRT" + suffix + pulses,
                   rectrk="",
                   result="MuEXAngular4",
                   lcspan=0,
                   repeat=4,
                   usempe=True,
                   detail=False,
                   energy=False,
                   icedir=os.path.expandvars(
                       "$I3_BUILD/mue/resources/ice/mie"))

    # spline MPE as a seed for MuMillipede
    spline_mie = I3PhotoSplineService(
        os.path.join(SPLINE_TABLES, 'InfBareMu_mie_abs_z20a10_V2.fits'),
        os.path.join(SPLINE_TABLES, 'InfBareMu_mie_prob_z20a10_V2.fits'), 4)
    llh = "MPE"
    tray.AddSegment(DoSplineReco, "spline%s" % llh,
                    Pulses="TWSRT" + suffix + pulses,
                    Seed="MuEXAngular4",
                    LLH=llh,
                    Suffix="",
                    spline=spline_mie)

    cascade_service_mie = I3PhotoSplineService(
        amplitudetable=os.path.join(SPLINE_TABLES, 'ems_mie_z20_a10.abs.fits'),
        timingtable=os.path.join(SPLINE_TABLES, 'ems_mie_z20_a10.prob.fits'),
        timingSigma=0)

    exclusionsHE = tray.AddSegment(HighEnergyExclusions,
                                   "excludes_high_energies",
                                   Pulses="Millipede"+Suffix+"SplitPulses",
                                   ExcludeDeepCore="DeepCoreDOMs",
                                   ExcludeSaturatedDOMs=False,
                                   ExcludeBrightDOMS="BrightDOMs",
                                   BrightDOMThreshold=10,
                                   SaturationWindows="SaturationWindows",
                                   BadDomsList="BadDomsList",
                                   CalibrationErrata="CalibrationErrata")
    exclusionsHE.append("Millipede"+Suffix+"SplitPulsesExcludedTimeRange")

    tray.AddModule("MuMillipede", "millipede_highenergy_mie",
                   MuonPhotonicsService=None,
                   CascadePhotonicsService=cascade_service_mie,
                   PhotonsPerBin=15,
                   MuonSpacing=0,
                   ShowerSpacing=10,
                   ShowerRegularization=1e-9,
                   MuonRegularization=0,
                   SeedTrack="SplineMPE",
                   Output="SplineMPE_MillipedeHighEnergyMIE",
                   ReadoutWindow="Millipede"+suffix+"SplitPulsesReadoutWindow",
                   ExcludedDOMs=exclusionsHE,
                   DOMEfficiency=0.99,
                   Pulses="Millipede"+suffix+"SplitPulses")


@icetray.traysegment
def monopod_segment(tray, name, cfg, pulses='InIcePulses',
                    amplitude_table=os.path.join(SPLINE_TABLES,
                                                 'ems_mie_z20_a10.abs.fits'),
                    timing_table=os.path.join(SPLINE_TABLES,
                                              'ems_mie_z20_a10.prob.fits')):
    # Rename Pulses for Cascade L3 Scripts
    tray.AddModule(maskify, 'maskify')

    # general cascade llh w/o DC for singles branches but run all events
    tray.AddSegment(CascadeLlhVertexFit, 'CascadeLlhVertexFit_IC',
                    Pulses='OfflinePulsesHLC_noDC')

    # Calc CascadeLlhVertexFit as a seed for Monopod
    tray.AddModule("I3CscdLlhModule", "CscdL3_CascadeLlhVertexFit",
                   InputType="RecoPulse",
                   RecoSeries=pulses,
                   FirstLE=True,
                   SeedWithOrigin=False,
                   SeedKey="CascadeLlhVertexFit_IC",
                   MinHits=8,
                   AmpWeightPower=0.0,
                   ResultName="CscdL3_CascadeLlhVertexFit",
                   Minimizer="Powell",
                   PDF="UPandel",
                   ParamT="1.0, 0.0, 0.0, false",
                   ParamX="1.0, 0.0, 0.0, false",
                   ParamY="1.0, 0.0, 0.0, false",
                   ParamZ="1.0, 0.0, 0.0, false")

    # Rename the L3 Fit to the expected key in the L3_Monopod Segment
    # for the year 2012, changing the year just changes the Seed Key
    tray.AddModule("Rename", keys=['CscdL3_CascadeLlhVertexFit',
                                   'CascadeLlhVertexFit_L2'])

    tray.AddSegment(L3_Monopod, 'monopod',
                    Pulses='OfflinePulses',
                    year="2012",
                    AmplitudeTable=amplitude_table,
                    TimingTable=timing_table)


@click.command()
@click.argument('cfg', click.Path(exists=True))
@click.argument('run_number', type=int)
@click.option('--scratch/--no-scratch', default=True)
def main(cfg, run_number, scratch):
    with open(cfg, 'r') as stream:
        cfg = yaml.load(stream)
    icetray.logging.set_level("WARN")
    cfg['run_number'] = run_number
    cfg['run_folder'] = get_run_folder(run_number)
    infile = cfg['infile_pattern'].format(**cfg)
    infile = infile.replace(' ', '0')
    infile = infile.replace('Level0.{}'.format(cfg['previous_step']),
                            'Level2')
    infile = infile.replace('2012_pass2', 'pass2')

    if scratch:
        outfile = cfg['scratchfile_pattern'].format(**cfg)
    else:
        outfile = cfg['outfile_pattern'].format(**cfg)
    outfile = outfile.replace('Level0.{}'.format(cfg['step']),
                              'Level2.5')
    outfile = outfile.replace(' ', '0')
    outfile = outfile.replace('2012_pass2', 'pass2')
    print('Outfile != $FINAL_OUT clean up for crashed scripts not possible!')

    outfile = outfile.replace(' ', '0')
    outfile = outfile.replace('2012_pass2', 'pass2')

    tray = I3Tray()

    tray.AddModule('I3Reader', 'reader', filenamelist=[cfg['gcd'], infile])

    tray.AddSegment(taupede_segment, 'TaupedeSegment', cfg=cfg)

    tray.AddSegment(mu_millipede_segment, 'TaupedeSegment', cfg=cfg)

    tray.AddSegment(monopod_segment, 'TaupedeSegment', cfg=cfg)

    tray.AddModule('I3Writer', 'writer',
                   Streams=[icetray.I3Frame.DAQ, icetray.I3Frame.Physics],
                   Filename=out_file)

    tray.AddModule("TrashCan", "Bye")
    tray.Execute()
    tray.Finish()

if __name__ == '__main__':
    main()
