#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v1/icetray-start
#METAPROJECT /home/mmeier/combo_stable/build
import os

import click
import yaml

from utils import get_run_folder

from I3Tray import I3Tray
from icecube import icetray, dataio, dataclasses, hdfwriter, phys_services
from icecube import lilliput, gulliver, gulliver_modules
from icecube import linefit
from icecube.icetray import I3Units

from icecube.photonics_service import I3PhotoSplineService
from icecube.millipede import HighEnergyExclusions
from modules.taupede import TaupedeWrapper
from icecube.level3_filter_muon.level3_Reconstruct import DoSplineReco
from resources.fixed_hive_splitter import SplitAndRecoHiveSplitter
from icecube import mue
from icecube.level3_filter_cascade.L3_monopod import L3_Monopod
from icecube import STTools
from icecube.level3_filter_cascade.level3_Recos import CascadeLlhVertexFit
import icecube.lilliput.segments
from icecube.weighting import get_weighted_primary


SPLINE_TABLES = '/cvmfs/icecube.opensciencegrid.org/data/photon-tables/splines'
PHOTON_TABLES = '/cvmfs/icecube.opensciencegrid.org/data/photon-tables'
DRIVER_FILE = 'mu_photorec.list'


@icetray.traysegment
def taupede_segment(tray, name, cfg,
                    pulses='SplitInIcePulses',
                    seed_key='L3_MonopodFit4_AmptFit'):
    cascade_service = I3PhotoSplineService(
        amplitudetable=os.path.join(SPLINE_TABLES, 'ems_mie_z20_a10.abs.fits'),
        timingtable=os.path.join(SPLINE_TABLES, 'ems_mie_z20_a10.prob.fits'),
        timingSigma=0)

    # add DOM exclusions
    tray.Add('Delete', keys=['BrightDOMs', 'DeepCoreDOMs', 'SaturatedDOMs'])
    excludedDOMs = tray.Add(HighEnergyExclusions, 'HEExclTaupede',
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

    gcdfile = dataio.I3File(cfg['gcd_pass2'])
    frame = gcdfile.pop_frame()
    while 'I3Geometry' not in frame:
        frame = gcdfile.pop_frame()
    omgeo = frame['I3Geometry'].omgeo

    tray.AddSegment(TaupedeWrapper, 'TaupedeFit',
                    omgeo=omgeo,
                    Seed=seed_key,
                    Iterations=4,
                    PhotonsPerBin=5,
                    **millipede_params)


@icetray.traysegment
def mu_millipede_segment(tray, name, cfg, pulses='InIcePulses'):
    tray.Add('Delete', keys=['BrightDOMs', 'DeepCoreDOMs', 'SaturatedDOMs'])
    # Run HiveSplitter and TimeWindow cleaning for TWSRTHVInIcePulses
    suffix = 'HV'
    tray.AddSegment(SplitAndRecoHiveSplitter, 'HiveSplitterSegment',
                    Suffix=suffix)

    tray.AddService('I3GulliverMinuitFactory', 'Minuit',
                    Algorithm='SIMPLEX',
                    MaxIterations=1000,
                    Tolerance=0.01)
    tray.AddService("I3SimpleParametrizationFactory", "SimpleTrack",
                    StepX=20*I3Units.m,
                    StepY=20*I3Units.m,
                    StepZ=20*I3Units.m,
                    StepZenith=0.1*I3Units.radian,
                    StepAzimuth=0.2*I3Units.radian,
                    BoundsX=[-2000*I3Units.m, 2000*I3Units.m],
                    BoundsY=[-2000*I3Units.m, 2000*I3Units.m],
                    BoundsZ=[-2000*I3Units.m, 2000*I3Units.m])

    tray.AddService("I3PowExpZenithWeightServiceFactory", "ZenithWeight",
                    Amplitude=2.49655e-07,                # Default
                    CosZenithRange=[-1, 1],               # Default
                    DefaultWeight=1.383896526736738e-87,  # Default
                    ExponentFactor=0.778393,              # Default
                    FlipTrack=False,                      # Default
                    PenaltySlope=-1000,                   # Penalty
                    PenaltyValue=-200,                    # Default
                    Power=1.67721)                        # Default

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
                                   Pulses="Millipede"+suffix+"SplitPulses",
                                   ExcludeDeepCore="DeepCoreDOMs",
                                   ExcludeSaturatedDOMs=False,
                                   ExcludeBrightDOMS="BrightDOMs",
                                   BrightDOMThreshold=10,
                                   SaturationWindows="SaturationWindows",
                                   BadDomsList="BadDomsList",
                                   CalibrationErrata="CalibrationErrata")
    exclusionsHE.append("Millipede"+suffix+"SplitPulsesExcludedTimeRange")

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

    def add_timerange(frame, pulses):
        time_range = frame['CalibratedWaveformRange']
        frame[pulses + 'TimeRange'] = dataclasses.I3TimeWindow(
            time_range.start - 25.*I3Units.ns, time_range.stop)
        return True

    tray.AddModule(add_timerange, 'add timerange for monopod',
                   pulses='SplitInIcePulses')

    def maskify(frame):
        if frame.Has('SplitInIcePulses'):
            # In IC86-2013 'SplitInIcePulses'
            # is used as 'OfflinePulses' in IC86-2011
            frame['OfflinePulses'] = frame['SplitInIcePulses']
            frame['OfflinePulsesTimeRange'] = frame[
                'SplitInIcePulsesTimeRange']
        else:
            return True
        if frame.Has('SRTInIcePulses'):
            frame['SRTOfflinePulses'] = frame['SRTInIcePulses']
        else:
            return True
        return True

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
                                   'CascadeLlhVertexFit_L2'],
                   If=lambda frame: not frame.Has('CascadeLlhVertexFit_L2'))

    tray.AddSegment(L3_Monopod, 'monopod',
                    Pulses='OfflinePulses',
                    year="2012",
                    AmplitudeTable=amplitude_table,
                    TimingTable=timing_table)


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
    # icetray.logging.set_level("DEBUG")
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

    tray = I3Tray()

    tray.AddModule('I3Reader', 'reader',
                   filenamelist=[cfg['gcd_pass2'], infile])

    def split_selector(frame):
        if frame.Stop == icetray.I3Frame.Physics:
            if frame['I3EventHeader'].sub_event_stream == 'InIceSplit':
                return True
        return False

    tray.AddModule(split_selector, 'select_inicesplit')

    def check_srt_pulses(frame):
        if not frame.Has('SRTInIcePulses'):
            return False
        else:
            return True

    tray.AddModule(check_srt_pulses, 'check_srt')

    tray.AddModule(get_weighted_primary, 'get_the_primary',
                   If=lambda frame: not frame.Has('MCPrimary'))

    tray.AddSegment(monopod_segment, 'MonopodSegment', cfg=cfg)

    tray.AddSegment(taupede_segment, 'TaupedeSegment', cfg=cfg)

    tray.AddSegment(mu_millipede_segment, 'MuMillipedeSegment', cfg=cfg)

    tray.AddModule('I3Writer', 'writer',
                   Streams=[icetray.I3Frame.DAQ, icetray.I3Frame.Physics],
                   Filename=outfile)

    tray.AddModule("TrashCan", "Bye")
    tray.Execute()
    tray.Finish()

if __name__ == '__main__':
    main()
