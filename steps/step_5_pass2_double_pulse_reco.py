#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v1/icetray-start
#METAPROJECT /home/mmeier/combo_stable/build
import os

import click
import yaml

from utils import get_run_folder

from I3Tray import I3Tray
from icecube import icetray, dataio, dataclasses, hdfwriter, phys_services
from icecube import lilliput, gulliver, gulliver_modules
from icecube import linefit, rootwriter
from icecube.icetray import I3Units

from icecube.photonics_service import I3PhotoSplineService
from icecube.millipede import HighEnergyExclusions
from modules.taupede import TaupedeWrapper
from icecube.level3_filter_muon.level3_Reconstruct import DoSplineReco
#from icecube.level3_filter_muon.level3_SplitHiveSplitter import SplitAndRecoHiveSplitter
from icecube import mue
from icecube.level3_filter_cascade.L3_monopod import L3_Monopod
from icecube import STTools
from icecube.level3_filter_cascade.level3_Recos import CascadeLlhVertexFit

SPLINE_TABLES = '/cvmfs/icecube.opensciencegrid.org/data/photon-tables/splines'
PHOTON_TABLES = '/cvmfs/icecube.opensciencegrid.org/data/photon-tables'
DRIVER_FILE = 'mu_photorec.list'


from I3Tray import *
from icecube import icetray, dataclasses, dataio, gulliver, lilliput, linefit, HiveSplitter
from icecube.CoincSuite import which_split
from icecube import CoincSuite
from icecube.icetray import I3Frame, I3Units, I3PacketModule
import icecube.lilliput.segments
import numpy as np

load("level3-filter-muon")

class TimeWindowCollector(I3PacketModule):

    def __init__(self, ctx):
        I3PacketModule.__init__(self, ctx, icetray.I3Frame.DAQ)
        self.AddOutBox("OutBox")
        self.AddParameter("TimeWindowName", "Name of the time windows to collect")
        self.AddParameter("TimeWindowSeriesName", "Name of the timewindow  series to write")

    def Configure(self):
        self.TimeWindowName=self.GetParameter("TimeWindowName")
        self.TimeWindowSeriesName=self.GetParameter("TimeWindowSeriesName")

    def FramePacket(self, frames):
        windows=dataclasses.I3TimeWindowSeries()
        for frame in frames:
            if frame.Stop==frame.DAQ:
                continue
            windows.append(frame[self.TimeWindowName])
        frames[0][self.TimeWindowSeriesName]=windows
        for frame in frames:
            self.PushFrame(frame)

class P_Combiner(I3PacketModule):
    def __init__(self, ctx):
        I3PacketModule.__init__(self, ctx, icetray.I3Frame.DAQ)
        self.AddOutBox('OutBox')

    def Configure(self):
        I3PacketModule.Configure(self)

    def FramePacket(self, frames):
        daq_frame = frames[0]
        for frame in frames:
            if frame['I3EventHeader'].sub_event_stream == 'Final':
                final_frame = frame
                final_keys = frame.keys()
            if frame['I3EventHeader'].sub_event_stream == 'InIceSplit':
                split_keys = frame.keys()
                split_frame = frame
        for key in final_keys:
            if key not in split_keys:
                split_frame[key] = final_frame[key]
        self.PushFrame(daq_frame)
        self.PushFrame(split_frame)


@icetray.traysegment
def SplitAndRecoHiveSplitter(tray, Name, Suffix):
    def re_reconstruct(tray, Pulses, Suffix, If):
        tray.AddSegment(lilliput.segments.I3SinglePandelFitter, "SPEFitSingle_"+Suffix,
            domllh="SPE1st",
            pulses=Pulses,
            seeds=["LineFit_"+Suffix],
            If=If
            )

        tray.AddSegment(lilliput.segments.I3IterativePandelFitter, "SPEFit2_"+Suffix,
            pulses=Pulses,
            seeds=["SPEFitSingle_"+Suffix],
            n_iterations=2,
            If=If
            )

        tray.AddSegment(lilliput.segments.I3SinglePandelFitter, "MPEFit_"+Suffix,
            domllh="MPE",
            pulses=Pulses,
            seeds=["SPEFit2_"+Suffix],
            If=If
            )

    tray.AddModule(TimeWindowCollector, "collector",
        TimeWindowName="SplitInIcePulsesTimeRange",
        TimeWindowSeriesName="TriggerSplitterTimeWindows")

    tray.AddModule("I3HiveSplitter", "HiveSplitter",
        InputName="InIcePulses",
        OutputName=Suffix+"InIcePulses",
        Multiplicity=5,
        TimeWindow=2000.*I3Units.ns,
        TimeConeMinus=1000.*I3Units.ns,
        TimeConePlus=1000.*I3Units.ns,
        SingleDenseRingLimits=[300., 300., 272.7, 272.7, 165.8, 165.8], #I3Units.meter
        DoubleDenseRingLimits=[150., 150., 131.5, 131.5, 40.8, 40.8], #I3Units.meter
        #TripleDenseRingLimits=[150., 150., 144.1, 144.1, 124.7, 124.7, 82.8, 82.8], #I3Units.meter
        Mode=1,
        SaveSplitCount=True) # is needed by CoincSuite
 
    ReducedCountMaker = lambda frame: frame.Put("HiveSplitterReducedCount", icetray.I3Int(0))
    tray.AddModule(ReducedCountMaker,
        Streams=[icetray.I3Frame.DAQ])

    tray.AddModule("AfterpulseDiscard", "AfterpulseDiscard",
        SplitName="HiveSplitter",
        RecoMapName=Suffix+"InIcePulses",
        QTotFraction=0.1,
        TimeOffset=3.E3*I3Units.ns,
        OverlapFraction=0.75)

    #change stream of identified afterpulses so that they are not used with hypoframes
    def change_stream(frame, initialStream, finalStream):
        if frame["I3EventHeader"].sub_event_stream==initialStream:
            eh=dataclasses.I3EventHeader(frame["I3EventHeader"])
            del frame["I3EventHeader"]
            eh.sub_event_stream=finalStream
            frame.Put("I3EventHeader", eh)
    tray.AddModule(change_stream,
        initialStream="HiveSplitter",
        finalStream="AfterpulsesStream",
        If=lambda frame: frame.Has("AfterpulseDiscard"))

    tray.AddModule("HypoFrameCreator",
        SplitName="HiveSplitter",
        HypoName="hypoframe",
        RecoMapName=Suffix+"InIcePulses",
        MaxTimeSeparation = 3000.*I3Units.ns)

    tray.AddSegment(linefit.simple, "LineFit_"+Suffix,
        inputResponse=Suffix+"InIcePulses",
        fitName="LineFit_"+Suffix,
        If = (which_split(split_name="HiveSplitter") | which_split(split_name="hypoframe")))

    tray.AddModule("TrackSystemTester", "TestHypoTrackSystem",
        SplitName="HiveSplitter",
        RecoMapName=Suffix+"InIcePulses",
        RecoFitName="LineFit_"+Suffix,
        HypoName="hypoframe",
        HypoFitName="LineFit_"+Suffix,
        CriticalRatio=0.7,
        CylinderRadius=150.*I3Units.meter,
        ResTimeWindow = dataclasses.make_pair(-float("Inf"), float("Inf")),
        ParticleSpeed = dataclasses.I3Constants.c,
        MutualCompare=False)

    tray.AddModule("TrackSystemTester", "TestMutualTrackSystem",
        SplitName="HiveSplitter",
        RecoMapName=Suffix+"InIcePulses",
        RecoFitName="LineFit_"+Suffix,
        HypoName="hypoframe",
        HypoFitName="LineFit_"+Suffix,
        CriticalRatio=0.7,
        CylinderRadius=150.*I3Units.meter,
        ResTimeWindow = dataclasses.make_pair(-float("Inf"), float("Inf")),
        ParticleSpeed = dataclasses.I3Constants.c,
        MutualCompare=True)

    tray.AddModule("AlignmentTester", "TestHypoAlignment",
        SplitName="HiveSplitter",
        RecoFitName="LineFit_"+Suffix,
        HypoName="hypoframe",
        HypoFitName="LineFit_"+Suffix,
        CriticalAngle=25.*I3Units.degree,
        CriticalDistance=20.*I3Units.meter,
        MutualCompare=False)

    tray.AddModule("AlignmentTester", "TestMutualAlignment",
        SplitName="HiveSplitter",
        RecoFitName="LineFit_"+Suffix,
        HypoName="hypoframe",
        HypoFitName="LineFit_"+Suffix,
        CriticalAngle=15.*I3Units.degree,
        CriticalDistance=100.*I3Units.meter,
        MutualCompare=True)

    tray.AddModule("SpeedTester", "TestSpeed",
        SplitName="HiveSplitter",
        HypoName="hypoframe",
        HypoFitName="LineFit_"+Suffix,
        SpeedUpperCut=0.35*I3Units.meter/I3Units.ns,
        SpeedLowerCut=0.15*I3Units.meter/I3Units.ns)

    tray.AddModule("cogCausalConnectTester", "TestcogCausalConnect",
        SplitName="HiveSplitter",
        RecoMapName=Suffix+"InIcePulses",
        HypoFitName="LineFit_"+Suffix,
        HypoName="hypoframe",
        TravelTimeResidual = dataclasses.make_pair(-1000.*I3Units.ns, 1000.*I3Units.ns),
        WallTime=3000.*I3Units.ns,
        MaxVerticalDist=700.*I3Units.meter,
        MaxHorizontalDist=700.*I3Units.meter,
        MaxTrackDist=200.*I3Units.m,
        MaxFurthestDist=600.*I3Units.m)

    LikeNameList=["TestHypoTrackSystem", "TestHypoAlignment", "TestMutualTrackSystem", "TestMutualAlignment"]
    VetoNameList=["TestcogCausalConnect", "TestSpeed"]

    tray.AddModule("DecisionMaker", "DecisionDiscard",
        SplitName="HiveSplitter",
        RecoMapName=Suffix+"InIcePulses",
        LikeNameList=LikeNameList,
        VetoNameList=VetoNameList)

    def removeHypoFrames(frame):
        return not(frame["I3EventHeader"].sub_event_stream=="hypoframe")
    tray.AddModule(removeHypoFrames, "RemoveHypoFrames")

    def removeRecombined(frame):
        return not(frame["I3EventHeader"].sub_event_stream=="HiveSplitter" and frame.Has("DecisionDiscard"))
    tray.AddModule(removeRecombined, "RemoveRecombinedFrame")

    tray.AddModule("SplitTimeWindowCalculator",
        SubEventStream="HiveSplitter",
        AfterpulseEventStream="AfterpulseStream",
        BasePulses="InIcePulses",
        SplitPulses=Suffix+"InIcePulses",
        OutputPulses="Millipede"+Suffix+"SplitPulses",
        TriggerSplitterTimeWindows="TriggerSplitterTimeWindows")

    def discardAfterpulses(frame):
        return frame["I3EventHeader"].sub_event_stream!="AfterpulseStream"
    tray.AddModule(discardAfterpulses)

    tray.AddModule("Delete",
        Keys=[Suffix+"InIcePulsesTimeRange"])

    def FinalStream(frame):
        if frame.Has("I3EventHeader"):
            if frame["I3EventHeader"].sub_event_stream=="HiveSplitter":
                eh=dataclasses.I3EventHeader(frame["I3EventHeader"])
                eh.sub_event_stream="Final"
                frame.Delete("I3EventHeader")
                frame.Put("I3EventHeader", eh)
    tray.AddModule(FinalStream, "Finalstream")

    tray.AddModule(P_Combiner, 'combine_final_and_inicesplit_stream')

    def cleanStreams(frame):
        return frame["I3EventHeader"].sub_event_stream=="Final"
    # tray.AddModule(cleanStreams)

    def removeSmallHLCs(frame, Pulses, MinimumHLCs):
        if frame.Has(Pulses):
            pulsemap=dataclasses.I3RecoPulseSeriesMap.from_frame(frame, Pulses)
            hlcs=len([p.time for key, ps in pulsemap.iteritems() for p in ps if p.flags!=4])
            if hlcs<MinimumHLCs:
                return False
            else:
                return True
        else:
            return False
    tray.AddModule(removeSmallHLCs, Pulses=Suffix+"InIcePulses", MinimumHLCs=1)

    from icecube.STTools.seededRT.configuration_services import I3DOMLinkSeededRTConfigurationService
    stConfigService = I3DOMLinkSeededRTConfigurationService(
        allowSelfCoincidence    = True,            # Not Default, but resembles old SeededRTBehaviour
        useDustlayerCorrection  = False,           # Not Default, but resembles old SeededRTBehaviour
        treat_string_36_as_deepcore = False,       # Not Default, but resembles old SeededRT behaviour
        dustlayerUpperZBoundary = 0*I3Units.m,     # Default
        dustlayerLowerZBoundary = -150*I3Units.m,  # Default
        ic_ic_RTTime            = 1000*I3Units.ns, # Default
        ic_ic_RTRadius          = 150*I3Units.m    # Default
    )

    # Do the classic seeded RT cleaning.
    tray.AddModule("I3SeededRTCleaning_RecoPulseMask_Module", "seededRTcleaning",
        STConfigService         = stConfigService,
        InputHitSeriesMapName   = Suffix+"InIcePulses",
        OutputHitSeriesMapName  = "SRT"+Suffix+"InIcePulses",
        SeedProcedure           = "HLCCoreHits",
        MaxNIterations          = 3,
        Streams                 = [icetray.I3Frame.Physics]
    )

    def removeSmallNHitDOMs(frame, Pulses, MinimumHitDOMs):
        if frame.Has(Pulses):
            pulsemap=dataclasses.I3RecoPulseSeriesMap.from_frame(frame, Pulses)
            if len(pulsemap)<MinimumHitDOMs:
                return False
            else:
                return True
        else:
            return False
    tray.AddModule(removeSmallNHitDOMs, Pulses="SRT"+Suffix+"InIcePulses", MinimumHitDOMs=6)

    tray.AddModule("StaticDOMTimeWindowCleaning",
        InputPulses="SRT"+Suffix+"InIcePulses",
        OutputPulses="TWSRT"+Suffix+"InIcePulses",
        MaximumTimeDifference=3e3*I3Units.ns)

    recos=["LineFit_"+Suffix, "SPEFitSingle_"+Suffix, "SPEFit2_"+Suffix, "MPEFit_"+Suffix]
    fitparams=["LineFit_"+Suffix+"Params", "SPEFitSingle_"+Suffix+"FitParams", "SPEFit2_"+Suffix+"FitParams", "MPEFit_"+Suffix+"FitParams"]
    tray.AddModule("Delete", "remove_recos",
        Keys=recos+fitparams)

    tray.AddSegment(linefit.simple, "LineFit_"+Suffix+"SRT",
        inputResponse="SRT"+Suffix+"InIcePulses",
        fitName="LineFit_"+Suffix,
        If = lambda frame: frame["I3EventHeader"].sub_event_stream=="Final"
        )
    tray.AddSegment(linefit.simple, "LineFit_"+Suffix+"TWSRT",
        inputResponse="TWSRT"+Suffix+"InIcePulses",
        fitName="LineFit_"+"TW"+Suffix,
        If = lambda frame: frame["I3EventHeader"].sub_event_stream=="Final"
        )

    re_reconstruct(tray, Pulses="SRT"+Suffix+"InIcePulses", Suffix=Suffix, If=lambda frame: frame["I3EventHeader"].sub_event_stream=="Final")
    re_reconstruct(tray, Pulses="TWSRT"+Suffix+"InIcePulses", Suffix="TW"+Suffix, If=lambda frame: frame["I3EventHeader"].sub_event_stream=="Final")

@icetray.traysegment
def taupede_segment(tray, name, cfg, pulses='SplitInIcePulses', seed_key='L3_MonopodFit4_AmptFit'):
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
	    StepX = 20*I3Units.m,
	    StepY = 20*I3Units.m,
	    StepZ = 20*I3Units.m,
	    StepZenith = 0.1*I3Units.radian,
	    StepAzimuth= 0.2*I3Units.radian,
	    BoundsX = [-2000*I3Units.m, 2000*I3Units.m],
	    BoundsY = [-2000*I3Units.m, 2000*I3Units.m],
	    BoundsZ = [-2000*I3Units.m, 2000*I3Units.m])

    tray.AddService( "I3PowExpZenithWeightServiceFactory", "ZenithWeight",
	    Amplitude=2.49655e-07,               # Default
	    CosZenithRange=[ -1, 1 ],            # Default
	    DefaultWeight=1.383896526736738e-87, # Default
	    ExponentFactor=0.778393,             # Default
	    FlipTrack=False,                     # Default
	    PenaltySlope=-1000,                  # ! Add penalty for being in the wrong region
	    PenaltyValue=-200,                   # Default
	    Power=1.67721)                       # Default


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
        frame[pulses + 'TimeRange'] = dataclasses.I3TimeWindow(time_range.start - 25.*I3Units.ns, time_range.stop)
        return True

    tray.AddModule(add_timerange, 'add timerange for monopod',
                   pulses='SplitInIcePulses')

    def maskify(frame):
        if frame.Has('SplitInIcePulses'): 
            frame['OfflinePulses']=frame['SplitInIcePulses']  # In IC86-2013 'SplitInIcePulses' is used as 'OfflinePulses' in IC86-2011
            frame['OfflinePulsesTimeRange']=frame['SplitInIcePulsesTimeRange']
        else:
            return True
        if frame.Has('SRTInIcePulses'):
            frame['SRTOfflinePulses']=frame['SRTInIcePulses']
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
@click.argument('cfg', click.Path(exists=True))
@click.argument('run_number', type=int)
@click.option('--scratch/--no-scratch', default=True)
def main(cfg, run_number, scratch):
    with open(cfg, 'r') as stream:
        cfg = yaml.load(stream)
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

    tray.AddModule('I3Reader', 'reader', filenamelist=[cfg['gcd_pass2'], infile])

    def split_selector(frame):
        if frame.Stop == icetray.I3Frame.Physics:
            if frame['I3EventHeader'].sub_event_stream == 'InIceSplit':
                return True
        return False

    tray.AddModule(split_selector, 'select_inicesplit')

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
    from time import time
    t0 = time()
    main()
    t1 = time()
    print(t1 - t0)
