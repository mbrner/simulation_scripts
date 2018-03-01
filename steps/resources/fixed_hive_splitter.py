from icecube import dataio, dataclasses, icetray
from icecube.icetray import I3PacketModule, I3Units, I3Frame
from icecube import lilliput, HiveSplitter, linefit
from icecube import CoincSuite
from icecube.CoincSuite import which_split
from icecube.STTools.seededRT.configuration_services import \
        I3DOMLinkSeededRTConfigurationService


class TimeWindowCollector(I3PacketModule):
    def __init__(self, ctx):
        I3PacketModule.__init__(self, ctx, icetray.I3Frame.DAQ)
        self.AddOutBox("OutBox")
        self.AddParameter("TimeWindowName",
                          "Name of the time windows to collect")
        self.AddParameter("TimeWindowSeriesName",
                          "Name of the timewindow  series to write")

    def Configure(self):
        self.TimeWindowName = self.GetParameter("TimeWindowName")
        self.TimeWindowSeriesName = self.GetParameter("TimeWindowSeriesName")

    def FramePacket(self, frames):
        windows = dataclasses.I3TimeWindowSeries()
        for frame in frames:
            if frame.Stop == frame.DAQ:
                continue
            windows.append(frame[self.TimeWindowName])
        frames[0][self.TimeWindowSeriesName] = windows
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
        final_keys = None
        split_keys = None
        for frame in frames:
            if frame['I3EventHeader'].sub_event_stream == 'Final':
                final_frame = frame
                final_keys = frame.keys()
            if frame['I3EventHeader'].sub_event_stream == 'InIceSplit':
                split_keys = frame.keys()
                split_frame = frame
        if final_keys is not None and split_keys is not None:
            for key in final_keys:
                if key not in split_keys:
                    split_frame[key] = final_frame[key]
            self.PushFrame(daq_frame)
            self.PushFrame(split_frame)


@icetray.traysegment
def SplitAndRecoHiveSplitter(tray, Name, Suffix):
    def re_reconstruct(tray, Pulses, Suffix, If):
        tray.AddSegment(
            lilliput.segments.I3SinglePandelFitter, "SPEFitSingle_"+Suffix,
            domllh="SPE1st",
            pulses=Pulses,
            seeds=["LineFit_"+Suffix],
            If=If
            )

        tray.AddSegment(
            lilliput.segments.I3IterativePandelFitter, "SPEFit2_"+Suffix,
            pulses=Pulses,
            seeds=["SPEFitSingle_"+Suffix],
            n_iterations=2,
            If=If
            )

        tray.AddSegment(
            lilliput.segments.I3SinglePandelFitter, "MPEFit_"+Suffix,
            domllh="MPE",
            pulses=Pulses,
            seeds=["SPEFit2_"+Suffix],
            If=If
            )

    tray.AddModule(TimeWindowCollector, "collector",
                   TimeWindowName="SplitInIcePulsesTimeRange",
                   TimeWindowSeriesName="TriggerSplitterTimeWindows")

    tray.AddModule(
        "I3HiveSplitter", "HiveSplitter",
        InputName="InIcePulses",
        OutputName=Suffix+"InIcePulses",
        Multiplicity=5,
        TimeWindow=2000.*I3Units.ns,
        TimeConeMinus=1000.*I3Units.ns,
        TimeConePlus=1000.*I3Units.ns,
        SingleDenseRingLimits=[300., 300., 272.7, 272.7, 165.8, 165.8],
        DoubleDenseRingLimits=[150., 150., 131.5, 131.5, 40.8, 40.8],
        Mode=1,
        SaveSplitCount=True)  # is needed by CoincSuite

    ReducedCountMaker = lambda frame: frame.Put("HiveSplitterReducedCount",
                                                icetray.I3Int(0))
    tray.AddModule(ReducedCountMaker,
                   Streams=[icetray.I3Frame.DAQ])

    tray.AddModule("AfterpulseDiscard", "AfterpulseDiscard",
                   SplitName="HiveSplitter",
                   RecoMapName=Suffix+"InIcePulses",
                   QTotFraction=0.1,
                   TimeOffset=3.E3*I3Units.ns,
                   OverlapFraction=0.75)

    # change stream of identified afterpulses
    # so that they are not used with hypoframes
    def change_stream(frame, initialStream, finalStream):
        if frame["I3EventHeader"].sub_event_stream == initialStream:
            eh = dataclasses.I3EventHeader(frame["I3EventHeader"])
            del frame["I3EventHeader"]
            eh.sub_event_stream = finalStream
            frame.Put("I3EventHeader", eh)
    tray.AddModule(change_stream,
                   initialStream="HiveSplitter",
                   finalStream="AfterpulsesStream",
                   If=lambda frame: frame.Has("AfterpulseDiscard"))

    tray.AddModule("HypoFrameCreator",
                   SplitName="HiveSplitter",
                   HypoName="hypoframe",
                   RecoMapName=Suffix+"InIcePulses",
                   MaxTimeSeparation=3000.*I3Units.ns)

    tray.AddSegment(linefit.simple, "LineFit_"+Suffix,
                    inputResponse=Suffix+"InIcePulses",
                    fitName="LineFit_"+Suffix,
                    If=(which_split(split_name="HiveSplitter") |
                        which_split(split_name="hypoframe")))

    tray.AddModule("TrackSystemTester", "TestHypoTrackSystem",
                   SplitName="HiveSplitter",
                   RecoMapName=Suffix+"InIcePulses",
                   RecoFitName="LineFit_"+Suffix,
                   HypoName="hypoframe",
                   HypoFitName="LineFit_"+Suffix,
                   CriticalRatio=0.7,
                   CylinderRadius=150.*I3Units.meter,
                   ResTimeWindow=dataclasses.make_pair(-float("Inf"),
                                                       float("Inf")),
                   ParticleSpeed=dataclasses.I3Constants.c,
                   MutualCompare=False)

    tray.AddModule("TrackSystemTester", "TestMutualTrackSystem",
                   SplitName="HiveSplitter",
                   RecoMapName=Suffix+"InIcePulses",
                   RecoFitName="LineFit_"+Suffix,
                   HypoName="hypoframe",
                   HypoFitName="LineFit_"+Suffix,
                   CriticalRatio=0.7,
                   CylinderRadius=150.*I3Units.meter,
                   ResTimeWindow=dataclasses.make_pair(-float("Inf"),
                                                       float("Inf")),
                   ParticleSpeed=dataclasses.I3Constants.c,
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
                   TravelTimeResidual=dataclasses.make_pair(-1000.*I3Units.ns,
                                                            1000.*I3Units.ns),
                   WallTime=3000.*I3Units.ns,
                   MaxVerticalDist=700.*I3Units.meter,
                   MaxHorizontalDist=700.*I3Units.meter,
                   MaxTrackDist=200.*I3Units.m,
                   MaxFurthestDist=600.*I3Units.m)

    LikeNameList = ["TestHypoTrackSystem", "TestHypoAlignment",
                    "TestMutualTrackSystem", "TestMutualAlignment"]
    VetoNameList = ["TestcogCausalConnect", "TestSpeed"]

    tray.AddModule("DecisionMaker", "DecisionDiscard",
                   SplitName="HiveSplitter",
                   RecoMapName=Suffix+"InIcePulses",
                   LikeNameList=LikeNameList,
                   VetoNameList=VetoNameList)

    def removeHypoFrames(frame):
        return not(frame["I3EventHeader"].sub_event_stream == "hypoframe")
    tray.AddModule(removeHypoFrames, "RemoveHypoFrames")

    def removeRecombined(frame):
        return not(frame["I3EventHeader"].sub_event_stream == "HiveSplitter" and
                   frame.Has("DecisionDiscard"))
    tray.AddModule(removeRecombined, "RemoveRecombinedFrame")

    tray.AddModule("SplitTimeWindowCalculator",
                   SubEventStream="HiveSplitter",
                   AfterpulseEventStream="AfterpulseStream",
                   BasePulses="InIcePulses",
                   SplitPulses=Suffix+"InIcePulses",
                   OutputPulses="Millipede"+Suffix+"SplitPulses",
                   TriggerSplitterTimeWindows="TriggerSplitterTimeWindows")

    def discardAfterpulses(frame):
        return frame["I3EventHeader"].sub_event_stream != "AfterpulseStream"
    tray.AddModule(discardAfterpulses)

    tray.AddModule("Delete",
                   Keys=[Suffix+"InIcePulsesTimeRange"])

    def FinalStream(frame):
        if frame.Has("I3EventHeader"):
            if frame["I3EventHeader"].sub_event_stream == "HiveSplitter":
                eh = dataclasses.I3EventHeader(frame["I3EventHeader"])
                eh.sub_event_stream = "Final"
                frame.Delete("I3EventHeader")
                frame.Put("I3EventHeader", eh)
    tray.AddModule(FinalStream, "Finalstream")

    tray.AddModule(P_Combiner, 'combine_final_and_inicesplit_stream')

    # def cleanStreams(frame):
    #     return frame["I3EventHeader"].sub_event_stream == "Final"
    # tray.AddModule(cleanStreams)

    def removeSmallHLCs(frame, Pulses, MinimumHLCs):
        if frame.Has(Pulses):
            pulsemap = dataclasses.I3RecoPulseSeriesMap.from_frame(frame,
                                                                   Pulses)
            hlcs = len([p.time for key, ps in pulsemap.iteritems()
                        for p in ps if p.flags != 4])
            if hlcs < MinimumHLCs:
                return False
            else:
                return True
        else:
            return False
    tray.AddModule(removeSmallHLCs, Pulses=Suffix+"InIcePulses", MinimumHLCs=1)

    stConfigService = I3DOMLinkSeededRTConfigurationService(
        allowSelfCoincidence=True,  # Old SeededRTBehaviour
        useDustlayerCorrection=False,  # Old SeededRTBehaviour
        treat_string_36_as_deepcore=False,  # Old SeededRT behaviour
        dustlayerUpperZBoundary=0*I3Units.m,  # Default
        dustlayerLowerZBoundary=-150*I3Units.m,  # Default
        ic_ic_RTTime=1000*I3Units.ns,
        ic_ic_RTRadius=150*I3Units.m
    )

    # Do the classic seeded RT cleaning.
    tray.AddModule(
        "I3SeededRTCleaning_RecoPulseMask_Module",
        "seededRTcleaning",
        STConfigService=stConfigService,
        InputHitSeriesMapName=Suffix+"InIcePulses",
        OutputHitSeriesMapName="SRT"+Suffix+"InIcePulses",
        SeedProcedure="HLCCoreHits",
        MaxNIterations=3,
        Streams=[icetray.I3Frame.Physics]
    )

    def removeSmallNHitDOMs(frame, Pulses, MinimumHitDOMs):
        if frame.Has(Pulses):
            pulsemap = dataclasses.I3RecoPulseSeriesMap.from_frame(frame,
                                                                   Pulses)
            if len(pulsemap) < MinimumHitDOMs:
                return False
            else:
                return True
        else:
            return False
    tray.AddModule(removeSmallNHitDOMs,
                   Pulses="SRT"+Suffix+"InIcePulses",
                   MinimumHitDOMs=6)

    tray.AddModule("StaticDOMTimeWindowCleaning",
                   InputPulses="SRT"+Suffix+"InIcePulses",
                   OutputPulses="TWSRT"+Suffix+"InIcePulses",
                   MaximumTimeDifference=3e3*I3Units.ns)

    recos = ["LineFit_"+Suffix, "SPEFitSingle_"+Suffix,
             "SPEFit2_"+Suffix, "MPEFit_"+Suffix]
    fitparams = ["LineFit_"+Suffix+"Params",
                 "SPEFitSingle_"+Suffix+"FitParams",
                 "SPEFit2_"+Suffix+"FitParams",
                 "MPEFit_"+Suffix+"FitParams"]
    tray.AddModule("Delete", "remove_recos",
                   Keys=recos+fitparams)

    tray.AddSegment(
        linefit.simple, "LineFit_"+Suffix+"SRT",
        inputResponse="SRT"+Suffix+"InIcePulses",
        fitName="LineFit_"+Suffix,
        If=lambda frame: frame["I3EventHeader"].sub_event_stream == "Final")

    tray.AddSegment(
        linefit.simple, "LineFit_"+Suffix+"TWSRT",
        inputResponse="TWSRT"+Suffix+"InIcePulses",
        fitName="LineFit_"+"TW"+Suffix,
        If=lambda frame: frame["I3EventHeader"].sub_event_stream == "Final")

    re_reconstruct(
        tray,
        Pulses="SRT"+Suffix+"InIcePulses",
        Suffix=Suffix,
        If=lambda frame: frame["I3EventHeader"].sub_event_stream == "Final")
    re_reconstruct(
        tray,
        Pulses="TWSRT"+Suffix+"InIcePulses",
        Suffix="TW"+Suffix,
        If=lambda frame: frame["I3EventHeader"].sub_event_stream == "Final")
