from icecube import icetray, dataclasses, dataio, gulliver, millipede, linefit, hdfwriter, rootwriter, simclasses
from icecube.common_variables import direct_hits, hit_multiplicity, hit_statistics, track_characteristics
from icecube.icetray import I3Units
import numpy as np

@icetray.traysegment
def WriteOutput(tray, name, Suffix, output_i3, output_hd5, output_root):

    # Waveform and Pulse Series Names
    raw_inice_readout = "InIceRawData"
    raw_icetop_readout = "IceTopRawData"
    offline_inice_errata = "OfflineInIceCalibrationErrata"
    masked_offline_pulses = "MaskedOfflinePulses"

    offline_pulses="OfflinePulses"
    offline_pulses_hlc="OfflinePulsesHLC"

    # Stuff for EHE reconstructions
    portia_event_name="EHESummaryPulseInfo"
    atwd_portia_pulse="ATWDPortiaPulse"
    fadc_portia_pulse="FADCPortiaPulse"

    # IceTop Stuff
    icetop_tank_pulse_merger_excluded_stations="TankPulseMergerExcludedStations"
    icetop_cluster_cleaning_excluded_stations="ClusterCleaningExcludedStations"
    icetop_hlc_vem_pulses="OfflineIceTopHLCVEMPulses"
    icetop_slc_vem_pulses="OfflineIceTopSLCVEMPulses"
    icetop_hlc_pulse_info="OfflineIceTopHLCPulseInfo"
    icetop_hlc_pulses="OfflineIceTopHLCTankPulses" #these are the real icetop pulses
    icetop_clean_hlc_pulses="CleanedHLCTankPulses"  #this is only a pulse mask
    icetop_clean_coinc_pulses="CleanedCoincOfflinePulses"
    icetop_errata = "OfflineIceTopCalibrationErrata"

    # Online frame objects
    filter_mask = "FilterMask"
    icetop_vemcal = "I3VEMCalData"
    superdst = "I3SuperDST"
    superdst_errata = "InIceErrata"
    dstframeobj = "I3DST11"
    dstheaderobj = "I3DST11Header"
    eventheader = "I3EventHeader"
    triggerhierarchy = "I3TriggerHierarchy"

    muon_llhfit = "PoleMuonLlhFit"

    gcd_keeps = ["I3Geometry",
                 "I3Calibration",
                 "I3DetectorStatus",
                 ]

    online_base_keeps = [eventheader,
                 "DrivingTime",
                 "JEBEventInfo",
                 dstframeobj,
                 dstheaderobj,
                 #icetop_vemcal,  don"t need this.  used online for monitoring
                 triggerhierarchy,
                 superdst,
                 superdst_errata,
                 filter_mask,
                 "PassedConventional",
                 raw_inice_readout,
                 raw_icetop_readout,
                 ]

    online_reco_keeps =[muon_llhfit,
                muon_llhfit+"FitParams",
                ]

    online_l2_keeps = ["PoleL2IpdfGConvolute_2it",  #Reco results from online L2
               "PoleL2IpdfGConvolute_2itFitParams",
               "PoleL2MPEFit",
               "PoleL2MPEFitFitParams",
               "PoleL2MPEFitCuts",
               "PoleL2MPEFitMuE",
               "PoleL2BayesianFit",
               "PoleL2BayesianFitFitParams",
               "CramerRaoPoleL2IpdfGConvolute_2itParams",
               "CramerRaoPoleL2MPEFitParams",
               "PoleMuonLlhFitCutsFirstPulseCuts",
               "PoleL2SPE2it_TimeSplit1",
               "PoleL2SPE2it_TimeSplit2",
               "PoleL2SPEFit2it_GeoSplit1",
               "PoleL2SPEFit2it_GeoSplit2"
               ]

    mc_keeps = ["CorsikaWeightMap",
            "I3MCTree",
            "I3MCTreeCMC",
            "I3MCWeightDict",
            "MMCTrackList",
            "MCHitSeriesMap",
            "MCPrimary1",
            "MCPrimary2",
            "MCPrimary3",
            "MCPrimary4",
            "MCPrimary5",
            "MCPrimary6",
            "MCPrimary7",
            "MCPrimary8",
            "MCPrimary9",
            "MCPrimary10",
            "MCPrimaryInfo",
            "GCFilterMJD",
            "signal_nch",
            "signal_nhit",
            "MCMostEnergeticTrack",
            "MCMostEnergeticInIce",
            "MCECenter",
            ]

    offline_extraction_keeps = [offline_pulses,
        offline_inice_errata,
        masked_offline_pulses,
        offline_pulses_hlc,
        "SRTOfflinePulses",
        portia_event_name,
        atwd_portia_pulse,
        fadc_portia_pulse,
        icetop_tank_pulse_merger_excluded_stations,
        icetop_cluster_cleaning_excluded_stations,
        icetop_hlc_vem_pulses,
        icetop_slc_vem_pulses,
        icetop_hlc_pulses,
        icetop_clean_hlc_pulses,
        icetop_errata,
        icetop_clean_coinc_pulses,
        icetop_hlc_pulse_info]

    level3_keeps=["TWSRT"+Suffix+"InIcePulses",
        "SRT"+Suffix+"InIcePulses",
        Suffix+"InIcePulses",
        "BestTrack",
        "BestTrackName",
        "BestTrackCuts",
        "BestTrack_AvgDistQ",
        "HitMultiplicityValues",
        "HitMultiplicityValuesIC",
        "HitStatisticsValues",
        "HitStatisticsValuesIC",
        "Recombined",
        "HiveSplitterSplitCount",
        "HiveSplitterReducedCount",
        "SplitGeo1",
        "SplitGeo2",
        "SplitTime1",
        "SplitTime2",
        "CVMultiplicity",
        "CVStatistics",
        "MPEFitCharacteristics",
        "SRTInIcePulses_Qtot",
        "SRTHVInIcePulses_Qtot",
        "SRTInIcePulses_QtotWithDC",
        "SRTHVInIcePulses_QtotWithDC"]

    for track in ["SPEFitSingle_TW"+Suffix,"SPEFit2_TW"+Suffix,"MPEFit_TW"+Suffix,"SPEFitSingle_"+Suffix,\
            "SPEFit2_"+Suffix,"MPEFit_"+Suffix,"MPEFitHighNoise","MPEFitParaboloid","SplineMPE",\
            "SplineMPE_MillipedeHighEnergyMIE","SplineMPE_MillipedeHighEnergySPICE1","SPEFitSingle",\
            "SPEFit2","MPEFit"]:
        level3_keeps+=[track]
        level3_keeps+=[track+"FitParams"]

    for track in ["LineFit_"+Suffix, "LineFit_TW"+Suffix, "LineFit"]:
        level3_keeps+=[track]
        level3_keeps+=[track+"Params"]

    level3_keeps+=["BestTrackCramerRaoParams","SplineMPEMuEXDifferential","SplineMPEMuEXDifferential_r",\
               "SplineMPEMuEXDifferential_list", "MuEXAngular4","MuEXAngular4_Sigma","MuEXAngular4_rllt",
               "SplineMPECramerRaoParams"]

    for track in ["BestTrack","SplineMPE"]:
        level3_keeps+=["{0}ShieldNHitsOnTime".format(track)]
        level3_keeps+=["{0}ShieldNHitsOffTime".format(track)]
        for type in ["HLC","SLC"]:
            level3_keeps+=["{0}Shield{1}".format(track, type)]

    for item in ["","All"]:
        for type in ["BINS","DOMS"]:
            for part in ["MuEres","Muon","Neutrino","dEdX"]:
                level3_keeps+=["SplineMPETruncatedEnergy_SPICEMie_%s%s_%s"\
                     % (item,type,part)]

    for item in ["Muon","Neutrino","dEdX"]:
            level3_keeps+=["SplineMPETruncatedEnergy_SPICEMie_ORIG_%s" % (item)]

    iter=2
    level3_keeps+=["SPEFit%iBayesian" % (iter),"SPEFit%iBayesianFitParams" % (iter)]
    for fit in [1,2]:
        for type in ["Geo","Time"]:
            level3_keeps+=["LineFit%sSplit%i" % (type,fit)]
            level3_keeps+=["LineFit%sSplit%iParams" % (type,fit)]
            for iter in ["Single","2"]:
                level3_keeps+=["SPEFit%s%sSplit%i" % (iter,type,fit)]
                level3_keeps+=["SPEFit%s%sSplit%iFitParams" % (iter,type,fit)]
                level3_keeps+=["SPEFit%s%sSplit%iBayesian" % (iter,type,fit)]
                level3_keeps+=["SPEFit%s%sSplit%iBayesianFitParams" % (iter,type,fit)]

    for track in ["BestTrack","SplineMPE"]:
        level3_keeps+=["%sCharacteristics" % (track)]
        level3_keeps+=["%sCharacteristicsIC" % (track)]
        for type in ["A","B","C","D","E"]:
            level3_keeps+=["%sDirectHits%s" % (track,type)]
            level3_keeps+=["%sDirectHitsIC%s" % (track,type)]

    keep_all = gcd_keeps + online_base_keeps + online_reco_keeps + online_l2_keeps\
           + mc_keeps + offline_extraction_keeps + level3_keeps

    level3_booking_keeps = level3_keeps[:]
    level3_booking_keeps.remove(Suffix+"InIcePulses")
    level3_booking_keeps.remove("SRT"+Suffix+"InIcePulses")
    level3_booking_keeps.remove("TWSRT"+Suffix+"InIcePulses")
    level3_booking_keeps.remove("BestTrackName")
    level3_booking_keeps.remove("SplitTime1")
    level3_booking_keeps.remove("SplitTime2")
    level3_booking_keeps.remove("SplitGeo1")
    level3_booking_keeps.remove("SplitGeo2")
    level3_booking_keeps.remove("SplineMPE_MillipedeHighEnergyMIE")
    level3_booking_keeps.remove("SplineMPE_MillipedeHighEnergySPICE1")
    for track in ["BestTrack","SplineMPE"]:
        for type in ["HLC","SLC"]:
            level3_booking_keeps.remove("{0}Shield{1}".format(track, type))

    mc_keeps_booking = mc_keeps[:]
    mc_keeps_booking.remove("MMCTrackList")
    mc_keeps_booking.remove("MCHitSeriesMap")
    mc_keeps_booking.remove("GCFilterMJD")
    mc_keeps_booking.remove("I3MCTree")
    mc_keeps_booking.remove("I3MCTreeCMC")

    keep_booking = [eventheader,filter_mask] + online_reco_keeps\
                + online_l2_keeps + mc_keeps_booking\
            + level3_booking_keeps

    # calculate variables
    def CleanUpCommonVariables(frame):
        for track in ["BestTrack","SplineMPE"]:
            for timewindow in ["A", "B", "C", "D", "E"]:
                if frame.Has(track+"DirectHits"+timewindow):
                    frame.Delete(track+"DirectHits"+timewindow)
            if frame.Has(track+"Characteristics"):
                frame.Delete(track+"Characteristics")
        for variable in ["HitMultiplicityValues", "HitStatisticsValues", "HitMultiplicityValuesIC", "HitStatisticsValuesIC"]:
            if frame.Has(variable):
                frame.Delete(variable)

    tray.AddModule(CleanUpCommonVariables)

    DirectHitsDefs=direct_hits.default_definitions

    # TimeWindowDefinition E is already in DirectHitsDefs
    #DirectHitsDefs.append(direct_hits.I3DirectHitsDefinition("E",-15.,250.))

    from copy import copy
    def selectIceCubeOnly(frame, Pulses):
        mask=copy(dataclasses.I3RecoPulseSeriesMapMask(frame, Pulses))
        pulsemap=frame[Pulses].apply(frame)
        for omkey in pulsemap.keys():
            if omkey.string in [79,80,81,82,83,84,85,86]:
                mask.set(omkey, False)
        frame[Pulses+"IC"]=mask

    tray.AddModule(selectIceCubeOnly, "ic_only_pulsemap",
        Pulses="TWSRT"+Suffix+"InIcePulses")


    tray.AddSegment(hit_multiplicity.I3HitMultiplicityCalculatorSegment, "HitMultiplicityWriteOutput",
        PulseSeriesMapName                = "TWSRT"+Suffix+"InIcePulses",
        OutputI3HitMultiplicityValuesName = "HitMultiplicityValues",
        BookIt                            = True)

    tray.AddSegment(hit_multiplicity.I3HitMultiplicityCalculatorSegment, "HitMultiplicityWriteOutputIC",
        PulseSeriesMapName                = "TWSRT"+Suffix+"InIcePulsesIC",
        OutputI3HitMultiplicityValuesName = "HitMultiplicityValuesIC",
        BookIt                            = True)

    tray.AddSegment(hit_statistics.I3HitStatisticsCalculatorSegment, "HitStatisticsWriteOutput",
        PulseSeriesMapName              = "TWSRT"+Suffix+"InIcePulses",
        OutputI3HitStatisticsValuesName = "HitStatisticsValues",
        BookIt                          = True,
        COGBookRefFrame                 = dataclasses.converters.I3PositionConverter.BookRefFrame.Sph)

    tray.AddSegment(hit_statistics.I3HitStatisticsCalculatorSegment, "HitStatisticsWriteOutputIC",
        PulseSeriesMapName              = "TWSRT"+Suffix+"InIcePulsesIC",
        OutputI3HitStatisticsValuesName = "HitStatisticsValuesIC",
        BookIt                          = True,
        COGBookRefFrame                 = dataclasses.converters.I3PositionConverter.BookRefFrame.Sph)

    for track in ["BestTrack","SplineMPE"]:
        tray.AddSegment(direct_hits.I3DirectHitsCalculatorSegment, "DirectHits"+track,
            DirectHitsDefinitionSeries       = DirectHitsDefs,
            PulseSeriesMapName               = "TWSRT"+Suffix+"InIcePulses",
            ParticleName                     = track,
            OutputI3DirectHitsValuesBaseName = track+"DirectHits",
            BookIt                           = True)

        tray.AddSegment(direct_hits.I3DirectHitsCalculatorSegment, "DirectHitsIC"+track,
            DirectHitsDefinitionSeries       = DirectHitsDefs,
            PulseSeriesMapName               = "TWSRT"+Suffix+"InIcePulsesIC",
            ParticleName                     = track,
            OutputI3DirectHitsValuesBaseName = track+"DirectHitsIC",
            BookIt                           = True)

        tray.AddSegment(track_characteristics.I3TrackCharacteristicsCalculatorSegment, "TrackCharacterists"+track,
            PulseSeriesMapName                     = "TWSRT"+Suffix+"InIcePulses",
            ParticleName                           = track,
            OutputI3TrackCharacteristicsValuesName = track+"Characteristics",
            TrackCylinderRadius                    = 150*I3Units.m,
            BookIt                                 = True)

        tray.AddSegment(track_characteristics.I3TrackCharacteristicsCalculatorSegment, "TrackCharacteristsIC"+track,
            PulseSeriesMapName                     = "TWSRT"+Suffix+"InIcePulsesIC",
            ParticleName                           = track,
            OutputI3TrackCharacteristicsValuesName = track+"CharacteristicsIC",
            TrackCylinderRadius                    = 150*I3Units.m,
            BookIt                                 = True)

    #tray.AddModule( "Keep", "CleanUpKeys",
    #    Keys = keep_all)

    def GetPrimary(frame):
        if frame.Stop==icetray.I3Frame.Physics and frame.Has("I3MCTree") and not frame.Has("MCPrimary1"):
            primaries=frame["I3MCTree"].primaries
            if len(primaries)==1:
                idx=0
            elif "I3MCWeightDict" in frame:
                idx=[i for i in range(len(primaries)) if primaries[i].is_neutrino][0]
            elif "CorsikaWeightMap" in frame:
                wmap=frame["CorsikaWeightMap"]
                primary_type=0
                if "PrimaryType" in wmap:
                    primary_type=wmap["PrimaryType"]
                elif "ParticleType" in wmap:
                    primary_type=wmap["ParticleType"]
                elif "SpectrumType" in wmap: # unweighted corsika
                    if "PrimarySpectralIndex" in wmap:
                        prim_e=wmap["Weight"]**(-1./wmap["PrimarySpectralIndex"])
                        idx=int(np.argmin([abs(p.energy-prim_e) for p in primaries]))
                    elif "SpectralIndexChange" in wmap and wmap["SpectralIndexChange"]>0.01:
                        prim_e=wmap["Weight"]**(-1./wmap["SpectralIndexChange"])
                        idx=int(np.argmin([abs(p.energy-prim_e) for p in primaries]))
                    else: # for unweighted corsika (Hoerandel) the primary should be arbitrary
                        idx=0
                else:
                    icetray.logging.log_warn("Unkown type of corsika simulation. You cannot"\
                    " rely on MCPrimary1 beeing the weighting primary")
                if "PrimaryType" in wmap or "ParticleType" in wmap:
                    primaries=[p for p in primaries if p.type==primary_type]
                if len(primaries)==1:
                    idx=0
                elif "PrimaryEnergy" in wmap:
                    prim_e=wmap["PrimaryEnergy"]
                    idx=int(np.argmin([abs(p.energy-prim_e) for p in primaries]))
                elif "PrimarySpectralIndex" in wmap:
                    prim_e=wmap["Weight"]**(-1./wmap["PrimarySpectralIndex"])
                    idx=int(np.argmin([abs(p.energy-prim_e) for p in primaries]))
                else:
                    idx=0

            frame["MCPrimary1"]=primaries[idx]
            i=2
            for p in frame["I3MCTree"].primaries:
                if (p.minor_id!=primaries[idx].minor_id) or (p.major_id!=primaries[idx].major_id):
                    frame["MCPrimary{0}".format(i)]=p
                    i+=1

        return True


    def GetMostEnergeticTrack(frame):
        if frame.Stop==icetray.I3Frame.Physics and frame.Has("I3MCTree"):
            if frame.Has("I3MCWeightDict") and not frame.Has("MCMostEnergeticTrack"):
                # neutrinos aren"t tracks (but are in-ice), so most_energetic_track gets in-ice muon
                frame["MCMostEnergeticTrack"]=frame["I3MCTree"].most_energetic_track
            if frame.Has("CorsikaWeightMap") and not frame.Has("MCMostEnergeticInIce"):
                # primary nucleons aren"t in-ice (but are tracks), so most_energetic_in_ice get in-ice muon
                frame["MCMostEnergeticInIce"]=frame["I3MCTree"].most_energetic_in_ice
            if not frame.Has("MCECenter"):
                maxE=0
                for item in frame["MMCTrackList"]:
                    if item.GetEc()>maxE:
                        maxE=item.GetEc()
                frame["MCECenter"]=dataclasses.I3Double(maxE)


    # Replace I3MCTree with I3LinearizedMCTree to save diskspace
    def replaceMCTree(frame):
        if frame.Has("I3MCTree"):
            tree=frame["I3MCTree"]
            del frame["I3MCTree"]
            frame["I3MCTree"]=dataclasses.I3LinearizedMCTree(tree)

    # Write hd5"s, root"s, and i3"s
    tray.AddModule("I3Writer",
        Filename = output_i3,
        SkipKeys=["CalibratedWaveforms", "CleanIceTopRawData", "CleanInIceRawData"],
        DropOrphanStreams=[icetray.I3Frame.DAQ],
        Streams=[icetray.I3Frame.DAQ, icetray.I3Frame.Physics])
