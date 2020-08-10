from __future__ import division

from I3Tray import I3Tray
from icecube import icetray, dataclasses, dataio

from ic3_labels.labels.utils import detector
from ic3_labels.labels.utils import muon as mu_utils


class MuonGeometryFilter(icetray.I3ConditionalModule):

    """Class to filter out muons based on their geometry.
    This is intended to run before PROPOSAL.

    Attributes
    ----------
    mctree_name : str
        The name of the I3MCTree key.
    range_length_inside_combined : None or tuple of float
        Allowed range for combined length in upper and lower
        convex hulls.
    range_length_inside_icecube : None or tuple of float
        Allowed range for length in IceCube hull.
    range_length_inside_icecube_lower : None or tuple of float
        Allowed range for length in lower IceCube hull.
    range_length_inside_icecube_upper : None or tuple of float
        Allowed range for length in upper IceCube hull.
    """

    def __init__(self, context):
        """Class to import events from another I3-File

        Parameters
        ----------
        context : TYPE
            Description
        """
        icetray.I3ConditionalModule.__init__(self, context)
        self.AddOutBox('OutBox')
        self.AddParameter(
            'range_length_inside_combined',
            'Only muons with a combined length inside the upper and lower '
            'IceCube hulls within this specified range are selected. '
            'If None no selection will be performed based on this variable.',
            None)
        self.AddParameter(
            'range_length_inside_icecube',
            'Only muons with a length inside the IceCube hull within this '
            'specified range are selected. '
            'If None no selection will be performed based on this variable.',
            None)
        self.AddParameter(
            'range_length_inside_icecube_upper',
            'Only muons with a length inside the upper IceCube hull within '
            'this specified range are selected. '
            'If None no selection will be performed based on this variable.',
            None)
        self.AddParameter(
            'range_length_inside_icecube_lower',
            'Only muons with a length inside the lower IceCube hull within '
            'this specified range are selected. '
            'If None no selection will be performed based on this variable.',
            None)
        self.AddParameter(
            'mctree_name', 'Name of I3MCTree.', 'I3MCTree_preMuonProp')

    def Configure(self):
        """Configures MuonGeometryFilter.
        """
        self.range_length_inside_combined = self.GetParameter(
            'range_length_inside_combined')
        self.range_length_inside_icecube = self.GetParameter(
            'range_length_inside_icecube')
        self.range_length_inside_icecube_upper = self.GetParameter(
            'range_length_inside_icecube_upper')
        self.range_length_inside_icecube_lower = self.GetParameter(
            'range_length_inside_icecube_lower')

        self.mctree_name = self.GetParameter('mctree_name')

        # sanity checks
        for value_range in (
                self.range_length_inside_combined,
                self.range_length_inside_icecube,
                self.range_length_inside_icecube_upper,
                self.range_length_inside_icecube_lower,
                ):
            if value_range is not None:
                assert len(value_range) == 2, value_range
                assert value_range[1] > value_range[0], value_range

    def DAQ(self, frame):
        """Filter events based on muon geometry.
        """

        # get primary
        mc_tree = frame[self.mctree_name]
        primaries = mc_tree.get_primaries()
        assert len(primaries) == 1, 'Expected only 1 Primary!'

        # get muon
        muon = mu_utils.get_muon(
            frame, primaries[0], detector.icecube_hull,
            mctree_name=self.mctree_name,
        )

        passed_filter = True

        # ------------------
        # Filter for lengths
        # ------------------
        dist_icecube = mu_utils.get_muon_track_length_inside(
                muon, detector.icecube_hull)
        dist_upper = mu_utils.get_muon_track_length_inside(
                muon, detector.icecube_hull_upper)
        dist_lower = mu_utils.get_muon_track_length_inside(
                muon, detector.icecube_hull_lower)

        value_list = [
            dist_icecube,
            dist_upper,
            dist_lower,
            dist_lower + dist_upper,
        ]
        range_list = [
            self.range_length_inside_icecube,
            self.range_length_inside_icecube_upper,
            self.range_length_inside_icecube_lower,
            self.range_length_inside_combined,
        ]

        for value, allowed_range in zip(value_list, range_list):
            if allowed_range is not None:
                if value < allowed_range[0] or value > allowed_range[1]:
                    passed_filter = False
        # ------------------

        if passed_filter:
            self.PushFrame(frame)


class MuonLossProfileFilter(icetray.I3ConditionalModule):

    """Class to filter out muons based on their loss profile.
    This is intended to run after PROPOSAL.

    Attributes
    ----------
    mctree_name : str
        The name of the I3MCTree key.
    """

    def __init__(self, context):
        """Class to import events from another I3-File

        Parameters
        ----------
        context : TYPE
            Description
        """
        icetray.I3ConditionalModule.__init__(self, context)
        self.AddOutBox('OutBox')
        self.AddParameter(
            'mctree_name', 'Name of I3MCTree.', 'I3MCTree_preMuonProp')

    def Configure(self):
        """Configures MuonLossProfileFilter.
        """
        self.mctree_name = self.GetParameter('mctree_name')

    def DAQ(self, frame):
        """Filter events based on muon geometry.
        """

        # get primary
        mc_tree = frame[self.mctree_name]
        primaries = mc_tree.get_primaries()
        assert len(primaries) == 1, 'Expected only 1 Primary!'

        # get muon
        muon = mu_utils.get_muon(
            frame, primaries[0], detector.icecube_hull,
            mctree_name=self.mctree_name,
        )

        passed_filter = True

        # -------------
        # filter events
        # -------------
        # ToDo: add filtering logic
        # -------------

        if passed_filter:
            self.PushFrame(frame)
