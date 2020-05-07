from __future__ import division

from I3Tray import I3Tray
from icecube import icetray, dataclasses, dataio


def export_frame(frame, frame_list, mctree_name, keys_to_export, rename_dict):
    """Export frames and append them to the provided frame list.

    Parameters
    ----------
    frame : I3Frame
        The current I3Frame.
    frame_list : list
        The list to which to append the exported frames.
    mctree_name : str
        The name of the I3MCTree key.
    keys_to_export : list of str
        The keys to extract save to the exported frame
    rename_dict : dict
        A dictionary that defines the renaming of the keys.
        Signature: rename_dict[old_name] = new_name
    """

    # create a new DAQ frame
    fr = icetray.I3Frame(icetray.I3Frame.DAQ)

    # extract I3MCTree
    fr[mctree_name] = dataclasses.I3MCTree(frame[mctree_name])

    # extract specified keys
    for key in keys_to_export:

        if key in frame:
            new_name = rename_dict.get(key, key)
            fr[new_name] = frame[key]

    # append frame to list
    frame_list.append(fr)


class ImportEvents(icetray.I3ConditionalModule):

    """Class to import events from another I3-File

    Attributes
    ----------
    files : list of str
        The list of I3-files from which to import events.
    keys_to_import : list of str
        The keys to import from the provided files.
    mctree_name : str
        The name of the I3MCTree key.
    rename_dict : dict
        A dictionary that defines the renaming of the keys.
        Signature: rename_dict[old_name] = new_name
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
        self.AddParameter('mctree_name',
                          'The name of the I3MCTree.',
                          'I3MCTree')
        self.AddParameter('files',
                          'The list of I3-files from which to import events.',
                          None)
        self.AddParameter('num_events',
                          'The number of events to import. '
                          'If None, all events are imported.',
                          None)
        self.AddParameter('keys_to_import',
                          'The list of frame keys to import.',
                          [])
        self.AddParameter('rename_dict',
                          'A dictionary that defines the renaming of the keys '
                          'to import: rename_dict[old_name] = new_name',
                          {})

    def Configure(self):
        """Configures ImportEvents.
        """
        self.mctree_name = self.GetParameter('mctree_name')
        self.files = self.GetParameter('files')
        self.num_events = self.GetParameter('num_events')
        self.keys_to_import = self.GetParameter('keys_to_import')
        self.rename_dict = self.GetParameter('rename_dict')

    def Process(self):
        """Inject frames.
        """
        # read in files and create frames
        frames = self.create_frames(self.files)

        # push frames
        event_counter = 0
        for frame in frames:

            # check if we have added the number of specified events
            if self.num_events is not None:
                if event_counter >= self.num_events:
                    break

            self.PushFrame(frame)
            event_counter += 1

        # end frame stream
        self.RequestSuspension()

    def create_frames(self, files):
        """Create a list of frames to import from the provided files

        Parameters
        ----------
        files : list of str
            The list of file paths from which to import the events.
        """
        frame_list = []
        tray = I3Tray()
        tray.context['I3FileStager'] = dataio.get_stagers()
        tray.Add('I3Reader', FilenameList=files)
        tray.Add(
            export_frame, 'export_frame',
            frame_list=frame_list,
            mctree_name=self.mctree_name,
            keys_to_export=self.keys_to_import,
            rename_dict=self.rename_dict,
        )
        tray.Execute()
        tray.Finish()

        return frame_list
