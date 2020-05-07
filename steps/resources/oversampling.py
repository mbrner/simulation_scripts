from __future__ import division

from icecube import icetray, dataclasses


class DAQFrameMultiplier(icetray.I3ConditionalModule):
    def __init__(self, context):
        """Class to create and inject Cascades.

        Parameters
        ----------
        context : TYPE
            Description
        """
        icetray.I3ConditionalModule.__init__(self, context)
        self.AddOutBox('OutBox')
        self.AddParameter('oversampling_factor',
                          'Oversampling Factor to be used. Simulation is '
                          'averaged over these many simulations.',
                          None)
        self.AddParameter('mctree_keys',
                          'The I3MCTree keys to multiply.',
                          ['I3MCTree_preMuonProp', 'I3MCTree'])

    def Configure(self):
        """Configures DAQFrameMultiplier.

        Raises
        ------
        ValueError
            If interaction type or flavor is unkown.
        """
        self.mctree_keys = self.GetParameter('mctree_keys')
        self.oversampling_factor = self.GetParameter('oversampling_factor')
        if self.oversampling_factor is None:
            self.oversampling_factor = 1
        self.events_done = 0

        # sanity checks:
        if self.oversampling_factor < 1:
            raise ValueError('Oversampling must be set to "None" or integer'
                             ' greater than 1. It is currently set to: '
                             '{!r}'.format(self.oversampling_factor))

    def DAQ(self, frame):
        """Inject casacdes into I3MCtree.

        Parameters
        ----------
        frame : icetray.I3Frame.DAQ
            An I3 q-frame.

        """

        trees = [frame[t] for t in self.mctree_keys]

        # oversampling
        for i in range(self.oversampling_factor):
            if i > 0:
                # create a new frame
                frame = icetray.I3Frame(frame)
                del frame['oversampling']

                for key, tree in zip(self.mctree_keys, trees):
                    del frame[key]
                    frame[key] = dataclasses.I3MCTree(tree)

            if self.oversampling_factor > 1:
                frame['oversampling'] = dataclasses.I3MapStringInt({
                                        'event_num_in_run': self.events_done,
                                        'oversampling_num': i,
                                    })
            self.PushFrame(frame)

        self.events_done += 1
