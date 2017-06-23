import numpy as np

from icecube import phys_services, icetray, dataclasses

MAX_DATASET_NUMBER = 100000
MAX_RUN_NUMBER = 100000


def create_random_services(dataset_number, run_number, seed):
    if run_number < 0:
        raise RuntimeError("negative run numbers are not supported")
    elif run_number >= MAX_RUN_NUMBER:
        raise RuntimeError("run numbers > %u are not supported".format(
            MAX_RUN_NUMBER))

    if dataset_number < 0:
        raise RuntimeError("negative dataset numbers are not supported")

    int_run_number = dataset_number * MAX_RUN_NUMBER + run_number

    random_service = phys_services.I3SPRNGRandomService(
        seed=seed,
        nstreams=MAX_RUN_NUMBER * 2,
        streamnum=run_number + MAX_RUN_NUMBER)

    random_service_prop = phys_services.I3SPRNGRandomService(
        seed=seed,
        nstreams=MAX_RUN_NUMBER * 2,
        streamnum=run_number)
    return random_service, random_service_prop, int_run_number


def no_oversize_stream(frame):
    if frame.Stop == icetray.I3Frame.DAQ:
        if frame.Has('no_oversize_stream'):
            if frame['no_oversize_stream']:
                return True
            else:
                return False
        else:
            raise KeyError('no_oversize_stream not found')
    else:
        return True


def oversize_stream(frame):
    if frame.Stop == icetray.I3Frame.DAQ:
        if frame.Has('no_oversize_stream'):
            if frame['no_oversize_stream']:
                return False
            else:
                return True
        else:
            raise KeyError('no_oversize_stream not found')
    else:
        return True


class qStreamSwitcher(icetray.I3ConditionalModule):
    q_stream = icetray.I3Frame.Stream('q')

    def __init__(self, context):
        icetray.I3ConditionalModule.__init__(self, context)

    def Configure(self):
        self.Register(self.q_stream, self.qFrame)
        self.switch = True

    def qFrame(self, frame):
        if self.switch:
            frame.stop = icetray.I3Frame.DAQ
        self.PushFrame(frame)


class OversizeSplitter(qStreamSwitcher):
    def __init__(self, context):
        icetray.I3ConditionalModule.__init__(self, context)
        self.AddParameter('threshold',                 # name
                          'Cut distance',       # doc
                          10)                           # default
        self.AddParameter('split_streams',
                          'Split into DAQ-frames for large oversize and q-'
                          'frames.',
                          False)
    def Configure(self):
        super(qStreamSwitcher, self).Configure()
        self.threshold = self.GetParameter('threshold')
        self.split_streams = self.GetParameter('split_streams')
        self.switch = False

    def Geometry(self, frame):
        omgeo = frame['I3Geometry'].omgeo
        self.dom_positions = np.zeros((len(omgeo), 3))
        for i, (_, om) in enumerate(omgeo.iteritems()):
            self.dom_positions[i, :] = np.array(om.position)
        self.PushFrame(frame)

    def DAQ(self, frame):
        particle = frame['MCMuon']
        v_dir = np.array([particle.dir.x, particle.dir.y, particle.dir.z])
        v_pos = np.array(particle.pos)
        distances = np.linalg.norm(np.cross(v_dir, v_pos-self.dom_positions),
                                   axis=1)
        if any(distances < self.threshold):
            frame['no_oversize_stream'] = dataclasses.I3Bool(True)
            if self.split_streams:
                frame.stop = self.q_stream
        else:
            frame['no_oversize_stream'] = dataclasses.I3Bool(False)
        self.PushFrame(frame)
