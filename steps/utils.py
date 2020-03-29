import numpy as np

MAX_DATASET_NUMBER = 100000
MAX_RUN_NUMBER = 100000


def create_random_services(dataset_number, run_number, seed, n_services=1,
                           use_gslrng=False):
    from icecube import phys_services, icetray, dataclasses
    if run_number < 0:
        raise RuntimeError("negative run numbers are not supported")
    elif run_number >= MAX_RUN_NUMBER:
        raise RuntimeError("run numbers > %u are not supported".format(
            MAX_RUN_NUMBER))

    if dataset_number < 0:
        raise RuntimeError("negative dataset numbers are not supported")

    max_run_num = MAX_RUN_NUMBER // 10

    int_run_number = dataset_number * max_run_num + run_number

    random_services = []
    for i in range(n_services):
        streamnum = run_number + (MAX_RUN_NUMBER * i)

        if use_gslrng:
            random_services.append(phys_services.I3GSLRandomService(
                seed=seed*MAX_RUN_NUMBER*n_services + streamnum))
        else:
            random_services.append(phys_services.I3SPRNGRandomService(
                seed=seed,
                nstreams=MAX_RUN_NUMBER * n_services,
                streamnum=streamnum))

    return random_services, int_run_number


def get_run_folder(run_number, runs_per_folder=1000):
    fill = int(np.log10(MAX_RUN_NUMBER) + 0.5)
    start = (run_number // runs_per_folder) * runs_per_folder
    stop = start + runs_per_folder - 1
    return '{}-{}'.format(str(start).zfill(fill), str(stop).zfill(fill))


muongun_keys = ['MCOversizeStreamDefault',
                'MCOversizeStream0',
                'MCOversizeStream1',
                'MCOversizeStream2',
                'MCOversizeStream3',
                'MCMuon',
                'MuonEffectiveArea',
                'MCOversizing',
                'GenerateCosmicRayMuons',
                'MCDomThresholds',
                'MCDistanceCuts']
