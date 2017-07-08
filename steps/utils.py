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


def get_run_folder(run_number, runs_per_folder=1000):
    fill = int(np.log10(MAX_RUN_NUMBER) + 0.5)
    start = (run_number // runs_per_folder) * runs_per_folder
    stop = start + runs_per_folder - 1
    return '{}-{}'.format(str(start).zfill(fill), str(stop).zfill(fill))
