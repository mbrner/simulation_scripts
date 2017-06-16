import os

from icecube import phys_services

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
    elif dataset_number >= MAX_DATASET_NUMBER:
        raise RuntimeError("dataset numbers > %u are not supported".format(
            MAX_DATASET_NUMBER))


    int_run_number = dataset_number * MAX_RUN_NUMBER + run_number

    max_int_run_number = MAX_DATASET_NUMBER * MAX_RUN_NUMBER
    random_service = phys_services.I3SPRNGRandomService(
        seed=seed,
        nstreams=max_int_run_number * 2,
        streamnum=int_run_number + max_int_run_number)

    random_service_prop = phys_services.I3SPRNGRandomService(
        seed=seed,
        nstreams=max_int_run_number * 2,
        streamnum=int_run_number)
    return random_service, random_service_prop
