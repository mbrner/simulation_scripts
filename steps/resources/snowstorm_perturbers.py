import numpy as np

from icecube.dataclasses import I3Matrix
from icecube.snowstorm import MultivariateNormal
from icecube.ice_models.icewave.PlusModeParametrization import \
    PlusModeParametrization


def custom_uncorrelated_variation(modes_to_shift, amp_sigmas, phase_sigmas,
                                  **kwargs):
    """Create a custom uncorrelated fourier component perturbation

    Get custom ice model variations based on:
        https://github.com/UTA-REST/multisim

    Parameters
    ----------
    modes_to_shift : list of int
        A list of (zero-based) mode numbers to shift.
    amp_sigmas : list of float
        The sigmas for the amplitude shifts.
    phase_sigmas : list of float
        The sigmas for the phase shifts.
    **kwargs
        Arbitrary keyword arguments.

    Returns
    -------
    tuple (parametrization, distribution)
        Returns a tuple of a parameterization and a distribution which can be
        used with the  icecube.snowstorm.Perturber.
    """
    modes_to_shift = np.asarray(modes_to_shift)
    amp_sigmas = np.asarray(amp_sigmas)
    phase_sigmas = np.asarray(phase_sigmas)

    assert phase_sigmas.size == modes_to_shift.size
    assert phase_sigmas.size == amp_sigmas.size

    variance = np.concatenate((amp_sigmas, phase_sigmas))**2

    parametrization = PlusModeParametrization(modes_to_shift)
    distribution = MultivariateNormal(
        I3Matrix(np.diag(variance)), [0.]*variance.size)

    return parametrization, distribution
