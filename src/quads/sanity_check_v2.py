# quads core sanity check -- finds a 'fence' based on a reference historical data and counts how many points are out of fence

import numpy as np
from quads.make_tdigest import create_digest, get_quantiles_from_tdigest

innerq = 0.0001
outerq =  0.0000001
tail_scalar = 1.5
median_scalar =  6
#PSI_threshold = 1.5 - to be done


def cdf(q, reference_quantile_data):
    """
    Given reference quantile data `(data, quantile_list)` and a quantile q,
    estimate the data value at quantile q using linear interpolation.

    q: float in [0, 1]
    data: values corresponding to quantiles
    quantile_list: quantiles corresponding to data
    """
    data, quantile_list = reference_quantile_data

    data = np.asarray(data, dtype=float)
    quantile_list = np.asarray(quantile_list, dtype=float)

    # Linear interpolation
    return np.interp(q, quantile_list, data)

def fence(data):
    """find the fence for given reference_quantile_data"""
    #data = reference_quantile_data - reference quantile data two lists
    i = cdf(1-innerq, data)
    o = cdf(1-outerq, data)
    r = o - i
    h = o - cdf(0.5, data)
    e = r*tail_scalar
    c = h/median_scalar
    upper_bound = o + e + c

    i_low = cdf(innerq, data)
    o_low = cdf(outerq, data)
    r_low = i_low - o_low
    h_low = cdf(0.5, data) - o_low
    e_low = r_low*tail_scalar
    c_low =  h_low/median_scalar
    lower_bound = o_low - e_low - c_low
    #print("fence", lower_bound, upper_bound)
    return lower_bound, upper_bound

def edge_check(input_data, reference_quantile_data):
    """
    Count how many points fall outside the reference bounds.
    input_data: 1-D array-like (list, np.ndarray)
    reference_quantile_data: tuple of two lists: data, quantiles of that data, it is the direct output 
    of get_quantiles_from_tdigest
    """
    lower_bound, higher_bound = fence(reference_quantile_data)

    a = np.asarray(input_data).ravel()
    a = a[np.isfinite(a)]  # drop NaN/Inf

    n_low = int(np.count_nonzero(a < lower_bound))
    n_high = int(np.count_nonzero(a > higher_bound))
    return n_low, n_high, lower_bound, higher_bound

if __name__ == "__main__":
    N = 10_000_000
    #data = np.random.uniform(0, 100, N)
    data = np.zeros(N)

    td = create_digest(data)
    print(type(td))

    reference_quantile_data = get_quantiles_from_tdigest(td)
    print(edge_check(data, reference_quantile_data))
    
# Use Wasserstein distance instead of PSI. It still quantifies how much of 
# drift is there, does not need fixed grids, and naturally fits with quantiles 
# obtained from t-digests. One Caveat: Wd has absolute value, so might need normalization -- such as divide it by standard deviation
