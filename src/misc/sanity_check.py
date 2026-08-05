# find td-fence out of the quantiles of reference data
# find how many points are out of fence for each strata
# output the table as a pandas dataframe df = [[ model, date, colllection, variable, level, strata_name, n_below_tdfenc, n_above_tdfence]]
# actual t-digest is not needed for this


# quads parameters
import numpy as np
from make_tdigest import create_digest, get_quantiles_from_tdigest

innerq = 0.0001
outerq =  0.0000001
tail_scalar = 1.5
median_scalar =  6
#PSI_threshold = 1.5

def cdf(q, reference_quantile_data):
    data, quantiles = reference_quantile_data # comes from the output of get_quantiles_from_tdigest
    for i in range(len(quantiles)):
        if quantiles[i] == q:
            return  data[i]


def fence(reference_quantile_data):
    data = reference_quantile_data
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

    return lower_bound, upper_bound

def edge_check(input_data, reference_quantile_data):
    """
    Count how many points fall outside the reference bounds.
    input_data: 1-D array-like (list, np.ndarray)
    reference_quantile_data: whatever fence() expects
    """
    lower_bound, higher_bound = fence(reference_quantile_data)

    a = np.asarray(input_data).ravel()
    a = a[np.isfinite(a)]  # drop NaN/Inf

    n_low = int(np.count_nonzero(a < lower_bound))
    n_high = int(np.count_nonzero(a > higher_bound))
    return n_low, n_high

if __name__ == "__main__":
    N = 10_000_000
    data = np.random.uniform(0, 100, N)

    td = create_digest(data)
    print(type(td))
   # print(end - start)

    #qs = [.0000001, .000001, .00001, .0001, .001, .01] \
     #+ list(np.arange(.1, .95, .05)) \
     #+ [.99, .999, .9999, .99999, .999999, .9999999]

    reference_quantile_data = get_quantiles_from_tdigest(td)
    edge_check(data, reference_quantile_data)

