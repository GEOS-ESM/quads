import numpy as np
from pytdigest import TDigest        # explicit package
# check details here: https://raw.githubusercontent.com/protivinsky/pytdigest/main/pytdigest/pytdigest.py

def create_digest(data, weights=None, compression=300):
    """Create a t-digest object for given data and compression parameter"""
    data = np.asarray(data)
    #if weights is None:
    #    weights = np.ones_like(data)
    td = TDigest.compute(data, weights, compression=compression)  # vectorised C loop
    return td

def get_quantiles_from_tdigest(td,
                               quantile_list=
                                  ( [.0000001, .000001, .00001, .0001, .001, .01]
                                   + list(np.arange(.1, .95, .05)) 
                                   + [.99, .999, .9999, .99999, .999999, .9999999])
                               ):
    """Create quantiles from a given data->t-digest and specified quantile values."""
    #td = create_digest(data)
    #print(td)
    #td = digest
    quantiles = [td.inverse_cdf(q) for q in quantile_list]
    return  quantiles, quantile_list

if __name__ == "__main__":

    N = 10_000_000
    data = np.random.uniform(0, 100, N)

    td = create_digest(data)
    print(type(td))
   # print(end - start)

    #qs = [.0000001, .000001, .00001, .0001, .001, .01] \
    # + list(np.arange(.1, .95, .05)) \
    # + [.99, .999, .9999, .99999, .999999, .9999999]

    print(get_quantiles_from_tdigest(td))

    #digest_dict = td.to_dict()
    #print(digest_dict)

