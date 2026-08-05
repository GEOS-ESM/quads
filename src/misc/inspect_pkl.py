import pickle
from pathlib import Path

#pkl_path = Path("/home/sadhika8/JupyterLinks/nobackup/quads_data/GEOSIT/2024/05/monthly_merged_digest_GEOSIT_2024-05.pkl")
pkl_path = Path("/home/sadhika8/JupyterLinks/nobackup/quads_data/GEOSFP/2024/05/monthly_merged_digest_GEOSFP_2024-05.pkl")

with pkl_path.open("rb") as fh:
    data = pickle.load(fh)

print(type(data))
print(len(data))
print(data[0])
#print(data.keys())
#print(data['inst1_2d_hwl_Nx|TOTSCATAU|None|Strat8'].keys())
#digest_dict = data[10000] # one piece of dictionary

#centroids = digest_dict["centroids"]
#quantiles = digest_dict["quantiles"]

#print(quantiles)
