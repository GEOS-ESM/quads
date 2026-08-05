import pickle
import numpy as np
import matplotlib.pyplot as plt

with open("debug_results.pkl", "rb") as f:
    results = pickle.load(f)

def plot_id(results, id_key, bins=60):
    r = next(x for x in results if x["id_key"] == id_key)

    flat = np.asarray(r["flat"])
    flat = flat[np.isfinite(flat)]

    q_vals = np.asarray(r["q_vals"])
    q_list = np.asarray(r["q_list"])

    fig, ax1 = plt.subplots()
    ax1.plot(q_list, q_vals)
    ax1.set_xlabel("q (quantile level)")
    ax1.set_ylabel("value at q")

    ax2 = ax1.twinx()
    ax2.hist(flat, bins=bins, density=True, alpha=0.3)
    ax2.set_ylabel("flat density")

    ax1.set_title(id_key)
    plt.show()

# list available keys
keys = [r["id_key"] for r in results]
print("num keys:", len(keys))
print("example:", keys[0])

plot_id(results, keys[0])

