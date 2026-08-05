# Quads-v2 Technical Report

Quads-v2 is a rewrite of Quads-v1 designed to restructure the code for modularity, simplify the data-storage scheme, eliminate Java dependencies, optimize HPC resource usage, and improve user flexibility. Operating the previous Java-dependent version introduced several serious challenges:

**Complex dependency stack:**  
Although the system was primarily written in Python, it relied on multiple Java libraries, which caused HPC job failures and unpredictable JVM behavior.

**Poor code maintainability:**  
The original design included cyclically interdependent Python classes (A calls B and B calls A) and an overly complex database setup involving both SQLite and PostgreSQL, making the system difficult to maintain or extend.

**Limited user flexibility:**  
The architecture made it hard for users to adapt or customize workflows.

**Resource-usage problems:**  
Straightforward use of Python’s multiprocessing module could trigger RAM overflows, and writing hundreds of thousands of tiny `.pkl` files easily pushed jobs over in-node file-quota limits.

---

# Quads-v2 Essential Components

Quads performs basic sanity checks on GMAO model data. For data corresponding to a given **model, date, collection, variable, level, and latitude stratum**, Quads compares it with historical data and determines whether there are abnormalities based on range checks (distribution-based checks will be extended later).

The Quads-v2 codebase has two key parts.

The first part, which is more relevant to developers, builds a SQLite database and stores statistical properties of historical datasets for each data chunk (with its key determined by model, date, collection, variable, level, and latitude stratum). These statistical properties are quantified as **t-digests and quantiles**.

From the user’s perspective, the workflow is simple: users provide the required inputs, submit a batch job, and receive results that, for each chunk of data, query the historical database, determine how many data points are outliers, and output a result table (this will change over iterations). Users can also obtain the number of violations for each chunk, visualizations, and additional diagnostics.

The philosophy is to give users the flexibility to run the analysis required for their workflow in a convenient way, rather than building a GUI with a pre-determined set of outputs.

---

# Design

It follows a simple modular design—easy to understand, extend, and maintain.

## From the developers’ perspective

For a given **model and date** (usually a day), go to the data source and obtain a dictionary of

```
{collection_name: files_list}
```

using `get_collections_and_files.py`.

For each model, a `.yaml` file is pre-prepared with a list of all collections. The correctness and completeness of this list is critical.

For each collection:

1. Consolidate files so that there is **one file per collection per day**.
2. From the consolidated file, extract chunks of data based on **variable, level, and latitude strata**.
3. Compute **t-digest objects**.

This produces several thousand

```
(unique_identifier_key, t-digest)
```

pairs stored in a single `.pkl` file.

For a typical month, this produces **30 `.pkl` files**, each containing close to **100k key–digest pairs**.

For a given model and date,

```
(collection, variable, level, latitude strata)
```

form a **unique identifier key**.

This step is implemented in `compute_and_save_daily_digests.py`.

For each month:

1. Merge the daily t-digests with their identifier keys into a single **monthly digest `.pkl` file** (`merge_digests_and_write_pickle.py`)
2. Copy that data into a **SQLite database** (`copy_from_monthly_pickle_to_sqlitedb.py`)

This enables users to easily compare new datasets against historical statistics.

---

## From the users’ perspective

For a given **day or month**, users may want to see how many data violations occur across each data chunk identified by

```
(collection, variable, level, latitude strata)
```

The workflow begins by retrieving

```
{collection_name: files_list}
```

for the specified model and date.

For each chunk of data:

- Individual data points are compared against historical **t-digest objects stored in the SQLite database**
- Users may compare against last year’s monthly t-digest or any other reference dataset

Users submit a batch job:

```
submit_job.sh <model> <date>
```

and perform the analysis required for their workflow.

---

# Database Creation

One-time run for each database:

```
create_monthly_aggregated_database.py
```

## Database Details

Supported models:

- GEOSFP
- GEOSCF
- MERRA2
- GEOSIT

Each database contains the following columns:

```
['model', 'year', 'month', 'id_string', 'compression',
 'centroids', 'quantiles', 'quantile_list']
```

Each row is uniquely identified by the composite primary key:

```
(model, year, month, id_string)
```

### Payload Columns

The following columns are stored as **BLOB fields** (all `NOT NULL`) containing pickled Python objects:

| Column | Description |
|------|------|
| `centroids` | pickled `list[(mean, count)]` |
| `quantiles` | pickled `list[float]` |
| `quantile_list` | pickled `list[float]` |

### Table Names

```
geosfp
geoscf
merra2
geosit
```

### Example Database Path

```
/home/sadhika8/JupyterLinks/nobackup/quads_database/geosfp_monthly_aggregated_centroids_and_quantiles.db
```

For other models, replace `geosfp` with the corresponding table name.

---

# Optional Information

## merge_digests_and_write_pickle.py

This script merges daily per-`id_key` TDigest summaries into a single monthly summary for a given

```
(model, year, month)
```

This applies only to models with **daily subdirectories**.

### Expected Directory Layout

```
BASE_OUT_DIR/<model>/<YYYY>/<MM>/
```

Daily files are named

```
YYYY-MM-DD.pkl
```

Each file contains a pickled list of dictionaries:

```
{
"id_key": <str>,
"centroids": <Nx2 (mean, weight/count)>
}
```

### Processing Steps

For each day and `id_key`:

1. Reconstruct a TDigest

```
TDigest.of_centroids(...)
```

2. Combine into a monthly digest

```
TDigest.combine(...)
```

Missing days are skipped with a warning.

### Output File

```
monthly_merged_digest_<model>_<YYYY-MM>.pkl
```

Stored in the same month directory.

### Output Contents

A pickled list of dictionaries containing:

- merged digest centroids

```
td.get_centroids()
```

- quantile data

```
(quantile_values, quantile_levels)
```

returned by

```
get_quantiles_from_tdigest(td)
```

---

# Tdfence Computation Logic

Some data chunks are filled entirely with `NaN` values. These chunks are filtered out, resulting in a chunk size of zero. In this case, TDigest computation produces a null output.

Such entries are filtered out during database creation. When users query the database for those chunks, **no corresponding record will be found**.
