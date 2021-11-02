# S-Query evaluation

This repo contains several Python scripts used for processing the results from the experiments. It also contains the raw result files obtained from the experiments for easy verifiability.

## Plotter usage

Install Python 3.9+

(Optional but recommended) create a virtual environment:

```
python -m venv .venv
```

Activate the virtual environment:

```
.venv\Scripts\activate
```

Install dependencies from `requirements.txt`:

```
pip install -r requirements.txt
```

Run plotter:

```
python src/plotter.py -c configs.sl_state_size
```

Or run multiple plotter configs:

```
python src/plot_experiments.py
```

## Result types

There are different types of raw result files:

- Snapshot latency, recognizable by the `.sl` file extension.
- Query latency, recognizable by the `.ql` file extension. These are the same for both direct object queries and SQL queries.
- Source-sink latency, recognizable by the `.hgrm` file extension.

## Directory structure

- ## benchmarks:
  - ### dh
    Contains files with raw benchmark data from the Delivery Hero use case as described in the S-Query paper.
    - #### full
      Contains the latencies for the full snapshots configuration.
      - ##### direct_query
        Latencies from the direct queries.
      - #### query
        Latencies from the SQL queries.
      - #### snapshot
        Latencies of the snapshot creation process.
    - #### incrmental
      Contains the latencies for both the incremental snapshots and the queries running on the incremental snapshots.
  - ### nexmark
    Contains the hgrm (histogram) files from the NEXMark tool for the different stream throughputs (in events/s). Also contains snapshot latencies. For more details on structure view [README.md](benchmarks/nexmark/README.md) in that folder.
- ## src
  Contains the actual python code for plotting the benchmarks
  - `hdr_parser.py`
    Parser for hgrm (histogram) files from NEXMark tool.
  - `plotter.py`
    The main plotter file, see usage instructions below
  - `plot_experiments.py`
    Helper scripts that plots all configs at once
  - ### configs
    This folder houses the different plot configurations and data
    - `plot_config.py`
      Example configuration file
    - `ut_live_phase.py`
      Config for user tracking job. Live and phase state enabled latencies
    - `ut_live_snapshot_phase.py`
      Config for user tracking job. Live, snapshot and phase state enabled latencies
    - ### data
      Folder used for housing the python latency lists
      - `incremental_query.py`
        Contains the query latencies from the raw data in `benchmarks/dh/incremental/query`
      - `incremental_sl.py`
        Contains the snapshot latencies of incremental snapshots from the raw data in `benchmark/dh/incremental/snapshot`
      - `query_time.py`
        Contains the query latencies from the raw data in `benchmarks/dh/sql_query`
      - `snapshot_latency.py`
        Contains the snapshot latenices (copied) from the raw data in `benchmarks/dh/full/snapshot`
- ## excel
  Contains the excel files for plotting the query throughput and scalability experiments. Data for throughput experiment comes from `benchmarks/dh/full/direct_query`.
  Raw data for scalability experiment comes from `benchmarks/nexmark`.
