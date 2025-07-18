# Index Recommendation Experiments

This directory contains the code and resources for running experiments related to index recommendation. 
It uses [`yanex`](https://github.com/rueckstiess/yanex) to manage the experiments and provides a structured 
way to evaluate different workloads.


## Directory Structure

All experiments are organized in the `experiments` directory, which contains the following files and subdirectories:

- `evaluate.py`: The main script to run the experiments. It orchestrates the evaluation of different index recommendations 
  based on the defined workloads. Run with `yanex run evaluate.py` (to track experiments) or `python evaluate.py` (to run without tracking).
- `workloads/`: Contains the workload definitions for the experiments. Each workload is defined in a separate class derivned from `BaseWorkload`.
- `utils.py`: Contains utility functions used in `evaluate.py` for setting up experiments and handling results.
- `README.md`: This file, providing an overview of the experiments and how to run them
- `config.yaml`: Configuration file for the experiments, defining parameters for MongoDB, the workload, mindexer configuration and evaluation settings.


## Setting Up the Environment

### Dependencies

First make sure all dependencies (e.g. `yanex`) are installed in your Python virtual environment by running:

```bash
pip install -r requirements.txt
```

You should now be able to run `yanex` from the CLI (e.g. `yanex list`, which would show an empty list of experiments on the first run).


### Datasets and Workloads

(TODO, currently assumes Ember dataset is already set up in local MongoDB instance).

We need to set up the datasets and workloads for the experiments. The `workloads.py` file contains classes that define the workloads to be used in the experiments.


## Running the Experiments

To run the experiments, use the following command:

```bash
yanex run evaluate.py
```

This will execute the `evaluate.py` script, using the
parameters in the `config.yaml` file, The results will be stored in the `.yanex/experiments/<id>` directory.



