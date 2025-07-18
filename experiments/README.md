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


#### Ember Dataset

The `ember` and `test` workloads are based on the Ember dataset. It can be downloaded from our OneDrive folder, together with a `README.md` file that contains instructions on how to import the data into MongoDB. Use `ember2018` as the database name and `ember_train` / `ember_test` as the collection names.


#### Linkbench Dataset

TODO

#### Custom Datasets and Workloads

You can also define your own datasets and workloads. To do so, create a new class in `workloads.py` that inherits from `BaseWorkload` 
with the correct database and collection names.

Implement the `execute_workload()` method, which can either execute queries directly using the PyMongo driver, or call 
a bash script that runs the workload using [`yanex.execute_bash_script()`](https://github.com/rueckstiess/yanex/blob/main/docs/python-api.md#yanexexecute_bash_scriptcommand-timeoutnone-raise_on_errorfalse-stream_outputtrue-working_dirnone). 

Optional `setup_hook()` and `cleanup_hook()` methods can be implemented to set up the database and clean up after the workload execution.

Give your workload a unique name and add it to `get_workload()`. 


## Running Experiments

To run the experiments, use the following command:

```bash
yanex run evaluate.py
```

This will execute the `evaluate.py` script, using the parameters in the `config.yaml` file. 
The results will be stored in the `.yanex/experiments/<id>` directory, see stdout output after the
experiment has finished.

In the experiment directory, you will find a number of files. See [Experiment Directory Structure](https://github.com/rueckstiess/yanex/blob/main/docs/experiment-structure.md) in the Yanex documentation for details on the files and their contents.

Individual parameters can be overridden by passing them as command line arguments, e.g.:

```bash
yanex run evaluate.py --param workload=weather --param "mindexer.sample_ratio=0.05"
```


## Viewing Results

To view the results of the experiments, you can use the `yanex` CLI commands.

To see a list of all experiments, run:

```bash
yanex list
```


To see details for a specific experiment, use:

```bash
yanex show <experiment_id>
```

To compare parameters and metrics of different experiments in an interactive table, you can use:
```bash
yanex compare [FILTERS]
```

For all available commands, check the [Yanex documentation](https://github.com/rueckstiess/yanex/blob/main/docs/README.md).

