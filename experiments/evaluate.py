import yanex
from yanex.utils.exceptions import ValidationError, ExperimentError
from pymongo import MongoClient
import numpy as np
from workloads import get_workload

from utils import (
    enable_profiling,
    disable_profiling,
    run_mindexer,
    create_indexes,
    drop_indexes,
    eval_workload,
)


def main():
    # get config parameters
    uri = yanex.get_param("uri", "mongodb://localhost:27017")
    workload_name = yanex.get_param("workload", "test")
    slowms = yanex.get_param("slowms", 100)
    num_runs = yanex.get_param("eval.num_runs", 1)
    discard_best_worst = yanex.get_param("eval.discard_best_worst", False)
    workload_params = yanex.get_param(f"workload_params.{workload_name}", "{}")

    # validate parameters
    if num_runs <= 0:
        raise ValidationError("num_runs must be positive")
    if not workload_name:
        raise ValidationError("Workload configuration is missing.")
    try:
        workload = get_workload(workload_name, workload_params)
    except ValueError as e:
        raise ValidationError(str(e))

    # --- Workload Profiling Phase ---

    # connect to MongoDB and prepare it for workload execution
    client = MongoClient(uri)
    enable_profiling(client, workload, slowms)

    # execute workload
    workload.setup_hook(client)
    workload.execute_workload(client)
    workload.cleanup_hook(client)

    # disable profiling after workload execution
    disable_profiling(client, workload)

    # --- Index Recommendation Phase ---

    # run mindexer evaluation
    recommended_indexes = run_mindexer(uri, workload)

    # --- Evaluation Phase ---

    # drop existing indexes before evaluation
    print("\n=== Evaluating workload without indexes ===\n")
    drop_indexes(client, workload)

    # perform warmup run
    print("Performing warmup run...")
    eval_workload(client, workload)

    # evaluate workload without indexes
    exec_times_no_indexes = []
    for n in range(num_runs):
        print(f"\nRun {n + 1}/{num_runs} without indexes")
        try:
            exec_time = eval_workload(client, workload)
            yanex.log_metrics({"exec_time_no_indexes": exec_time}, step=n)
            exec_times_no_indexes.append(exec_time)
        except Exception as e:
            print(f"Run {n + 1} failed: {e}")
            raise

    # create recommended indexes
    print("\n=== Evaluating workload with recommended indexes ===\n")
    if not recommended_indexes:
        raise ExperimentError("No recommended indexes found from mindexer.")
    create_indexes(client, workload, recommended_indexes)

    # perform warmup run
    print("Performing warmup run...")
    eval_workload(client, workload)

    # evaluate workload with recommended indexes
    exec_times_with_indexes = []
    for n in range(num_runs):
        print(f"\nRun {n + 1}/{num_runs} with indexes")
        try:
            exec_time = eval_workload(client, workload)
            yanex.log_metrics({"exec_time_with_indexes": exec_time}, step=n)
            exec_times_with_indexes.append(exec_time)
        except Exception as e:
            print(f"Run {n + 1} failed: {e}")
            raise

    # calculate statistics
    if discard_best_worst:
        if num_runs < 3:
            print("Not enough runs to discard best/worst. Using all runs.")
        else:
            exec_times_no_indexes.sort()
            exec_times_with_indexes.sort()
            exec_times_no_indexes = exec_times_no_indexes[1:-1]
            exec_times_with_indexes = exec_times_with_indexes[1:-1]

    mean_no_indexes = np.mean(exec_times_no_indexes)
    stddev_no_indexes = np.std(exec_times_no_indexes)
    mean_with_indexes = np.mean(exec_times_with_indexes)
    stddev_with_indexes = np.std(exec_times_with_indexes)

    # log final results
    yanex.log_metrics(
        {
            "mean_exec_time_no_indexes": mean_no_indexes,
            "stddev_exec_time_no_indexes": stddev_no_indexes,
            "mean_exec_time_with_indexes": mean_with_indexes,
            "stddev_exec_time_with_indexes": stddev_with_indexes,
            "num_recommended_indexes": len(recommended_indexes),
        }
    )


if __name__ == "__main__":
    main()
