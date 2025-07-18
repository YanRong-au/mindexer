import yanex
from yanex.utils.exceptions import ValidationError, ExperimentError
from pymongo import MongoClient
import json
import ast
import time
import numpy as np

from workloads import get_workload, BaseWorkload


def enable_profiling(client: MongoClient, workload: BaseWorkload, slowms: int):
    """Prepare MongoDB for workload execution: Delete system.profile collection and ensure collection exists."""
    db = client[workload.db_name]

    # Disable profiling
    db.command({"profile": 0})

    # Ensure the database and collection exist
    if workload.collection_name not in db.list_collection_names():
        raise ValidationError(
            f"Collection '{workload.collection_name}' does not exist in database '{workload.db_name}'."
        )

    # Drop system.profile collection if it exists
    if "system.profile" in db.list_collection_names():
        db.drop_collection("system.profile")

    # Set profiling level
    namespace = f"{workload.db_name}.{workload.collection_name}"
    db.command({"profile": 1, "slowms": slowms, "filter": {"ns": namespace, "op": "query"}})

    print(f"MongoDB profiling enabled for {namespace} with slowms={slowms}.")


def disable_profiling(client: MongoClient, workload: BaseWorkload):
    """Disable profiling for the specified collection and store profiling data."""

    db = client[workload.db_name]
    namespace = f"{workload.db_name}.{workload.collection_name}"
    db.command({"profile": 0, "filter": {"ns": namespace}})

    # Print number of documents in system.profile matching the namespace and op="query"
    profile_count = db.system.profile.count_documents({"ns": namespace, "op": "query"})
    print(f"Profiling disabled for {namespace}. Number of profile documents: {profile_count}.")

    # Get all system.profile documents matching the filter and store as JSON file
    profile_docs = db.system.profile.find({"ns": namespace, "op": "query"}).to_list()
    yanex.log_text(json.dumps(profile_docs, indent=2, default=str), "system_profile.json")


def parse_mindexer_output(output: str):
    """Parse the output of mindexer to extract recommended indexes."""

    output_lines = output.strip().split("\n")

    # Find the line that starts with ">> recommending"
    recommending_line_idx = None
    for i, line in enumerate(output_lines):
        if line.strip().startswith(">> recommending"):
            recommending_line_idx = i
            break

    if recommending_line_idx is None:
        print("No recommendation section found in mindexer output")
        recommended_indexes = []
    else:
        # Extract lines after the recommending line that contain index dictionaries
        recommended_indexes = []
        for line in output_lines[recommending_line_idx + 1 :]:
            line = line.strip()
            if not line:
                continue

            # Skip lines that don't look like index dictionaries
            if not (line.startswith("{") and line.endswith("}")):
                continue

            try:
                # Parse the dictionary string
                index_dict = ast.literal_eval(line)
                recommended_indexes.append(index_dict)
            except (ValueError, SyntaxError) as e:
                print(f"Failed to parse index line: {line}, error: {e}")
                continue

    return recommended_indexes


def run_mindexer(uri: str, workload: BaseWorkload) -> list[dict]:
    """Run mindexer evaluation and parse outputs for recommended indexes."""

    sample_ratio = yanex.get_param("mindexer.sample_ratio", 0.01)
    max_indexes = yanex.get_param("mindexer.max_indexes", 0)
    verbose = yanex.get_param("mindexer.verbose", False)

    bash_command = (
        f"mindexer --uri {uri} -d {workload.db_name} -c {workload.collection_name} "
        f"--sample-ratio {sample_ratio} --max-indexes {max_indexes} "
    )
    if verbose:
        bash_command += "-v"

    results = yanex.execute_bash_script(bash_command, raise_on_error=True, artifact_prefix="mindexer")
    recommended_indexes = parse_mindexer_output(results["stdout"])

    # save recommended indexes to a JSON file
    yanex.log_text(json.dumps(recommended_indexes, indent=2), "mindexer_recommended_indexes.json")

    return recommended_indexes


def drop_indexes(client: MongoClient, workload: BaseWorkload):
    """Drop all indexes for the specified workload collection."""
    db = client[workload.db_name]
    collection = db[workload.collection_name]

    # Drop all indexes except the default _id index
    collection.drop_indexes()
    print(f"Dropped all indexes for {workload.db_name}.{workload.collection_name}.")


def create_indexes(client: MongoClient, workload: BaseWorkload, indexes: list[dict]):
    """Create indexes for the specified workload collection."""
    db = client[workload.db_name]
    collection = db[workload.collection_name]

    # Create new indexes
    for index in indexes:
        try:
            if isinstance(index, dict):
                # Convert dict to list of (field, direction) tuples
                index_spec = [(field, direction) for field, direction in index.items()]
            else:
                index_spec = index
            print(f"Creating index: {index_spec}")
            collection.create_index(index_spec)
        except Exception as e:
            print(f"Failed to create index {index}: {e}")
            raise


def run_workload(client: MongoClient, workload: BaseWorkload) -> float:
    """Execute workload with given indexes and return execution time."""

    # Execute the workload and return time taken
    print(f"Executing workload {workload.db_name}.{workload.collection_name}")

    start_time = time.perf_counter()
    workload.execute_workload(client)
    end_time = time.perf_counter()

    execution_time = end_time - start_time
    print(f"Workload execution time: {execution_time:.3f} seconds")
    return execution_time


def main():
    # get config parameters
    uri = yanex.get_param("uri", "mongodb://localhost:27017")
    workload_name = yanex.get_param("workload", "test")
    slowms = yanex.get_param("slowms", 100)
    num_runs = yanex.get_param("eval.num_runs", 1)
    discard_best_worst = yanex.get_param("eval.discard_best_worst", False)

    # validate parameters
    if num_runs <= 0:
        raise ValidationError("num_runs must be positive")
    if not workload_name:
        raise ValidationError("Workload configuration is missing.")
    try:
        workload = get_workload(workload_name)
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
    run_workload(client, workload)

    # evaluate workload without indexes
    exec_times_no_indexes = []
    for n in range(num_runs):
        print(f"\nRun {n + 1}/{num_runs} without indexes")
        try:
            exec_time = run_workload(client, workload)
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
    run_workload(client, workload)

    # evaluate workload with recommended indexes
    exec_times_with_indexes = []
    for n in range(num_runs):
        print(f"\nRun {n + 1}/{num_runs} with indexes")
        try:
            exec_time = run_workload(client, workload)
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

    # use numpy to calculate mean and stddev
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
