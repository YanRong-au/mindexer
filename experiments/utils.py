import json
import ast
import time
from workloads import BaseWorkload
from pymongo import MongoClient
from yanex.utils.exceptions import ValidationError
import yanex


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


def eval_workload(client: MongoClient, workload: BaseWorkload) -> float:
    """Execute workload with given indexes and return execution time."""

    # Execute the workload and return time taken
    print(f"Executing workload {workload.db_name}.{workload.collection_name}")

    start_time = time.perf_counter()
    workload.execute_workload(client)
    end_time = time.perf_counter()

    execution_time = end_time - start_time
    print(f"Workload execution time: {execution_time:.3f} seconds")
    return execution_time
