import yanex
from yanex.utils.exceptions import ValidationError
from pymongo import MongoClient
import json
import ast


def enable_profiling(client: MongoClient, db_name: str, collection_name: str, slowms: int):
    """Prepare MongoDB for workload execution: Delete system.profile collection and ensure collection exists."""
    db = client[db_name]

    # Disable profiling
    db.command({"profile": 0})

    # Ensure the database and collection exist
    if collection_name not in db.list_collection_names():
        raise ValidationError(f"Collection '{collection_name}' does not exist in database '{db_name}'.")

    # Drop system.profile collection if it exists
    if "system.profile" in db.list_collection_names():
        db.drop_collection("system.profile")

    # Set profiling level
    namespace = f"{db_name}.{collection_name}"
    db.command({"profile": 1, "slowms": slowms, "filter": {"ns": namespace, "op": "query"}})

    print(f"MongoDB profiling enabled for {namespace} with slowms={slowms}.")


def validate_workload(workload: str):
    """Validate the workload configuration."""
    workload = yanex.get_param("workload")
    if not workload:
        raise ValidationError("Workload configuration is missing.")

    if workload not in ["linkbench", "ember", "test"]:
        raise ValidationError(f"Unsupported workload: {workload}. Supported workloads are: linkbench, ember, test.")


def run_workload_linkbench(client: MongoClient, db_name: str, collection_name: str):
    pass


def run_workload_ember(client: MongoClient, db_name: str, collection_name: str):
    pass


def run_workload_test(client: MongoClient, db_name: str, collection_name: str):
    """Run a test workload that executes a specific query on the collection."""
    db = client[db_name]
    collection = db[collection_name]

    # Example query 1: Find documents with entropy greater than 7.99 and sort by entropy in descending order
    query = {"section.sections.entropy": {"$gt": 7.99}}
    projection = None
    sort = {"section.sections.entropy": -1}

    # Execute the query
    print(f"Executing query Q1: find={query}, projection={projection}, sort={sort}")
    results = collection.find(query, projection).sort(sort).comment("Q1").to_list()
    print(f"Query Q1 returned {len(results)} results.")

    # Example query 2: Find documents with subsystem "WINDOWS_GUI" and entropy greater than 6.0
    query = {"appeared": {"$gte": "2018-10"}, "general.size": {"$gt": 2000000}, "general.has_signature": 1}
    projection = {"appeared": 1, "general.size": 1, "general.has_signature": 1, "header.coff.timestamp": 1}

    # Execute the query
    print(f"Executing query Q2: find={query}, projection={projection}")
    results = collection.find(query, projection).comment("Q2").to_list()
    print(f"Query Q2 returned {len(results)} results.")


def disable_profiling(client: MongoClient, db_name: str, collection_name: str):
    """Disable profiling for the specified collection."""
    db = client[db_name]
    namespace = f"{db_name}.{collection_name}"
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


def run_mindexer():
    """Run mindexer evaluation"""

    uri = yanex.get_param("uri", "mongodb://localhost:27017")
    db_name = yanex.get_param("db", "ember2018")
    collection_name = yanex.get_param("collection", "ember_train")
    sample_ratio = yanex.get_param("mindexer.sample_ratio", 0.01)
    max_indexes = yanex.get_param("mindexer.max_indexes", 0)
    verbose = yanex.get_param("mindexer.verbose", False)

    bash_command = (
        f"mindexer --uri {uri} -d {db_name} -c {collection_name} "
        f"--sample-ratio {sample_ratio} --max-indexes {max_indexes} "
    )
    if verbose:
        bash_command += "-v"

    results = yanex.execute_bash_script(bash_command, raise_on_error=True, artifact_prefix="mindexer")
    recommended_indexes = parse_mindexer_output(results["stdout"])

    # save recommended indexes to a JSON file
    yanex.log_text(json.dumps(recommended_indexes, indent=2), "mindexer_recommended_indexes.json")


def main():
    # get params
    uri = yanex.get_param("uri", "mongodb://localhost:27017")
    db_name = yanex.get_param("db", "ember2018")
    collection_name = yanex.get_param("collection", "ember_train")
    workload = yanex.get_param("workload", "test")
    slowms = yanex.get_param("slowms", 100)

    # connect to MongoDB and prepare it for workload execution
    client = MongoClient(uri)
    enable_profiling(client, db_name, collection_name, slowms)

    # execute workload based on configuration
    validate_workload(workload)

    if workload == "linkbench":
        run_workload_linkbench(client, db_name, collection_name)
    elif workload == "ember":
        run_workload_ember(client, db_name, collection_name)
    elif workload == "test":
        run_workload_test(client, db_name, collection_name)

    # disable profiling after workload execution
    disable_profiling(client, db_name, collection_name)

    # run mindexer evaluation
    run_mindexer()


if __name__ == "__main__":
    main()
