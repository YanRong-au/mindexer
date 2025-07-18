from abc import ABC, abstractmethod
from pymongo import MongoClient


class BaseWorkload(ABC):
    """Base class for all workloads."""

    def __init__(self):
        self.db_name = None
        self.collection_name = None

    @abstractmethod
    def setup_hook(self, client: MongoClient):
        """Called before workload execution. Override for custom setup."""
        pass

    @abstractmethod
    def execute_workload(self, client: MongoClient):
        """Execute the workload queries. Must be implemented by subclasses."""
        pass

    @abstractmethod
    def cleanup_hook(self, client: MongoClient):
        """Called after workload execution. Override for custom cleanup."""
        pass


class EmberWorkload(BaseWorkload):
    """Workload for Ember dataset."""

    def __init__(self):
        super().__init__()
        self.db_name = "ember2018"
        self.collection_name = "ember_train"

    def setup_hook(self, client: MongoClient):
        pass

    def execute_workload(self, client: MongoClient):
        # TODO: Implement ember-specific workload queries
        pass

    def cleanup_hook(self, client: MongoClient):
        pass


class LinkbenchWorkload(BaseWorkload):
    """Workload for Linkbench dataset."""

    def __init__(self):
        super().__init__()
        self.db_name = "linkbench"
        self.collection_name = "linktable"

    def setup_hook(self, client: MongoClient):
        pass

    def execute_workload(self, client: MongoClient):
        # TODO: Implement linkbench-specific workload queries
        # To run a shell script, you can use `yanex.execute_bash_script("...")``
        pass

    def cleanup_hook(self, client: MongoClient):
        pass


class TestWorkload(BaseWorkload):
    """Test workload with example queries."""

    def __init__(self):
        super().__init__()
        self.db_name = "ember2018"
        self.collection_name = "ember_test"

    def setup_hook(self, client: MongoClient):
        pass

    def execute_workload(self, client: MongoClient):
        """Run a test workload that executes specific queries on the collection."""
        db = client[self.db_name]
        collection = db[self.collection_name]

        # Example query 1: Find documents with entropy greater than 7.99 and sort by entropy in descending order
        query = {"section.sections.entropy": {"$gt": 7.99}}
        projection = None
        sort = {"section.sections.entropy": -1}

        # Execute the query
        results = collection.find(query, projection).sort(sort).comment("Q1").to_list()
        print(f"Query Q1: find={query}, projection={projection}, sort={sort}, returned {len(results)} results.")

        # Example query 2: Find documents with subsystem "WINDOWS_GUI" and entropy greater than 6.0
        query = {"appeared": {"$gte": "2018-10"}, "general.size": {"$gt": 2000000}, "general.has_signature": 1}
        projection = {"appeared": 1, "general.size": 1, "general.has_signature": 1, "header.coff.timestamp": 1}

        # Execute the query
        results = collection.find(query, projection).comment("Q2").to_list()
        print(f"Query Q2: find={query}, projection={projection}, returned {len(results)} results.")

    def cleanup_hook(self, client: MongoClient):
        pass


def get_workload(workload_name: str) -> BaseWorkload:
    """Factory function to get workload instance by name."""
    workloads = {"ember": EmberWorkload, "linkbench": LinkbenchWorkload, "test": TestWorkload}

    if workload_name not in workloads:
        raise ValueError(f"Unknown workload: {workload_name}. Available workloads: {list(workloads.keys())}")

    return workloads[workload_name]()
