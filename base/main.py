import json
from connection import get_mongo_connection
# Import QueryEngine
from query_engine import QueryEngine

def display_graph(collection_name, source_name):
    """
    Fetch and display the metadata graph from the specified MongoDB collection for a specific source.
    Traverse the graph for cross-document relationships.

    :param collection_name: Name of the MongoDB collection to fetch the graph from
    :param source_name: The source name (file path) to filter the graph
    """
    try:
        db = get_mongo_connection()
        collection = db[collection_name]
        graph = collection.find_one({"metadata.source_name": source_name})

        if graph:
            print("Metadata Graph:")
            print("Metadata:", graph.get("metadata", {}))
            print("Nodes:")
            for node in graph.get("nodes", []):
                print(f"  {node}")
            print("Edges:")
            for edge in graph.get("edges", []):
                print(f"  {edge}")

            # Traverse the graph for cross-document relationships
            print("\nTraversing Graph for Cross-Document Relationships:")
            visited = set()
            queue = [graph]
            while queue:
                current = queue.pop(0)
                current_id = current.get("_id")
                if current_id in visited:
                    continue
                visited.add(current_id)

                # Display current node
                print(f"Node: {current.get('metadata', {}).get('source_name', 'Unknown')}")

                # Fetch related nodes via edges
                for edge in current.get("edges", []):
                    related_collection = edge.get("related_collection")
                    related_id = edge.get("related_id")
                    if related_collection and related_id:
                        # Avoid redundant graph construction by checking visited nodes
                        if related_id not in visited:
                            related_doc = db[related_collection].find_one({"_id": related_id})
                            if related_doc:
                                print(f"  Related Node in {related_collection}: {related_doc.get('metadata', {}).get('source_name', 'Unknown')}")
                                queue.append(related_doc)
            print("-" * 50)
        else:
            print(f"No graph found for source: {source_name}")
    except Exception as e:
        print(f"Error displaying graph: {e}")
        raise

def process_schema_and_display_graph(schema_file_path):
    """
    Process the schema file and display the graph for the specified data.

    :param schema_file_path: Path to the schema file
    """
    try:
        with open(schema_file_path, 'r') as schema_file:
            schema = json.load(schema_file)

        db = get_mongo_connection()

        for entity in schema:
            entity_label = entity["entity_label"]
            file_path = entity["file_path"]
            print(f"\nDisplaying Metadata Graph for {entity_label}:")
            if file_path.endswith(".csv"):
                display_graph("relational", file_path)
            elif file_path.endswith(".xml"):
                display_graph("xml", file_path)
            elif file_path.endswith(".json"):
                display_graph("json", file_path)
            else:
                print(f"Unsupported file type for {file_path}")
    except Exception as e:
        print(f"Error processing schema file: {e}")
        raise

def execute_cross_document_queries(queries_file_path):
    """
    Execute queries defined in the queries file using QueryEngine.

    :param queries_file_path: Path to the JSON file containing a list of queries
    """
    try:
        # Instantiate QueryEngine
        query_engine = QueryEngine()
        print("Executing queries using QueryEngine...")
        # Use the method from QueryEngine to execute the list of queries
        query_engine.execute_queries_from_file(queries_file_path)

    except Exception as e:
        print(f"Error executing queries from file: {e}")
        # Optionally re-raise or handle differently
        # raise

if __name__ == "__main__":
    schema_file_path = "/Users/dhruvasharma/Documents/SEM2/DM/MULTI-DB/base/market_data/schema_file.json"
    queries_file_path = "/Users/dhruvasharma/Documents/SEM2/DM/MULTI-DB/base/market_data/queries.json"
    try:
        # The graph building and data loading should ideally happen before querying.
        # Consider using run_queries.py as the main entry point or ensure
        # graph_builder.py has been run successfully beforehand.
        print("Attempting to execute queries...")
        # Note: This script doesn't build the graph or load data itself.
        # Ensure graph_builder.py has been run.

        # Execute queries from the file
        execute_cross_document_queries(queries_file_path)

    except FileNotFoundError:
        print(f"Error: Schema file not found at {schema_file_path} or queries file not found at {queries_file_path}")
    except Exception as e:
        print(f"An error occurred: {e}")
        # raise # Uncomment to see the full traceback if needed
