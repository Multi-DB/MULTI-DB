from connection import get_mongo_connection
import json
from bson import ObjectId # Import ObjectId if needed for comparison, though IDs are strings in graph

class QueryEngine:
    def __init__(self):
        self.db = get_mongo_connection()

    def query_within_graph(self, query_json):
        """
        Execute a query within a single collection.

        :param query_json: JSON object containing the collection name and query filter
        :return: Query results
        """
        try:
            # Validate that the 'collection' key exists
            if "collection" not in query_json:
                raise KeyError("The query JSON must contain a 'collection' key.")

            collection_name = query_json["collection"]
            query_filter = query_json.get("filter", {})  # Default to an empty filter if not provided
            collection = self.db[collection_name]
            results = collection.find(query_filter)
            return list(results)
        except KeyError as e:
            print(f"Error querying within graph: {e}")
            raise
        except Exception as e:
            print(f"Unexpected error querying within graph: {e}")
            raise

    def _evaluate_filter(self, data, key, value):
        """Helper function to evaluate filter conditions including operators."""
        node_val = data.get(key)

        if isinstance(value, dict):
            # Handle operators like $gt, $lt, $gte, $lte, $ne, $exists
            op, op_val = list(value.items())[0]
            if node_val is None and op != "$exists": # Allow $exists: false check
                 return False # Cannot compare if value is None unless checking existence

            if op == "$gt": return node_val > op_val
            if op == "$lt": return node_val < op_val
            if op == "$gte": return node_val >= op_val
            if op == "$lte": return node_val <= op_val
            if op == "$ne": return node_val != op_val
            if op == "$exists": return (op_val and node_val is not None) or (not op_val and node_val is None)
            # Add more operators here if needed
            return False # Unsupported operator
        else:
            # Direct equality check
            return node_val == value

    def query_across_graphs(self, query_json):
        """
        Execute a dynamic cross-document query using the Graph collection
        by performing a 1-level traversal from matching start nodes.

        :param query_json: JSON object containing the start_entity, filter, and projection
        :return: List of combined results
        """
        try:
            graph_collection = self.db["Graph"]
            graph = graph_collection.find_one({})
            if not graph:
                print("No graph found in the 'Graph' collection.")
                return []

            query_filter = query_json.get("filter", {})
            projection = query_json.get("projection", {})
            start_entity = query_json.get("start_entity") # Entity type to start search from

            results = []
            nodes_dict = {node['id']: node for node in graph.get("nodes", [])}
            edges_source_map = {}
            for edge in graph.get("edges", []):
                source_id = edge['source']
                if source_id not in edges_source_map:
                    edges_source_map[source_id] = []
                edges_source_map[source_id].append(edge)

            # 1. Find starting nodes matching the filter and start_entity
            start_nodes = []
            for node_id, node in nodes_dict.items():
                if start_entity and node['entity'] != start_entity:
                    continue # Skip if node is not the specified start entity type

                # Check if node data matches the filter
                match = True
                for key, value in query_filter.items():
                    if not self._evaluate_filter(node['data'], key, value):
                        match = False
                        break
                if match:
                    start_nodes.append(node)

            # 2. For each start node, perform 1-level traversal and combine data
            for start_node in start_nodes:
                # Initialize combined data with the starting node's data, keyed by entity type
                combined_data = {start_node['entity']: start_node['data']}

                # Follow outgoing edges from the start node
                related_edges = edges_source_map.get(start_node['id'], [])
                for edge in related_edges:
                    target_node = nodes_dict.get(edge['target'])
                    if target_node:
                        # Add related node's data, keyed by its entity type
                        relation_key = target_node['entity']
                        # If multiple relations to same entity type, this overwrites.
                        # Consider storing as a list if needed.
                        combined_data[relation_key] = target_node['data']

                # 3. Apply projection to the combined data
                projected_result = {}
                if not projection:
                    projected_result = combined_data # Return all combined data if no projection
                else:
                    # Apply projection based on top-level keys (entity names)
                    for key, include in projection.items():
                        if include == 1 and key in combined_data:
                            projected_result[key] = combined_data[key]
                        # Note: This simple projection doesn't handle nested fields like "Products.price"
                        # or excluding fields (_id: 0). Needs enhancement for full functionality.

                # Only add if the projected result is not empty (or based on projection logic)
                if projected_result:
                     results.append(projected_result)

            return results

        except Exception as e:
            print(f"Error querying across graphs: {e}")
            raise

    def execute_query(self, full_query_object):
        """
        Dynamically execute a query based on the type (within or across graphs).

        :param full_query_object: The complete query object including 'type' and 'query' keys.
        :return: Query results
        """
        try:
            # Get type from the main object
            query_type = full_query_object.get("type", "within")
            # Get the inner query details
            query_details = full_query_object.get("query", {})

            if query_type == "within":
                # Pass the inner details to query_within_graph
                return self.query_within_graph(query_details)
            elif query_type == "across":
                # Pass the inner details to query_across_graphs
                return self.query_across_graphs(query_details)
            else:
                raise ValueError(f"Unsupported query type: {query_type}")
        except Exception as e:
            # Add more context to the error message
            print(f"Error executing query ({full_query_object.get('description', 'No description')}): {e}")
            raise

    def execute_queries_from_file(self, queries_file_path):
        """
        Load and execute queries from a JSON file.

        :param queries_file_path: Path to the JSON file containing queries
        """
        try:
            with open(queries_file_path, 'r') as file:
                queries = json.load(file)

            for query in queries:
                print(f"Executing Query: {query.get('description', 'No description')}")
                try:
                    # Pass the entire query object to execute_query
                    results = self.execute_query(query)
                    if results:
                        for result in results:
                            print(result)
                    else:
                        print("No results found.")
                except KeyError as e:
                    # More specific error for missing keys if needed
                    print(f"Error executing query - Missing key: {e}")
                except Exception as e:
                    # Catching the re-raised exception from execute_query
                    print(f"Unexpected error during query execution: {e}")
                print("-" * 50)
        except FileNotFoundError:
             print(f"Error: Queries file not found at {queries_file_path}")
        except json.JSONDecodeError:
             print(f"Error: Could not decode JSON from {queries_file_path}")
        except Exception as e:
            print(f"Error loading or processing queries file: {e}")
            # raise # Optional: re-raise for full traceback
