from connection import get_mongo_connection
import json
from bson import ObjectId # Import ObjectId if needed for comparison, though IDs are strings in graph
import copy # Needed for deep copying in helper
from functools import reduce # For nested dictionary access
import operator # For nested dictionary access

class QueryEngine:
    def __init__(self):
        self.db = get_mongo_connection()

    def _stringify_objectids_in_doc(self, doc):
        """Recursively convert ObjectId instances to strings within a document."""
        if isinstance(doc, list):
            return [self._stringify_objectids_in_doc(item) for item in doc]
        elif isinstance(doc, dict):
            # Use copy to avoid modifying the original dictionary if it's reused
            cleaned_doc = copy.copy(doc)
            for key, value in doc.items():
                if isinstance(value, ObjectId):
                    cleaned_doc[key] = str(value)
                elif isinstance(value, (dict, list)):
                    cleaned_doc[key] = self._stringify_objectids_in_doc(value)
            return cleaned_doc
        return doc # Return non-dict/list values as is

    def query_within_graph(self, query_json):
        """
        Execute a query within a single collection, applying filter, projection (field selection),
        and cleaning ObjectIds.

        :param query_json: JSON object containing collection, filter, and optional select list
        :return: Query results with ObjectIds as strings and selected fields
        """
        try:
            if "collection" not in query_json:
                raise KeyError("The query JSON must contain a 'collection' key.")

            collection_name = query_json["collection"]
            query_filter = query_json.get("filter", {})
            select_fields = query_json.get("select", None) # Get the list of fields to select
            collection = self.db[collection_name]

            # Construct MongoDB projection from select_fields list
            projection = None
            if isinstance(select_fields, list) and select_fields:
                projection = {field: 1 for field in select_fields}
                # Exclude _id by default unless explicitly included in select_fields
                if "_id" not in select_fields:
                    projection["_id"] = 0
            # Note: If select_fields is empty or not a list, projection remains None (fetch all fields)

            # Execute find with filter and projection
            results_cursor = collection.find(query_filter, projection) # Pass constructed projection

            # Convert cursor to list and clean ObjectIds
            results_list = list(results_cursor)
            # Cleaning is still needed if _id was selected or if other fields contain ObjectIds
            cleaned_results = [self._stringify_objectids_in_doc(doc) for doc in results_list]

            return cleaned_results
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

    def _get_nested_value(self, data_dict, key_string, default=None):
        """Safely retrieves a value from a nested dictionary using dot notation."""
        keys = key_string.split('.')
        try:
            # Use reduce to iteratively get items from the dictionary
            return reduce(operator.getitem, keys, data_dict)
        except (KeyError, TypeError, IndexError):
            # Handle cases where keys don't exist or data is not subscriptable
            return default

    def _apply_select_to_result(self, result_dict, select_map):
        """
        Filters a result dictionary based on a select map, handling nested fields.

        :param result_dict: The dictionary representing a query result (e.g., {'Students': {...}, 'Hackathons': {...}})
        :param select_map: A dictionary mapping entity names to lists of fields to select (e.g., {'Students': ['FirstName'], 'Hackathons': ['eventName', 'project.title']})
        :return: A new dictionary with only the selected fields for each entity.
        """
        if not select_map or not isinstance(select_map, dict):
            return result_dict # Return original if no select map

        filtered_result = {}
        for entity_key, entity_data in result_dict.items():
            if entity_key in select_map:
                fields_to_select = select_map[entity_key]
                if isinstance(fields_to_select, list) and isinstance(entity_data, dict): # Ensure entity_data is a dict
                    selected_data = {}
                    for field_key in fields_to_select:
                        # Use _get_nested_value to retrieve potentially nested data
                        value = self._get_nested_value(entity_data, field_key)
                        # Store the value if found, using the original key (dot notation included)
                        # This preserves the structure requested in the select clause
                        if value is not None: # Or handle default values differently if needed
                             selected_data[field_key] = value

                    # Only include the entity if selected fields were found or the list was empty (select all)
                    if selected_data or not fields_to_select:
                         filtered_result[entity_key] = selected_data
                elif not fields_to_select: # Handle empty select list (select all for this entity)
                     filtered_result[entity_key] = entity_data
            else:
                 # If entity is not in select_map, include all its data (as it passed projection)
                 filtered_result[entity_key] = entity_data
        return filtered_result

    def query_across_graphs(self, query_json):
        """
        Execute a dynamic cross-document query, applying filters, entity projection,
        and field selection (including nested fields).

        :param query_json: JSON object containing start_entity, filter, projection, and optional select
        :return: List of combined results with selected fields and ObjectIds as strings
        """
        try:
            graph_collection = self.db["Graph"]
            graph = graph_collection.find_one({})
            if not graph:
                print("No graph found in the 'Graph' collection.")
                return []

            query_filter = query_json.get("filter", {})
            entity_projection = query_json.get("projection", {}) # Projection for entities
            field_select_map = query_json.get("select", {}) # Selection for fields within entities
            start_entity = query_json.get("start_entity")

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
                # Apply cleaning here in case node data contains ObjectIds
                combined_data = {start_node['entity']: self._stringify_objectids_in_doc(start_node['data'])}

                # Follow outgoing edges from the start node
                related_edges = edges_source_map.get(start_node['id'], [])
                for edge in related_edges:
                    target_node = nodes_dict.get(edge['target'])
                    if target_node:
                        # Add related node's data, keyed by its entity type
                        relation_key = target_node['entity']
                        # Apply cleaning here as well
                        cleaned_target_data = self._stringify_objectids_in_doc(target_node['data'])
                        # If multiple relations to same entity type, this overwrites.
                        # Consider storing as a list if needed.
                        combined_data[relation_key] = cleaned_target_data

                # 3. Apply entity projection
                projected_entities_result = {}
                if not entity_projection:
                    projected_entities_result = combined_data # Include all related entities found
                else:
                    for key, include in entity_projection.items():
                        if include == 1 and key in combined_data:
                            projected_entities_result[key] = combined_data[key]

                # 4. Apply field selection (select) to the projected entities
                # Pass the result after entity projection and the field select map
                final_selected_result = self._apply_select_to_result(projected_entities_result, field_select_map)

                # Only add if the final result is not empty
                if final_selected_result:
                     results.append(final_selected_result)

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

            all_results = [] # Collect all results
            for query in queries:
                print(f"Executing Query: {query.get('description', 'No description')}")
                try:
                    # Pass the entire query object to execute_query
                    results = self.execute_query(query)
                    if results:
                        for result in results:
                            # Use json.dumps for consistent and readable output, handling potential complex types
                            print(json.dumps(result, indent=2, default=str))
                            all_results.append(result)
                    else:
                        print("No results found.")
                except KeyError as e:
                    # More specific error for missing keys if needed
                    print(f"Error executing query - Missing key: {e}")
                except Exception as e:
                    # Catching the re-raised exception from execute_query
                    print(f"Unexpected error during query execution: {e}")
                print("-" * 50)
            # Optionally return all results if needed elsewhere
            # return all_results
        except FileNotFoundError:
             print(f"Error: Queries file not found at {queries_file_path}")
        except json.JSONDecodeError:
             print(f"Error: Could not decode JSON from {queries_file_path}")
        except Exception as e:
            print(f"Error loading or processing queries file: {e}")
            # raise # Optional: re-raise for full traceback
