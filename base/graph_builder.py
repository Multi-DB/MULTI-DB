import json
import csv
import os
import xml.etree.ElementTree as ET

from bson import ObjectId

from connection import get_mongo_connection

class GraphBuilder:
    def __init__(self, schema_file_path):
        self.schema_file_path = schema_file_path
        self.db = get_mongo_connection()

    def load_data_from_schema(self):
        """
        Load data into MongoDB collections based on the schema file.
        """
        try:
            with open(self.schema_file_path, 'r') as schema_file:
                schema = json.load(schema_file)

            for entity in schema:
                collection_name = entity["entity_label"]
                file_path = entity["file_path"]
                file_extension = os.path.splitext(file_path)[1].lower()

                print(f"Loading data for {collection_name} from {file_path}...")

                # Clear the collection before inserting new data
                self.db[collection_name].delete_many({})

                if file_extension == ".csv":
                    self._load_csv(file_path, collection_name)
                elif file_extension == ".json":
                    self._load_json(file_path, collection_name)
                elif file_extension == ".xml":
                    self._load_xml(file_path, collection_name, entity["fields"])
                else:
                    print(f"Unsupported file type: {file_extension}")
        except Exception as e:
            print(f"Error loading data from schema: {e}")
            raise

    def build_graph(self):
        """
        Build a single dynamic graph representation by linking entities across collections based on the schema.
        """
        try:
            print("Building dynamic graph representation...")

            # Load schema to determine relationships
            with open(self.schema_file_path, 'r') as schema_file:
                schema = json.load(schema_file)

            graph = {"nodes": [], "edges": []}

            # Iterate through entities in the schema
            for entity in schema:
                collection_name = entity["entity_label"]

                # Fetch all documents from the current collection
                documents = list(self.db[collection_name].find())
                print(f"Adding {len(documents)} nodes from collection '{collection_name}'.")

                # Add documents as nodes
                for document in documents:
                    # Ensure _id is converted to string for consistent node IDs
                    doc_data = document.copy()
                    # Convert ObjectId to string for storage in the graph node data if desired
                    # This keeps the original ObjectId in the source collection
                    if '_id' in doc_data and isinstance(doc_data['_id'], ObjectId):
                         doc_data['_id'] = str(doc_data['_id'])
                    graph["nodes"].append({
                        "id": str(document["_id"]), # Use string representation of ObjectId as the node ID
                        "entity": collection_name,
                        "data": doc_data # Store the document data
                    })

                # Process relationships
                if "relationships" in entity:
                    relationships = entity["relationships"]
                    for relationship in relationships:
                        related_collection = relationship["related_entity"]
                        local_field = relationship["local_field"]
                        foreign_field = relationship["foreign_field"]
                        # Get relationship type from schema; raise error if missing
                        if "type" not in relationship:
                            raise ValueError(f"Missing 'type' key in relationship definition for entity '{collection_name}' relating to '{related_collection}'. Please define the relationship type in the schema.")
                        relationship_type = relationship["type"]

                        # Create edges by linking related documents
                        for document in documents:
                            # Ensure the value used for lookup matches the type in the related collection
                            lookup_value = document.get(local_field)
                            if lookup_value is None:
                                print(f"Warning: Skipping relationship lookup for document {document.get('_id')} in '{collection_name}' because local field '{local_field}' is missing or null.")
                                continue # Skip if the local field is missing or null

                            # Attempt to find related documents
                            try:
                                related_documents = list(self.db[related_collection].find(
                                    {foreign_field: lookup_value}
                                ))
                                if not related_documents:
                                     print(f"Warning: No related document found in '{related_collection}' for {foreign_field}={lookup_value} (from document {document.get('_id')} in '{collection_name}')")

                                for related_document in related_documents:
                                    graph["edges"].append({
                                        "source": str(document["_id"]),
                                        "target": str(related_document["_id"]),
                                        "relationship": relationship_type # Use defined type
                                    })
                            except Exception as find_error:
                                print(f"Error finding related documents in '{related_collection}' for {foreign_field}={lookup_value}: {find_error}")

                print(f"Added edges for relationships in collection '{collection_name}'.")

            # Store the graph in a dedicated collection
            self.db["Graph"].delete_many({})
            self.db["Graph"].insert_one(graph)

            print("Dynamic graph built successfully and stored in 'Graph' collection.")
        except ValueError as ve: # Catch the specific error for missing type
            print(f"Schema Error: {ve}")
            raise
        except Exception as e:
            print(f"Error building dynamic graph: {e}")
            raise

    def _load_csv(self, file_path, collection_name):
        """
        Load data from a CSV file into a MongoDB collection, ensuring type consistency based on schema.
        """
        try:
            with open(file_path, 'r') as file:
                reader = csv.DictReader(file)
                data = []
                entity_fields = self._get_entity_fields(collection_name)
                for row in reader:
                    # Convert fields based on schema
                    for field in entity_fields:
                        field_name = field["name"]
                        field_type = field["type"]
                        if field_name in row and row[field_name] is not None and row[field_name] != '':
                            original_value = row[field_name]
                            try:
                                if field_type == "number":
                                    row[field_name] = float(original_value)
                                elif field_type == "integer":
                                    row[field_name] = int(original_value)
                                elif field_type == "string":
                                    row[field_name] = str(original_value) # Explicit string conversion
                                # Add other type conversions like 'date', 'boolean' if necessary
                            except (ValueError, TypeError):
                                print(f"Warning: Could not convert value '{original_value}' for field '{field_name}' to type '{field_type}' in {collection_name} (CSV). Keeping original string value.")
                                row[field_name] = str(original_value) # Keep as string on error
                    data.append(row)
                if data: # Only insert if data was successfully read
                    self.db[collection_name].delete_many({})
                    self.db[collection_name].insert_many(data)
                    print(f"Data from {file_path} loaded into {collection_name} collection.")
                else:
                    print(f"No data loaded from {file_path} for {collection_name}.")
        except FileNotFoundError:
            print(f"Error: CSV file not found at {file_path}")
        except Exception as e:
            print(f"Error loading CSV data from {file_path}: {e}")
            raise # Re-raise after logging

    def _get_entity_fields(self, collection_name):
        """
        Get the fields for a given entity from the schema.
        """
        with open(self.schema_file_path, 'r') as schema_file:
            schema = json.load(schema_file)
            for entity in schema:
                if entity["entity_label"] == collection_name:
                    return entity["fields"]
        return []

    def _load_json(self, file_path, collection_name):
        """
        Load data from a JSON file into a MongoDB collection, ensuring type consistency based on schema.
        """
        try:
            with open(file_path, mode='r') as file:
                data = json.load(file)
                entity_fields = self._get_entity_fields(collection_name)
                processed_data = []

                items_to_process = data if isinstance(data, list) else [data]

                for item in items_to_process:
                    if not isinstance(item, dict): # Skip non-dict items in a list
                        print(f"Warning: Skipping non-dictionary item in JSON file {file_path}: {item}")
                        continue

                    processed_item = item.copy() # Work on a copy
                    for field in entity_fields:
                        field_name = field["name"]
                        field_type = field["type"]
                        if field_name in processed_item and processed_item[field_name] is not None:
                            original_value = processed_item[field_name]
                            try:
                                if field_type == "number":
                                    processed_item[field_name] = float(original_value)
                                elif field_type == "integer":
                                    processed_item[field_name] = int(original_value)
                                elif field_type == "string":
                                     processed_item[field_name] = str(original_value) # Explicit string conversion
                                # Add other type conversions like 'date', 'boolean' if necessary
                            except (ValueError, TypeError):
                                 print(f"Warning: Could not convert value '{original_value}' for field '{field_name}' to type '{field_type}' in {collection_name} (JSON). Keeping original value.")
                                 # Decide how to handle error: keep original, set to None, or keep as string? Keeping as string for now.
                                 processed_item[field_name] = str(original_value)

                    processed_data.append(processed_item)

                if processed_data:
                    self.db[collection_name].delete_many({})
                    if isinstance(data, list):
                        self.db[collection_name].insert_many(processed_data)
                    elif isinstance(data, dict):
                         self.db[collection_name].insert_one(processed_data[0]) # Insert the single processed dict
                    print(f"Data from {file_path} loaded into {collection_name} collection.")
                else:
                     print(f"No valid data processed from {file_path} for {collection_name}.")

        except FileNotFoundError:
            print(f"Error: JSON file not found at {file_path}")
        except json.JSONDecodeError:
            print(f"Error: Could not decode JSON from {file_path}")
        except Exception as e:
            print(f"Error loading JSON data from {file_path}: {e}")
            raise # Re-raise after logging

    def _load_xml(self, file_path, collection_name, fields):
        """
        Load data from an XML file into a MongoDB collection. Handles simple child elements
        and attributes based on field names defined in the schema.

        :param file_path: Path to the XML file
        :param collection_name: Name of the MongoDB collection
        :param fields: List of fields to extract from the XML elements
        """
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            data = []
            # Assuming the root contains a list of similar elements (e.g., <Memberships><Membership>...</Membership>...)
            # Adjust the loop if the structure is different (e.g., root.findall('Membership'))
            for element in root:
                record = {}
                for field in fields:
                    field_name = field["name"]
                    field_type = field.get("type", "string") # Default to string if type not specified

                    # Heuristic to check if the field name likely refers to an attribute
                    # Adjust this logic if attribute names clash with element names
                    is_attribute = field_name in ['id', 'studentId', 'active'] # Add other known attribute names here

                    value = None
                    if field_name == "Attendance": # Special case for Attendance/@count
                        attendance_element = element.find('Attendance')
                        if attendance_element is not None:
                            value = attendance_element.get('count')
                    elif is_attribute:
                        value = element.get(field_name) # Get attribute value
                    else:
                        # Try finding a child element
                        child_element = element.find(field_name)
                        if child_element is not None:
                            value = child_element.text # Get element text content

                    # Convert type if value is found
                    if value is not None:
                         try:
                             if field_type == "integer":
                                 record[field_name] = int(value)
                             elif field_type == "number":
                                 record[field_name] = float(value)
                             # Add other type conversions (boolean, date) if needed
                             else:
                                 record[field_name] = value # Keep as string by default
                         except (ValueError, TypeError) as e:
                             print(f"Warning: Could not convert value '{value}' for field '{field_name}' to type '{field_type}'. Storing as string. Error: {e}")
                             record[field_name] = value # Store as string on conversion error
                    # else: # Optional: handle missing optional fields if necessary
                    #    record[field_name] = None

                # Only add record if it's not empty (or meets other criteria)
                if record:
                    data.append(record)

            if data:
                self.db[collection_name].delete_many({})
                self.db[collection_name].insert_many(data)
                print(f"Data from {file_path} loaded into {collection_name} collection.")
            else:
                 print(f"No data extracted from {file_path} for {collection_name}.")
        except ET.ParseError as e:
             print(f"Error parsing XML file {file_path}: {e}")
             raise
        except Exception as e:
            print(f"Error loading XML data from {file_path}: {e}")
            raise

if __name__ == "__main__":
    schema_file_path = "/Users/dhruvasharma/Documents/SEM2/DM/MULTI-DB/base/sample_data/schema_file.json"
    graph_builder = GraphBuilder(schema_file_path)
    graph_builder.load_data_from_schema()
    graph_builder.build_graph()
