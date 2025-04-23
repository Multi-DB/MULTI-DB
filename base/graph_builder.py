import json
import csv
import os
import xml.etree.ElementTree as ET
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
                    graph["nodes"].append({
                        "id": str(document["_id"]),
                        "entity": collection_name,
                        "data": document
                    })

                # Process relationships
                if "relationships" in entity:
                    relationships = entity["relationships"]
                    for relationship in relationships:
                        related_collection = relationship["related_entity"]
                        local_field = relationship["local_field"]
                        foreign_field = relationship["foreign_field"]

                        # Create edges by linking related documents
                        for document in documents:
                            related_documents = self.db[related_collection].find(
                                {foreign_field: document[local_field]}
                            )
                            for related_document in related_documents:
                                graph["edges"].append({
                                    "source": str(document["_id"]),
                                    "target": str(related_document["_id"]),
                                    "relationship": relationship.get("type", "related")
                                })
                print(f"Added edges for relationships in collection '{collection_name}'.")

            # Store the graph in a dedicated collection
            self.db["Graph"].delete_many({})
            self.db["Graph"].insert_one(graph)

            print("Dynamic graph built successfully and stored in 'Graph' collection.")
        except Exception as e:
            print(f"Error building dynamic graph: {e}")
            raise

    def _load_csv(self, file_path, collection_name):
        """
        Load data from a CSV file into a MongoDB collection.
        """
        try:
            with open(file_path, 'r') as file:
                reader = csv.DictReader(file)
                data = []
                for row in reader:
                    # Convert numeric fields based on schema
                    for field in self._get_entity_fields(collection_name):
                        field_name = field["name"]
                        field_type = field["type"]
                        if field_name in row:
                            if field_type == "number":
                                row[field_name] = float(row[field_name])
                            elif field_type == "integer":
                                row[field_name] = int(row[field_name])
                    data.append(row)
                self.db[collection_name].delete_many({})
                self.db[collection_name].insert_many(data)
                print(f"Data from {file_path} loaded into {collection_name} collection.")
        except Exception as e:
            print(f"Error loading CSV data: {e}")
            raise

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
        Load data from a JSON file into a MongoDB collection.

        :param file_path: Path to the JSON file
        :param collection_name: Name of the MongoDB collection
        """
        try:
            with open(file_path, mode='r') as file:
                data = json.load(file)
                if isinstance(data, list):
                    self.db[collection_name].insert_many(data)
                elif isinstance(data, dict):
                    self.db[collection_name].insert_one(data)
                else:
                    raise TypeError("Unsupported JSON structure. Must be a dict or a list of dicts.")
                print(f"Data from {file_path} loaded into {collection_name} collection.")
        except Exception as e:
            print(f"Error loading JSON data: {e}")
            raise

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
    schema_file_path = "/Users/dhruvasharma/Documents/SEM2/DM/MULTI-DB/base/market_data/schema_file.json"
    graph_builder = GraphBuilder(schema_file_path)
    graph_builder.load_data_from_schema()
    graph_builder.build_graph()
