import csv
import json
import xml.etree.ElementTree as ET
from connection import get_mongo_connection

def build_metadata_graph_from_csv(file_path, table_name, db):
    """
    Parse a CSV file, build a metadata graph, and store it in the 'relational' collection in MongoDB.
    Store the actual data in the 'relational_data' collection.

    :param file_path: Path to the CSV file
    :param table_name: Name of the table (used as a label in the graph)
    :param db: MongoDB database object
    """
    try:
        # Read the CSV file
        with open(file_path, mode='r') as file:
            reader = csv.DictReader(file)
            columns = reader.fieldnames

            # Store actual data in 'relational_data' collection
            data = [row for row in reader]
            data_reference = db["relational_data"].insert_many(data).inserted_ids

            # Build nodes for the table and its columns
            nodes = [{"id": f"table_{table_name}", "type": "table", "label": table_name, "properties": {"data_reference": str(data_reference)}}]
            for column in columns:
                nodes.append({
                    "id": f"column_{column}_{table_name}",
                    "type": "column",
                    "label": column,
                    "properties": {"data_type": "unknown"}  # Data type can be inferred later
                })

            # Build edges between the table and its columns
            edges = []
            for column in columns:
                edges.append({
                    "source": f"table_{table_name}",
                    "target": f"column_{column}_{table_name}",
                    "relation": "HAS_COLUMN",
                    "properties": {}
                })

            # Create the metadata graph
            metadata_graph = {
                "metadata": {
                    "source_type": "relational",
                    "source_name": file_path
                },
                "nodes": nodes,
                "edges": edges
            }

            # Store the metadata graph in the 'relational' collection
            db["relational"].insert_one(metadata_graph)
            print(f"Metadata graph for table '{table_name}' stored in MongoDB.")
    except Exception as e:
        print(f"Error building metadata graph: {e}")
        raise

def build_metadata_graph_from_xml(file_path, db):
    """
    Parse an XML file, build a metadata graph, and store it in the 'xml' collection in MongoDB.
    Store the actual data in the 'xml_data' collection.

    :param file_path: Path to the XML file
    :param db: MongoDB database object
    """
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()

        # Store actual XML data in 'xml_data' collection
        xml_data = ET.tostring(root, encoding='unicode')
        data_reference = db["xml_data"].insert_one({"xml_content": xml_data}).inserted_id

        # Recursive function to build nodes and edges
        def parse_element(element, parent_id=None):
            element_id = f"element_{element.tag}_{id(element)}"
            nodes.append({
                "id": element_id,
                "type": "element",
                "label": element.tag,
                "properties": {**element.attrib, "text_content": element.text.strip() if element.text else ""}
            })
            if parent_id:
                edges.append({
                    "source": parent_id,
                    "target": element_id,
                    "relation": "PARENT_CHILD",
                    "properties": {}
                })
            for child in element:
                parse_element(child, element_id)

        # Build nodes and edges
        nodes = [{"id": "root_element", "type": "root", "label": root.tag, "properties": {"data_reference": str(data_reference)}}]
        edges = []
        parse_element(root)

        # Create the metadata graph
        metadata_graph = {
            "metadata": {
                "source_type": "xml",
                "source_name": file_path
            },
            "nodes": nodes,
            "edges": edges
        }

        # Store the metadata graph in the 'xml' collection
        db["xml"].insert_one(metadata_graph)
        print(f"Metadata graph for XML file '{file_path}' stored in MongoDB.")
    except Exception as e:
        print(f"Error building metadata graph from XML: {e}")
        raise

def build_metadata_graph_from_json(file_path, db):
    """
    Parse a JSON file, build a metadata graph, and store it in the 'json' collection in MongoDB.
    Store the actual data in the 'json_data' collection.

    :param file_path: Path to the JSON file
    :param db: MongoDB database object
    """
    try:
        with open(file_path, mode='r') as file:
            data = json.load(file)

        # Handle cases where the JSON data is a list
        if isinstance(data, list):
            data_reference = db["json_data"].insert_many(data).inserted_ids
        elif isinstance(data, dict):
            data_reference = db["json_data"].insert_one(data).inserted_id
        else:
            raise TypeError("Unsupported JSON structure. Must be a dict or a list of dicts.")

        # Recursive function to build nodes and edges
        def parse_json(obj, parent_id=None, key=None):
            node_id = f"node_{id(obj)}"
            if isinstance(obj, dict):
                nodes.append({"id": node_id, "type": "object", "label": key or "root", "properties": {}})
                if parent_id:
                    edges.append({
                        "source": parent_id,
                        "target": node_id,
                        "relation": "PROPERTY_OF",
                        "properties": {"key": key}
                    })
                for k, v in obj.items():
                    parse_json(v, node_id, k)
            elif isinstance(obj, list):
                nodes.append({"id": node_id, "type": "array", "label": key or "root", "properties": {}})
                if parent_id:
                    edges.append({
                        "source": parent_id,
                        "target": node_id,
                        "relation": "PROPERTY_OF",
                        "properties": {"key": key}
                    })
                for i, item in enumerate(obj):
                    parse_json(item, node_id, f"index_{i}")
            else:
                nodes.append({"id": node_id, "type": "primitive", "label": key, "properties": {"value": obj}})
                if parent_id:
                    edges.append({
                        "source": parent_id,
                        "target": node_id,
                        "relation": "PROPERTY_OF",
                        "properties": {"key": key}
                    })

        # Build nodes and edges
        nodes = [{"id": "root_object", "type": "root", "label": "root", "properties": {"data_reference": str(data_reference)}}]
        edges = []
        parse_json(data)

        # Create the metadata graph
        metadata_graph = {
            "metadata": {
                "source_type": "json",
                "source_name": file_path
            },
            "nodes": nodes,
            "edges": edges
        }

        # Store the metadata graph in the 'json' collection
        db["json"].insert_one(metadata_graph)
        print(f"Metadata graph for JSON file '{file_path}' stored in MongoDB.")
    except Exception as e:
        print(f"Error building metadata graph from JSON: {e}")
        raise

def process_schema_file(schema_file_path, db):
    """
    Process the schema file and build metadata graphs for all entities.

    :param schema_file_path: Path to the schema file
    :param db: MongoDB database object
    """
    try:
        with open(schema_file_path, 'r') as schema_file:
            schema = json.load(schema_file)

        for entity in schema:
            entity_label = entity["entity_label"]
            file_path = entity["file_path"]
            print(f"Processing {entity_label} from {file_path}...")

            if file_path.endswith(".csv"):
                build_metadata_graph_from_csv(file_path, entity_label, db)
            elif file_path.endswith(".xml"):
                build_metadata_graph_from_xml(file_path, db)
            elif file_path.endswith(".json"):
                build_metadata_graph_from_json(file_path, db)
            else:
                print(f"Unsupported file type for {file_path}")
    except Exception as e:
        print(f"Error processing schema file: {e}")
        raise

if __name__ == "__main__":
    db = get_mongo_connection()
    schema_file_path = "/Users/dhruvasharma/Documents/SEM2/DM/MULTI-DB/base/market_data/schema_file.json"
    process_schema_file(schema_file_path, db)
