import pandas as pd
import json
import xml.etree.ElementTree as ET
from datetime import datetime
import os
import logging
from typing import Dict, List, Any, Optional

# --- CSV Schema Inference ---
def infer_csv_schema(file_path: str) -> List[Dict[str, str]]:
    """Infer schema from a CSV file."""
    try:
        df = pd.read_csv(file_path)
        schema = []
        for column in df.columns:
            dtype = str(df[column].dtype)
            if "int" in dtype:
                data_type = "INT"
            elif "float" in dtype:
                data_type = "DECIMAL"
            elif "datetime" in dtype:
                data_type = "DATE"
            else:
                data_type = "STRING"
            schema.append({"label": column, "data_type": data_type})
        return schema
    except Exception as e:
        logging.error(f"Error inferring CSV schema for {file_path}: {e}")
        return []

# --- JSON Schema Inference ---
def infer_json_schema(file_path: str) -> List[Dict[str, str]]:
    """Infer schema from a JSON file."""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        schema = []
        sample = data[0] if isinstance(data, list) else data
        for key, value in sample.items():
            if isinstance(value, int):
                data_type = "INT"
            elif isinstance(value, float):
                data_type = "DECIMAL"
            elif isinstance(value, str):
                data_type = "STRING"
            elif isinstance(value, list):
                data_type = "ARRAY<STRING>"
            elif isinstance(value, dict):
                data_type = "OBJECT"
            else:
                data_type = "STRING"
            schema.append({"label": key, "data_type": data_type})
        return schema
    except Exception as e:
        logging.error(f"Error inferring JSON schema for {file_path}: {e}")
        return []

# --- XML Schema Inference ---
def infer_xml_schema(file_path: str, xpath_base: str = "./record") -> List[Dict[str, str]]:
    """Infer schema from an XML file using a base XPath for records."""
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        schema = []
        records = root.findall(xpath_base)
        if not records:
            logging.warning(f"No elements found for XPath '{xpath_base}' in {file_path}. Falling back to './*'.")
            records = root.findall("./*")
        if records:
            first_record = records[0]
            for child in first_record:
                data_type = "STRING"
                if child.text and child.text.strip().isdigit():
                    data_type = "INT"
                elif child.text and child.text.strip().replace('.', '', 1).isdigit():
                    data_type = "DECIMAL"
                schema.append({"label": child.tag, "data_type": data_type, "xpath": f"./{child.tag}"})
            for attr, value in first_record.attrib.items():
                schema.append({"label": attr, "data_type": "STRING", "xpath": f"@{attr}"})
        return schema
    except ET.ParseError as e:
        logging.error(f"XML parsing error in {file_path}: {e}")
        return []
    except Exception as e:
        logging.error(f"Unexpected error inferring XML schema for {file_path}: {e}")
        return []

# --- Schema Loading ---
def load_schemas_from_file(schema_file_path: str) -> List[Dict[str, Any]]:
    """Load schemas from a user-provided JSON schema file."""
    try:
        if not os.path.exists(schema_file_path):
            raise FileNotFoundError(f"Schema file not found: {schema_file_path}")
        with open(schema_file_path, 'r') as f:
            schemas = json.load(f)
        if not isinstance(schemas, list) or not all(isinstance(item, dict) for item in schemas):
            raise ValueError("Schema file must contain a list of dictionaries.")
        return schemas
    except Exception as e:
        logging.error(f"Error loading schemas from {schema_file_path}: {e}")
        return []

# --- MongoDB Validation Schema Generation ---
def map_data_type_to_bson(data_type: str) -> Any:
    """Map schema data types to MongoDB BSON types."""
    type_mapping = {
        "INT": "int",
        "DECIMAL": "double",
        "DATE": "date",
        "STRING": "string",
        "ARRAY<STRING>": {"bsonType": "array", "items": {"bsonType": "string"}},
        "OBJECT": "object"
    }
    return type_mapping.get(data_type, "string")

def generate_mongo_validation_schemas(schemas: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Generate MongoDB validation schemas dynamically."""
    validation_schemas = {}
    for source in schemas:
        for entity in source.get("entities", []):
            collection_name = entity["label"]
            properties = {}
            required_fields = []
            for field in entity.get("columns", []):
                field_name = field["label"]
                bson_type = map_data_type_to_bson(field["data_type"])
                properties[field_name] = {"bsonType": bson_type}
                if field.get("is_primary_key") or field.get("required", False):
                    required_fields.append(field_name)
            validation_schemas[collection_name] = {
                "$jsonSchema": {
                    "bsonType": "object",
                    "required": required_fields,
                    "properties": properties
                }
            }
    return validation_schemas

# --- Schema Finder ---
def find_schema_for_entity(entity_label: str, file_path: str) -> Optional[Dict[str, Any]]:
    """Find or infer the schema for a given entity label."""
    file_extension = os.path.splitext(file_path)[1].lower()
    if file_extension == ".csv":
        return {"schema": infer_csv_schema(file_path), "source_type": "CSV"}
    elif file_extension == ".json":
        return {"schema": infer_json_schema(file_path), "source_type": "JSON"}
    elif file_extension == ".xml":
        inferred_schema = infer_xml_schema(file_path)
        if inferred_schema:
            return {"schema": inferred_schema, "source_type": "XML"}
        logging.error(f"Failed to infer schema for XML file: {file_path}")
        return None
    else:
        logging.warning(f"Unsupported file type for schema inference: {file_extension}")
        return None

def get_collection_schemas(schema_file_path="/Users/dhruvasharma/Documents/SEM2/DM/MULTI-DB/market_data/schema_file.json"):
    """
    Load schema definitions for University data sources (CSV, JSON, XML)
    from a user-provided schema file.
    """
    return load_schemas_from_file(schema_file_path)

def get_mongo_validation_schemas(schema_file_path="/Users/dhruvasharma/Documents/SEM2/DM/MULTI-DB/market_data/schema_file.json"):
    """
    Generate MongoDB validation schemas dynamically from the user-provided schema file.
    """
    schemas = load_schemas_from_file(schema_file_path)
    validation_schemas = {}
    for source in schemas:
        for entity in source.get("entities", []):
            collection_name = entity["label"]
            properties = {}
            required_fields = []
            for field in entity.get("columns", entity.get("fields", [])):
                field_name = field["label"]
                data_type = field["data_type"]
                bson_type = map_data_type_to_bson(data_type)
                properties[field_name] = {"bsonType": bson_type}
                required = field.get("required", [])
                if isinstance(required, bool):
                    required = [field_name] if required else []
                elif not isinstance(required, list):
                    required = []
                if field.get("is_primary_key") or field_name in required:
                    required_fields.append(field_name)
            validation_schemas[collection_name] = {
                "$jsonSchema": {
                    "bsonType": "object",
                    "required": required_fields,
                    "properties": properties
                }
            }
    return validation_schemas

# --- Helper function to find schema by label ---
_schema_cache = None
_collection_schemas = None

def get_collection_schemas():
    """Return the schemas for all collections."""
    global _collection_schemas
    
    if _collection_schemas is not None:
        return _collection_schemas
    
    # Build collection schemas from the schema file
    schemas = []
    raw_schemas = get_schemas()
    
    if isinstance(raw_schemas, dict) and raw_schemas:
        for source_type, sources in raw_schemas.items():
            for source in sources:
                schemas.append(source)
    elif isinstance(raw_schemas, list):
        schemas = raw_schemas
    
    _collection_schemas = schemas
    return schemas

def get_schemas():
    """Get raw schemas from file."""
    global _schema_cache
    
    if (_schema_cache):
        return _schema_cache
    
    base_dir = "/Users/dhruvasharma/Documents/SEM2/DM/MULTI-DB/multidb_university_unified/sample_data"
    schema_path = os.path.join(base_dir, "schema_file.json")
    
    if not os.path.exists(schema_path):
        logging.error(f"Schema file not found at {schema_path}")
        return []
    
    try:
        with open(schema_path, 'r') as f:
            _schema_cache = json.load(f)
        return _schema_cache
    except Exception as e:
        logging.error(f"Error loading schema: {e}")
        return []

def get_mongo_validation_schemas():
    """Generate MongoDB JSON Schema validation for each entity."""
    validation_schemas = {}
    
    for source in get_collection_schemas():
        for entity in source.get('entities', []):
            entity_label = entity.get('label')
            if not entity_label:
                continue
            
            # Create a lenient validation schema
            validation_schema = {
                "$jsonSchema": {
                    "bsonType": "object",
                    "additionalProperties": True,
                    "required": [],
                    "properties": {}
                }
            }
            
            fields = entity.get('columns', []) or entity.get('fields', [])
            for field in fields:
                field_name = field.get('label')
                if not field_name:
                    continue
                
                if field.get('is_primary_key'):
                    validation_schema['$jsonSchema']['required'].append(field_name)
                
                # Create properties with multiple accepted types
                validation_schema['$jsonSchema']['properties'][field_name] = {
                    "bsonType": ["string", "int", "double", "bool", "date", "array", "object", "null"]
                }
            
            validation_schemas[entity_label] = validation_schema
            
    return validation_schemas

def find_schema_for_entity(entity_label: str) -> Optional[Dict[str, Any]]:
    """Find the schema definition for a specific entity by label."""
    if not entity_label:
        return None
        
    for source in get_collection_schemas():
        for entity in source.get('entities', []):
            if entity.get('label') == entity_label:
                return entity
    return None

def flush_collection(mongo_db, collection_name):
    """
    Delete all documents from the specified MongoDB collection.
    Call this before ingesting new data to avoid duplicates.
    """
    if collection_name in mongo_db.list_collection_names():
        mongo_db[collection_name].delete_many({})
        logging.info(f"Flushed collection '{collection_name}'.")
    else:
        logging.info(f"Collection '{collection_name}' does not exist, nothing to flush.")