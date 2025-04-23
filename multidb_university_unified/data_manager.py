import logging
import csv
import json
import xml.etree.ElementTree as ET
from pymongo import UpdateOne
from bson import ObjectId
from datetime import datetime, timedelta
from dateutil import parser as dateparser
from tabulate import tabulate
import config
from data_models import get_mongo_validation_schemas, find_schema_for_entity
import os  # Ensure os is imported for path resolution

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataManager:
    """Handles dynamic data ingestion, parsing, and retrieval for MongoDB."""

    def __init__(self, db):
        self.db = db
        self.parsers = {}  # Dynamically register parsers

    def register_parser(self, schema_type, parser_function):
        """Register a parser dynamically."""
        self.parsers[schema_type] = parser_function

    def setup_collections_with_validation(self):
        """Dynamically create collections with validation schemas."""
        schemas = get_mongo_validation_schemas()
        for name, schema in schemas.items():
            try:
                self.db.create_collection(name, validator=schema)
                logging.info(f"Created collection '{name}' with validation.")
            except Exception:
                self.db.command('collMod', name, validator=schema)
                logging.info(f"Updated validation for collection '{name}'.")

    def ingest_file(self, entity_label, file_path):
        """Ingest data dynamically based on schema."""
        # Dynamically resolve file paths
        base_path = "/Users/dhruvasharma/Documents/SEM2/DM/MULTI-DB/multidb_university_unified/sample_data"
        normalized_path = os.path.join(base_path, file_path)
        logging.debug(f"Resolved file path for '{entity_label}': {normalized_path}")
        
        if not os.path.exists(normalized_path):
            logging.warning(f"File for entity '{entity_label}' not found at '{normalized_path}'. Skipping ingestion.")
            return False

        schema = find_schema_for_entity(entity_label)
        if not schema:
            logging.error(f"No schema found for '{entity_label}'.")
            return False

        schema_type = schema.get("type")
        parser = self.parsers.get(schema_type)

        if not parser:
            logging.error(f"Unsupported schema type '{schema_type}' for '{entity_label}'.")
            return False

        try:
            data = parser(normalized_path, schema)
            self._bulk_upsert(entity_label, data, schema.get("primary_key"))
            logging.info(f"Ingested {len(data)} records into '{entity_label}'.")
            return True
        except Exception as e:
            logging.error(f"Error ingesting file '{normalized_path}': {e}")
            return False

    def _bulk_upsert(self, collection_name, data, pk_field):
        """Perform bulk upsert into MongoDB."""
        if not pk_field:
            self.db[collection_name].insert_many(data, ordered=False)
            return

        operations = [
            UpdateOne({pk_field: record[pk_field]}, {"$set": record}, upsert=True)
            for record in data if pk_field in record
        ]
        if operations:
            self.db[collection_name].bulk_write(operations, ordered=False)

    def _parse_csv(self, file_path, schema):
        """Parse CSV file dynamically."""
        with open(file_path, mode='r', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            data = []
            for row in reader:
                record = {}
                for col in schema.get("columns", []):
                    field_label = col["label"]
                    field_value = row.get(field_label)
                    # Convert to appropriate type based on schema
                    record[field_label] = self._convert_type(field_value, col.get("data_type"))
                data.append(record)
            return data

    def _parse_xml(self, file_path, schema):
        """Parse XML file dynamically."""
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Get the XML base path and remove any absolute path indicators
            xpath_base = schema.get("xpath_base", "")
            if xpath_base.startswith("/"):
                xpath_base = xpath_base[1:]
            if xpath_base.startswith("//"):
                xpath_base = xpath_base[2:]
                
            # Find elements using the correct path
            elements = []
            if xpath_base:
                elements = root.findall(".//" + xpath_base)
            else:
                elements = [root]  # Use root if no xpath_base specified
                
            data = []
            fields = schema.get("fields", [])
            
            for elem in elements:
                record = {}
                for field in fields:
                    field_label = field.get("label")
                    xpath = field.get("xpath", "")
                    field_type = field.get("data_type")
                    
                    # Handle attribute paths
                    if xpath.startswith("@"):
                        attr_name = xpath[1:]
                        value = elem.get(attr_name)
                    else:
                        # Find child element
                        child = elem.find(xpath) if xpath else None
                        value = child.text if child is not None else None
                        
                    # Convert to appropriate type
                    record[field_label] = self._convert_type(value, field_type)
                    
                data.append(record)
            return data
        except Exception as e:
            logging.error(f"XML parsing error: {e}")
            raise

    def _parse_json(self, file_path, schema):
        """Parse JSON file dynamically."""
        with open(file_path, mode='r', encoding='utf-8') as jsonfile:
            data = json.load(jsonfile)
            if isinstance(data, dict):
                data = [data]
            return [
                {field["label"]: self._convert_type(self._get_value_by_path(item, field["json_path"]), field.get("data_type"))
                 for field in schema.get("fields", [])}
                for item in data
            ]

    def _convert_type(self, value, target_type):
        """Convert value to the target type."""
        if value is None or value == '':
            return None
        
        try:
            # Handle integer types
            if target_type and target_type.upper() in ["INT", "INTEGER", "BIGINT"]:
                return int(value)
                
            # Handle float types
            if target_type and target_type.upper() in ["FLOAT", "DOUBLE", "DECIMAL"]:
                return float(value)
                
            # Handle boolean types
            if target_type and target_type.upper() in ["BOOL", "BOOLEAN"]:
                if isinstance(value, bool):
                    return value
                if isinstance(value, str):
                    return value.lower() in ["true", "1", "yes", "y"]
                return bool(value)
                
            # Handle date types
            if target_type and target_type.upper().startswith("DATE"):
                if isinstance(value, datetime):
                    return value
                from dateutil import parser as dateparser
                return dateparser.parse(value)
                
            # Handle array types
            if target_type and target_type.upper().startswith("ARRAY"):
                if isinstance(value, list):
                    return value
                if isinstance(value, str) and (value.startswith('[') and value.endswith(']')):
                    # Convert string representation of array to actual array
                    import ast
                    return ast.literal_eval(value)
                return [value]  # Single item array
                
            # Default to string
            return str(value)
        except Exception as e:
            logging.warning(f"Type conversion error for value '{value}' to {target_type}: {e}")
            # Return the original value when conversion fails
            return value

    def _extract_xml_value(self, element, field):
        """Extract value from XML element based on field definition."""
        xpath = field.get("xpath")
        if xpath.startswith("@"):
            return element.get(xpath[1:])
        child = element.find(xpath)
        return child.text if child is not None else None

    def _get_value_by_path(self, data, path):
        """Retrieve value from nested dict using dot notation."""
        for key in path.split('.'):
            if isinstance(data, dict):
                data = data.get(key)
            elif isinstance(data, list) and key.isdigit():
                data = data[int(key)]
            else:
                return None
        return data

    def retrieve_data(self, query):
        """Retrieve data dynamically based on query."""
        if not query:
            logging.error("Empty query provided")
            return []
            
        action = query.get("action", "get_entity")
        
        if action == "get_entity":
            entity = query.get("entity")
            if not entity:
                logging.error("No entity specified in query")
                return []
                
            # Ensure entity is a string
            entity = str(entity)
                
            filters = query.get("filters", {})
            fields = query.get("fields", [])
            
            # Convert filter values to their proper types based on schema
            processed_filters = {}
            schema = find_schema_for_entity(entity)
            
            if schema:
                schema_fields = schema.get("columns", []) or schema.get("fields", [])
                for key, value in filters.items():
                    field_def = next((f for f in schema_fields if f.get("label") == key), None)
                    if field_def:
                        data_type = field_def.get("data_type")
                        processed_filters[key] = self._convert_type(value, data_type)
                    else:
                        processed_filters[key] = value
            else:
                processed_filters = filters
                
            # Create projection if fields specified
            projection = None
            if fields:
                projection = {field: 1 for field in fields}
                
            # Query MongoDB collection
            try:
                results = list(self.db[entity].find(processed_filters, projection))
                if results:
                    print(tabulate(results, headers="keys", tablefmt="grid", missingval="N/A"))
                else:
                    print(f"No {entity} found matching the criteria.")
                return results
            except Exception as e:
                logging.error(f"Error querying collection '{entity}': {e}")
                return []
                
        elif action == "get_related":
            # Implementation for related data retrieval
            # ...existing related data retrieval code...
            pass
        else:
            logging.error(f"Unknown query action: {action}")
            return []

# Register default parsers
data_manager = DataManager(db=None)  # Replace `None` with your actual database instance
data_manager.register_parser("table", data_manager._parse_csv)
data_manager.register_parser("xml_structure", data_manager._parse_xml)
data_manager.register_parser("json_objects", data_manager._parse_json)