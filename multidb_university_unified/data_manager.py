# data_manager.py
import logging
import csv
import json
import xml.etree.ElementTree as ET
from datetime import datetime
from dateutil import parser as dateparser # More flexible date parsing
from pymongo.database import Database
from pymongo.errors import CollectionInvalid, OperationFailure, BulkWriteError
from bson import ObjectId # Not used directly here, but good practice
from tabulate import tabulate
import re

import config
from data_models import get_mongo_validation_schemas, find_schema_for_entity
from graph_models import GraphNode # For type hinting

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s') # DEBUG

# Helper for nested dictionary access in JSON parser
def _get_value_by_path(data_dict, path_string):
    """Retrieves a value from a nested dict using dot notation."""
    keys = path_string.split('.')
    value = data_dict
    try:
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            elif isinstance(value, list) and key.isdigit(): # Basic list index access
                 value = value[int(key)]
            else:
                return None # Path leads to non-dict/list or key not found
            if value is None:
                return None # Intermediate key not found
        return value
    except (KeyError, IndexError, TypeError):
        return None # Invalid path or type


class DataManager:
    """Handles data ingestion, parsing, insertion, and retrieval across multiple collections."""

    def __init__(self, db: Database):
        self.db = db
        self.nodes_collection = db[config.NODES_COLLECTION]
        self.edges_collection = db[config.EDGES_COLLECTION]

    def _get_data_collection(self, collection_name: str):
        return self.db[collection_name]

    def setup_collections_with_validation(self):
        """Creates target MongoDB collections and applies validation/indexes."""
        logging.info("Setting up data collections with validation...")
        validation_schemas = get_mongo_validation_schemas()
        all_entity_labels = list(validation_schemas.keys())

        existing_collections = self.db.list_collection_names()

        for collection_name in all_entity_labels:
            schema = validation_schemas[collection_name]
            if collection_name not in existing_collections:
                try:
                    self.db.create_collection(collection_name, validator=schema)
                    logging.info(f"  Created collection '{collection_name}' with validation.")
                except (CollectionInvalid, OperationFailure) as e:
                     logging.error(f"  Failed to create collection '{collection_name}': {e}")
            else:
                try:
                    self.db.command('collMod', collection_name, validator=schema)
                    logging.info(f"  Attempted to update validation for existing collection '{collection_name}'.")
                except OperationFailure as e:
                     logging.warning(f"  Could not update validation for collection '{collection_name}': {e}.")

            pk_field = self._find_primary_key_field_by_label(collection_name)
            if pk_field:
                try:
                    collection = self.db[collection_name]
                    index_info = collection.index_information()
                    pk_index_name = f"{pk_field}_unique_pk"

                    existing_pk_index = None
                    for name, info in index_info.items():
                        # Check if the key definition matches exactly [(pk_field, 1)] and is unique
                        if info.get('key') == [(pk_field, 1)] and info.get('unique'):
                            existing_pk_index = name
                            break
                        # Check if an index exists but isn't unique (problematic)
                        elif info.get('key') == [(pk_field, 1)] and not info.get('unique'):
                             logging.warning(f"  Found non-unique index on PK field '{pk_field}' for '{collection_name}'. Cannot enforce uniqueness automatically.")
                             existing_pk_index = "non_unique" # Mark as problematic
                             break

                    if existing_pk_index == "non_unique":
                        pass # Already logged warning
                    elif existing_pk_index:
                         logging.info(f"  Unique index on PK '{pk_field}' already exists for '{collection_name}'.")
                    else:
                        collection.create_index([(pk_field, 1)], unique=True, name=pk_index_name)
                        logging.info(f"  Created unique index on PK '{pk_field}' for '{collection_name}'.")

                except Exception as idx_e:
                    logging.warning(f"  Could not ensure unique index on PK '{pk_field}' for '{collection_name}': {idx_e}")

    # --- Data Ingestion and Parsing ---

    def ingest_and_process_file(self, entity_label: str, file_path: str):
        """Ingests data from file, parses based on schema, inserts into MongoDB."""
        logging.info(f"Starting ingestion for entity '{entity_label}' from file: {file_path}")

        schema_info = find_schema_for_entity(entity_label)
        if not schema_info:
            logging.error(f"  No schema definition found for entity label '{entity_label}'. Cannot ingest.")
            return False

        entity_schema = schema_info["schema"] # The specific entity definition from data_models
        target_collection_name = entity_label
        # **FIX**: Use entity_schema['type'] to determine the parser
        entity_type = entity_schema.get('type')

        parsed_data = []
        try:
            if entity_type == "table": # Maps to CSV in our current model
                parsed_data = self._parse_csv(file_path, entity_schema)
            elif entity_type == "xml_structure":
                parsed_data = self._parse_xml(file_path, entity_schema)
            elif entity_type == "json_objects":
                parsed_data = self._parse_json(file_path, entity_schema)
            else:
                # Use source_type as a fallback if needed, or handle other entity_types
                logging.error(f"  Unsupported entity schema type '{entity_type}' for entity '{entity_label}'.")
                return False

            if not parsed_data:
                 logging.warning(f"  Parsing yielded no data for '{entity_label}' from {file_path}.")
                 return True # Allow proceeding (maybe file was intentionally empty)

            logging.info(f"  Successfully parsed {len(parsed_data)} records for '{entity_label}'.")

        except FileNotFoundError:
             logging.error(f"  File not found: {file_path}")
             return False
        except Exception as e:
            logging.error(f"  Error parsing file {file_path} for '{entity_label}' (type: {entity_type}): {e}", exc_info=True)
            return False

        # --- Insert Parsed Data ---
        data_collection = self._get_data_collection(target_collection_name)
        pk_field = self._find_primary_key_field_by_label(target_collection_name)

        inserted_count = 0
        skipped_count = 0
        error_count = 0
        try:
            # Use bulk write operations with upsert for efficiency and handling duplicates
            if pk_field:
                from pymongo import UpdateOne
                operations = []
                for record in parsed_data:
                    pk_value = record.get(pk_field)
                    if pk_value is not None:
                         operations.append(
                             UpdateOne({pk_field: pk_value}, {"$set": record}, upsert=True)
                         )
                    else:
                         logging.warning(f"  Record for '{entity_label}' missing PK '{pk_field}': {record}. Skipping upsert.")
                         skipped_count += 1

                if operations:
                    logging.debug(f"Attempting bulk upsert for {len(operations)} records into '{target_collection_name}' based on PK '{pk_field}'.")
                    bulk_result = data_collection.bulk_write(operations, ordered=False)
                    inserted_count = bulk_result.upserted_count
                    logging.info(f"  Bulk write completed. Upserted (new): {bulk_result.upserted_count}, Matched/Updated: {bulk_result.matched_count}")
                    # **FIX**: Check for write errors safely
                    # Access the raw result dictionary for writeErrors
                    write_errors = bulk_result.bulk_api_result.get('writeErrors')
                    if write_errors:
                        error_count = len(write_errors)
                        logging.error(f"  {error_count} errors during bulk write. Example: {write_errors[0]}")

            else:
                # No PK defined, attempt insert_many (less safe for duplicates if data changes)
                logging.debug(f"Attempting insert_many for {len(parsed_data)} records into '{target_collection_name}' (no PK defined).")
                result = data_collection.insert_many(parsed_data, ordered=False)
                inserted_count = len(result.inserted_ids)
                logging.info(f"  Insert_many completed. Inserted: {inserted_count}.")

            logging.info(f"  Finished inserting data into '{target_collection_name}'. Inserted/Upserted: {inserted_count}, Skipped (e.g., missing PK): {skipped_count}, Write Errors: {error_count}")
            return True

        except BulkWriteError as bwe:
            # This catches errors not handled by ordered=False, e.g., validation errors on upsert
            logging.error(f"  Bulk write error inserting into '{target_collection_name}': {bwe.details}", exc_info=True)
            inserted_count = bwe.details.get('nUpserted', 0) # Get upserted count
            error_count = len(bwe.details.get('writeErrors', []))
            logging.info(f"  Operation failed. Inserted/Upserted: {inserted_count}. Write errors: {error_count}")
            return False # Indicate failure
        except Exception as e:
            logging.error(f"  Error inserting data into '{target_collection_name}': {e}", exc_info=True)
            return False

    def _parse_csv(self, file_path: str, schema: dict) -> list[dict]:
        """Parses a CSV file based on the schema columns."""
        data = []
        # **FIX** Get columns from 'columns' key
        columns = schema.get("columns", [])
        if not columns:
            raise ValueError(f"CSV parsing requires 'columns' definition in schema for '{schema.get('label')}'.")

        expected_headers = [col["label"] for col in columns]
        column_info = {col["label"]: col for col in columns} # Map label to full column schema

        with open(file_path, mode='r', encoding='utf-8-sig') as csvfile: # Use utf-8-sig for potential BOM
            reader = csv.DictReader(csvfile)
            # Make header check more robust if needed (e.g., checking for subsets)
            if not reader.fieldnames or set(reader.fieldnames) != set(expected_headers):
                logging.warning(f"CSV Headers mismatch! File: '{file_path}'. Expected: {expected_headers}, Found: {reader.fieldnames}. Trying to map...")

            for row_num, row in enumerate(reader, 1):
                record = {}
                try:
                    for col_label in expected_headers:
                        col_schema = column_info[col_label]
                        # Attempt case-insensitive mapping if exact header not found
                        raw_value = row.get(col_label)
                        if raw_value is None:
                             for file_header in row:
                                 if file_header.lower() == col_label.lower():
                                      raw_value = row[file_header]
                                      break
                        record[col_label] = self._convert_type(raw_value, col_schema.get("data_type"), col_label)
                    data.append(record)
                except Exception as row_err:
                    logging.error(f"  Error processing CSV row {row_num} in {file_path}: {row_err} - Row: {row}", exc_info=True)
                    continue # Skip problematic row
        return data

    def _parse_xml(self, file_path: str, schema: dict) -> list[dict]:
        """Parses an XML file based on the schema fields and XPath expressions."""
        data = []
        entity_label = schema.get("label")
        xpath_base = schema.get("xpath_base") # XPath to get the list of main records
        fields_schema = schema.get("fields", [])

        if not xpath_base or not fields_schema:
            raise ValueError(f"XML parsing requires 'xpath_base' and 'fields' definition in schema for '{entity_label}'.")

        try:
            tree = ET.parse(file_path)
            root = tree.getroot()

            # **FIX**: Use relative XPath from root for base elements if starting with //
            # The expression ".//" means search from the current node downwards.
            if xpath_base.startswith("//"):
                relative_xpath_base = "." + xpath_base
            else:
                relative_xpath_base = xpath_base # Assume already relative if not //

            base_elements = root.findall(relative_xpath_base)
            logging.debug(f"  Found {len(base_elements)} elements matching XPath '{relative_xpath_base}' from root in {file_path}")

            for elem_num, base_elem in enumerate(base_elements):
                record = {}
                try:
                    for field_info in fields_schema:
                        target_field_label = field_info["label"]
                        xpath_expr = field_info.get("xpath") # Path relative to base_elem
                        data_type = field_info.get("data_type")

                        if not xpath_expr: continue # Skip if no xpath

                        raw_value = None
                        # Handle attribute (@attr) vs element (tag) vs child attribute (tag/@attr)
                        # These paths are relative to base_elem
                        if xpath_expr.startswith('@'): # Attribute of base_elem
                             attr_name = xpath_expr[1:]
                             raw_value = base_elem.get(attr_name)
                        elif '/@' in xpath_expr: # Attribute of a direct child element
                             parts = xpath_expr.split('/@')
                             child_tag = parts[0]
                             attr_name = parts[1]
                             # Find first matching child relative to base_elem
                             child_elem = base_elem.find(child_tag)
                             if child_elem is not None:
                                 raw_value = child_elem.get(attr_name)
                        else: # Assume direct child element text
                             # Find first matching child relative to base_elem
                             child_elem = base_elem.find(xpath_expr)
                             if child_elem is not None:
                                 raw_value = child_elem.text

                        record[target_field_label] = self._convert_type(raw_value, data_type, target_field_label)

                    # Add record only if it's not empty or has a PK
                    pk_field = self._find_primary_key_field_by_label(entity_label)
                    if (pk_field and record.get(pk_field) is not None) or (not pk_field and any(record.values())):
                         data.append(record)
                    else:
                         logging.debug(f"  Skipping XML element {elem_num} potentially missing PK or empty: {ET.tostring(base_elem, encoding='unicode').strip()}")

                except Exception as elem_err:
                    logging.error(f"  Error processing XML element {elem_num} in {file_path}: {elem_err}", exc_info=True)
                    continue # Skip problematic element
        except ET.ParseError as e:
            logging.error(f"  XML Parse Error in {file_path}: {e}", exc_info=True); raise
        except Exception as e:
            logging.error(f"  Unexpected error parsing XML {file_path}: {e}", exc_info=True); raise
        return data

    def _parse_json(self, file_path: str, schema: dict) -> list[dict]:
        """Parses a JSON file (list or single object) based on schema fields and json_path."""
        # (No changes needed in this function from previous version)
        data = []
        entity_label = schema.get("label")
        fields_schema = schema.get("fields", [])
        if not fields_schema:
            raise ValueError(f"JSON parsing requires 'fields' definition in schema for '{entity_label}'.")

        with open(file_path, mode='r', encoding='utf-8') as jsonfile:
            try:
                json_content = json.load(jsonfile)
            except json.JSONDecodeError as e:
                logging.error(f"  JSON Decode Error in {file_path}: {e}", exc_info=True); raise

        if isinstance(json_content, dict): json_content = [json_content]
        elif not isinstance(json_content, list):
            raise ValueError(f"Unsupported JSON structure in {file_path}. Expected list or object.")

        for index, item in enumerate(json_content):
            if not isinstance(item, dict):
                logging.warning(f"  Skipping non-object item at index {index} in JSON file {file_path}."); continue

            record = {}
            try:
                for field_info in fields_schema:
                    target_field_label = field_info["label"]
                    json_path = field_info.get("json_path")
                    data_type = field_info.get("data_type")
                    if not json_path: continue # Skip if no path

                    raw_value = _get_value_by_path(item, json_path)
                    record[target_field_label] = self._convert_type(raw_value, data_type, target_field_label)

                pk_field = self._find_primary_key_field_by_label(entity_label)
                if (pk_field and record.get(pk_field) is not None) or (not pk_field and any(v is not None for v in record.values())):
                     data.append(record)
                else:
                     logging.debug(f"  Skipping JSON object {index} potentially missing PK or empty: {item}")

            except Exception as item_err:
                 logging.error(f"  Error processing JSON object at index {index} in {file_path}: {item_err} - Item: {item}", exc_info=True)
                 continue
        return data


    def _convert_type(self, value: any, target_type: str | None, field_name: str):
        """Converts parsed values to appropriate Python types based on schema data_type."""
        # (No changes needed in this helper function from previous version)
        if value is None or value == '': return None
        if target_type is None: return value
        target_type = target_type.upper()
        try:
            if "INT" in target_type: return int(float(value))
            elif "DECIMAL" in target_type or "FLOAT" in target_type or "DOUBLE" in target_type: return float(value)
            elif "DATE" in target_type or "DATETIME" in target_type:
                 try: return dateparser.parse(str(value))
                 except (ValueError, TypeError) as dt_err:
                      logging.warning(f"  Could not parse date/datetime field '{field_name}' value '{value}': {dt_err}. None.")
                      return None
            elif "BOOLEAN" in target_type:
                 if isinstance(value, bool): return value
                 lv = str(value).lower()
                 if lv in ['true', '1', 't', 'y', 'yes']: return True
                 if lv in ['false', '0', 'f', 'n', 'no']: return False
                 logging.warning(f"  Unrecognized boolean field '{field_name}' value '{value}'. None.")
                 return None
            elif target_type.startswith("ARRAY"):
                 if isinstance(value, list): return value
                 else:
                     logging.warning(f"  Value for array field '{field_name}' is not list: '{value}'. None.")
                     return None
            elif "VARCHAR" in target_type or "STRING" in target_type: return str(value)
            else: logging.warning(f"Unknown target_type '{target_type}' field '{field_name}'. Raw."); return value
        except (ValueError, TypeError) as e:
             logging.warning(f"  Type conversion error field '{field_name}': Cannot convert '{value}' ({type(value)}) to {target_type}. Error: {e}. None.")
             return None


    # --- Data Retrieval Methods ---

    def find_entity_node(self, label: str) -> dict | None:
        """Finds a logical entity node (collection) by its label."""
        return self.nodes_collection.find_one({"label": label, "type": "collection"})

    def get_entity_fields(self, entity_node_id: str) -> list[str]:
        """Gets the labels of fields belonging to an entity node (collection)."""
        field_edges = self.edges_collection.find({"source": entity_node_id, "relation": "HAS_FIELD"})
        field_node_ids = [edge["target"] for edge in field_edges]
        if not field_node_ids: return []
        field_nodes = self.nodes_collection.find({"_id": {"$in": field_node_ids}}).sort("label", 1)
        return [node["label"] for node in field_nodes if "label" in node]

    def retrieve_data(self, query_details: dict):
        """Retrieves data based on query details, potentially traversing the graph."""
        action = query_details.get("action")
        logging.info(f"Executing retrieve action: {action}")
        try:
            if action == "get_entity":
                return self._retrieve_single_entity(query_details)
            elif action == "get_related":
                return self._retrieve_related_data(query_details)
            else:
                logging.error(f"Unsupported retrieval action: {action}")
                print(f"❌ Unsupported retrieval action: {action}")
                return None
        except Exception as e:
             logging.exception(f"An unexpected error occurred during retrieve_data action '{action}': {e}")
             print(f"❌ An unexpected error occurred during retrieval: {e}")
             return None

    def _retrieve_single_entity(self, query_details: dict):
        """Handles simple retrieval from a single entity collection."""
        try:
            entity_label = query_details.get("entity")
            requested_fields = query_details.get("fields")
            filters = query_details.get("filters", {})

            entity_node = self.find_entity_node(entity_label)
            if not entity_node:
                logging.error(f"Entity '{entity_label}' not found in metadata.")
                print(f"❌ Entity '{entity_label}' not found.")
                return None

            collection_name = entity_node.get("collection_name")
            if not collection_name:
                logging.error(f"Metadata for '{entity_label}' missing collection name.")
                print(f"❌ Cannot find data collection for '{entity_label}'.")
                return None

            data_collection = self._get_data_collection(collection_name)
            metadata_headers = self.get_entity_fields(entity_node["_id"])
            projection = self._build_projection(requested_fields, metadata_headers)

            logging.debug(f"Retrieving from {collection_name} with filter: {filters}, projection: {projection}")
            results = list(data_collection.find(filters, projection))
            print(f"\n📄 Data from '{entity_label}':\n")

            if not results:
                 # Use calculated headers for empty table
                 empty_headers = requested_fields if requested_fields is not None else metadata_headers
                 if "_id" in empty_headers and projection.get("_id", 1) == 0: empty_headers.remove("_id")
                 elif "_id" not in empty_headers and projection.get("_id", 1) == 1 and not requested_fields : empty_headers.insert(0, "_id") # Add _id if default projection includes it
                 # Ensure _id is not included if projection excludes it
                 if projection.get("_id", 1) == 0 and "_id" in empty_headers: empty_headers.remove("_id")
                 print(tabulate([], headers=sorted(empty_headers), tablefmt="grid"))
            else:
                # **FIX**: Use headers="keys" for list of dicts input to tabulate
                print(tabulate(results, headers="keys", tablefmt="grid", missingval="N/A"))

            return results
        except Exception as e:
            # Catch specific tabulate errors if needed, otherwise general exception
            logging.exception(f"Error in _retrieve_single_entity for '{query_details.get('entity')}': {e}")
            print(f"❌ Error displaying data for {query_details.get('entity')}.")
            return None

    def _retrieve_related_data(self, query_details: dict):
        """Handles retrieval involving graph traversal and joins."""
        # (Logic is mostly unchanged, relies on graph and data collections)
        try:
            start_entity_label = query_details.get("start_entity")
            start_filters = query_details.get("start_filters", {})
            relations = query_details.get("relations", [])
            final_fields = query_details.get("final_fields", {})

            logging.info(f"Starting related data retrieval from '{start_entity_label}'")
            start_node = self.find_entity_node(start_entity_label)
            if not start_node or not start_node.get("collection_name"):
                logging.error(f"Start entity '{start_entity_label}' metadata missing.")
                print(f"❌ Cannot start traversal from '{start_entity_label}'.")
                return None
            start_collection_name = start_node["collection_name"]
            start_collection = self._get_data_collection(start_collection_name)
            start_node_id = start_node["_id"]

            # --- 1. Fetch Initial Data ---
            start_proj_fields_requested = final_fields.get(start_entity_label)
            start_metadata_fields = self.get_entity_fields(start_node_id)
            start_projection = self._build_projection(start_proj_fields_requested, start_metadata_fields)
            link_fields_to_add = set()
            if relations:
                first_link_info = self._get_relationship_link_fields(start_node_id, relations[0])
                if first_link_info: link_fields_to_add.update(first_link_info['start_entity_link_fields'])
                else: logging.error(f"Cannot determine link fields for first relation: {relations[0]}"); return None
            for field in link_fields_to_add: start_projection[field] = 1
            if "_id" not in start_projection: start_projection["_id"] = 0

            logging.info(f"Fetching initial data from '{start_collection_name}' with filters: {start_filters}")
            current_data = list(start_collection.find(start_filters, start_projection))
            if not current_data: print(f"ℹ️ No starting data found in '{start_entity_label}'."); return []
            logging.info(f"Found {len(current_data)} starting records in '{start_entity_label}'.")
            processed_results = {start_entity_label: current_data}
            current_entity_label = start_entity_label
            current_entity_node_id = start_node_id

            # --- 2. Traverse Relationships ---
            # (Traversal logic remains the same - queries collections based on graph links)
            for i, rel_step in enumerate(relations):
                target_entity_label = rel_step.get("target_entity")
                logging.info(f"Traversing step {i+1}: {current_entity_label} --[{rel_step.get('relation', 'REFERENCES')}/{rel_step.get('direction', '?')}]--> {target_entity_label}")
                if current_entity_node_id is None: logging.warning(f"Cannot traverse from '{current_entity_label}'. Skipping."); processed_results[target_entity_label] = []; current_entity_label = target_entity_label; continue
                target_node = self.find_entity_node(target_entity_label)
                if not target_node or not target_node.get("collection_name"): logging.error(f"Target entity '{target_entity_label}' metadata missing."); processed_results[target_entity_label] = []; current_entity_label = target_entity_label; current_entity_node_id = None; continue
                target_collection_name = target_node["collection_name"]; target_collection = self._get_data_collection(target_collection_name); target_node_id = target_node["_id"]
                link_info = self._get_relationship_link_fields(current_entity_node_id, rel_step)
                if not link_info: logging.error(f"Could not determine link fields for relation: {rel_step}."); processed_results[target_entity_label] = []; current_entity_label = target_entity_label; current_entity_node_id = target_node_id; continue
                current_link_fields = link_info['start_entity_link_fields']; target_link_fields = link_info['target_entity_link_fields']; is_array_link = link_info.get('start_entity_link_is_array', False)
                link_values = set()
                if current_entity_label in processed_results and processed_results[current_entity_label]:
                    link_field = current_link_fields[0]
                    for doc in processed_results[current_entity_label]:
                        value = doc.get(link_field)
                        if value is not None:
                             if is_array_link and isinstance(value, list): link_values.update(v for v in value if v is not None)
                             elif not is_array_link: link_values.add(value)
                if not link_values: logging.warning(f"No linking values found in '{current_entity_label}' to traverse to '{target_entity_label}'."); processed_results[target_entity_label] = []; current_entity_label = target_entity_label; current_entity_node_id = target_node_id; continue
                target_link_field = target_link_fields[0]; target_filter = {target_link_field: {"$in": list(link_values)}}; logging.info(f"Querying '{target_collection_name}' using filter on '{target_link_field}': {len(link_values)} potential matches.")
                target_proj_fields_requested = final_fields.get(target_entity_label); target_metadata_fields = self.get_entity_fields(target_node_id); target_projection = self._build_projection(target_proj_fields_requested, target_metadata_fields)
                for field in target_link_fields: target_projection[field] = 1
                if i + 1 < len(relations):
                    next_rel_step = relations[i+1]; next_link_info = self._get_relationship_link_fields(target_node_id, next_rel_step)
                    if next_link_info:
                        for field in next_link_info['start_entity_link_fields']: target_projection[field] = 1
                if "_id" not in target_projection: target_projection["_id"] = 0
                target_data = list(target_collection.find(target_filter, target_projection)); logging.info(f"Found {len(target_data)} related records in '{target_entity_label}'.")
                processed_results[target_entity_label] = target_data; current_entity_label = target_entity_label; current_entity_node_id = target_node_id

            # --- 3. Combine Results ---
            logging.info("Joining results from traversal...")
            # (Join logic remains the same)
            if not processed_results or start_entity_label not in processed_results or not processed_results[start_entity_label]:
                 print(f"ℹ️ No data found after traversal steps.")
                 expected_headers = self._get_expected_headers(final_fields, relations, start_entity_label)
                 print(tabulate([], headers=sorted(list(expected_headers)), tablefmt="grid")); return []
            current_joined_results = []
            start_fields_scope = self._get_fields_scope(final_fields, start_entity_label, start_node_id)
            first_link_field = None; first_link_is_array = False
            if relations:
                 first_link_info_join = self._get_relationship_link_fields(start_node_id, relations[0])
                 if first_link_info_join: first_link_field = first_link_info_join['start_entity_link_fields'][0]; first_link_is_array = first_link_info_join.get('start_entity_link_is_array', False)
            for start_doc in processed_results[start_entity_label]:
                 record = {};
                 for f in start_fields_scope:
                      if f in start_doc: record[f"{start_entity_label}.{f}"] = start_doc[f]
                 if first_link_field and first_link_field in start_doc: record[f"{start_entity_label}.{first_link_field}"] = start_doc[first_link_field]
                 if record: current_joined_results.append(record)
            last_entity_label = start_entity_label; last_entity_node_id = start_node_id
            for i, rel_step in enumerate(relations):
                 target_entity_label = rel_step.get("target_entity"); target_data = processed_results.get(target_entity_label, [])
                 if not target_data: logging.warning(f"No join data for '{target_entity_label}'."); current_joined_results = []; break
                 logging.info(f"Joining {last_entity_label} ({len(current_joined_results)}) with {target_entity_label} ({len(target_data)})...")
                 target_node = self.find_entity_node(target_entity_label);
                 if not target_node: break
                 link_info_join = self._get_relationship_link_fields(last_entity_node_id, rel_step)
                 if not link_info_join: logging.error(f"Cannot resolve join link {last_entity_label} -> {target_entity_label}."); current_joined_results = []; break
                 current_link_field = link_info_join['start_entity_link_fields'][0]; current_link_field_prefixed = f"{last_entity_label}.{current_link_field}"; target_link_field = link_info_join['target_entity_link_fields'][0]; current_link_is_array = link_info_join.get('start_entity_link_is_array', False)
                 target_lookup = {};
                 for target_doc in target_data:
                      key = target_doc.get(target_link_field)
                      if key is not None:
                           if key not in target_lookup: target_lookup[key] = []
                           target_lookup[key].append(target_doc)
                 next_link_field = None
                 if i + 1 < len(relations):
                      next_link_info_join = self._get_relationship_link_fields(target_node["_id"], relations[i+1])
                      if next_link_info_join: next_link_field = next_link_info_join['start_entity_link_fields'][0]
                 next_joined_results = []; target_fields_scope = self._get_fields_scope(final_fields, target_entity_label, target_node["_id"])
                 for current_record in current_joined_results:
                      link_val_or_list = current_record.get(current_link_field_prefixed); matched_targets = []
                      if current_link_is_array and isinstance(link_val_or_list, list):
                           for link_val_item in link_val_or_list: matched_targets.extend(target_lookup.get(link_val_item, []))
                      elif not current_link_is_array and link_val_or_list is not None: matched_targets = target_lookup.get(link_val_or_list, [])
                      if matched_targets:
                           for target_doc in matched_targets:
                                new_record = current_record.copy()
                                for f in target_fields_scope:
                                     if f in target_doc: new_record[f"{target_entity_label}.{f}"] = target_doc.get(f)
                                if next_link_field and next_link_field in target_doc: new_record[f"{target_entity_label}.{next_link_field}"] = target_doc.get(next_link_field)
                                next_joined_results.append(new_record)
                 current_joined_results = next_joined_results; last_entity_label = target_entity_label; last_entity_node_id = target_node["_id"]
                 if not current_joined_results: logging.warning(f"Join produced no results after '{target_entity_label}'."); break
            final_results = current_joined_results

            # --- 4. Format and Display Results ---
            requested_prefixed_headers = self._get_expected_headers(final_fields, relations, start_entity_label)
            final_results_cleaned = []
            for record in final_results:
                cleaned_record = {}
                # Ensure keys are present even if value is None
                for header in requested_prefixed_headers: cleaned_record[header] = record.get(header)
                if any(v is not None for v in cleaned_record.values()): final_results_cleaned.append(cleaned_record)

            print(f"\n📄 Combined Data from Traversal ({len(final_results_cleaned)} results):\n")
            if not final_results_cleaned:
                 print(tabulate([], headers=sorted(list(requested_prefixed_headers)), tablefmt="grid"))
            else:
                 try:
                     # **FIX**: Use headers="keys" for list of dicts input to tabulate
                     print(tabulate(final_results_cleaned, headers="keys", tablefmt="grid", missingval="N/A"))
                 except Exception as tab_e:
                     logging.error(f"Tabulate failed: {tab_e}. Fallback print.", exc_info=True)
                     print("--- Error Displaying Table, Raw Results ---")
                     print(json.dumps(final_results_cleaned, indent=2, default=str))
                     print("--- End Raw Results ---")

            return final_results_cleaned

        except Exception as e:
            logging.exception(f"Error in _retrieve_related_data starting from '{query_details.get('start_entity')}': {e}")
            print(f"❌ Error processing related data retrieval.")
            return None


    # --- Helper Methods ---
    # (No changes needed in helpers: _find_primary_key..., _get_relationship_link_fields,
    #  _build_projection, _get_fields_scope, _get_expected_headers)

    def _find_primary_key_field_by_label(self, entity_label: str) -> str | None:
        """Finds the label of the primary key field for an entity node using its label."""
        entity_node = self.find_entity_node(entity_label)
        if not entity_node: return None
        field_edges = list(self.edges_collection.find({"source": entity_node["_id"], "relation": "HAS_FIELD"}))
        if not field_edges: return None
        field_node_ids = [edge["target"] for edge in field_edges]
        pk_node = self.nodes_collection.find_one({
            "_id": {"$in": field_node_ids},
            "properties.is_primary_key": True
            })
        return pk_node.get("label") if pk_node else None

    def _find_primary_key_field(self, entity_node_id: str) -> str | None:
        """Find the label of the primary key field for an entity node using its ID."""
        node = self.nodes_collection.find_one({"_id": entity_node_id})
        return self._find_primary_key_field_by_label(node["label"]) if node else None

    def _get_relationship_link_fields(self, start_node_id: str | None, rel_info: dict) -> dict | None:
        """Determines linking fields based on 'REFERENCES' edge and 'on_field' property."""
        if start_node_id is None: return None
        target_entity_label = rel_info.get("target_entity")
        relation_type = rel_info.get("relation", "REFERENCES")
        direction = rel_info.get("direction")
        if not direction: logging.error(f"Direction missing: {rel_info}"); return None
        target_node = self.find_entity_node(target_entity_label)
        if not target_node: logging.error(f"Target node not found: {target_entity_label}"); return None
        target_node_id = target_node["_id"]
        edge = self.edges_collection.find_one(
            {"relation": relation_type, "$or": [
                {"source": start_node_id, "target": target_node_id},
                {"source": target_node_id, "target": start_node_id}
            ]}
        )
        if not edge: logging.error(f"Edge '{relation_type}' not found between {start_node_id} and {target_node_id}."); return None
        edge_props = edge.get("properties", {}); fk_field_name = edge_props.get("on_field")
        if not fk_field_name: logging.error(f"Edge {edge.get('_id')} missing 'on_field'."); return None
        fk_holder_node_id = edge["source"]; pk_holder_node_id = edge["target"]
        pk_field_name = self._find_primary_key_field(pk_holder_node_id)
        if not pk_field_name: logging.error(f"Cannot find PK for node {pk_holder_node_id}."); return None
        # Find the field node for the fk to check its type
        fk_field_node = self.nodes_collection.find_one({"label": fk_field_name, "_id": {"$regex": f"^{fk_holder_node_id}_"}})
        is_array = False
        if fk_field_node and fk_field_node.get("properties", {}).get("data_type", "").startswith("ARRAY"): is_array = True
        if direction == "out":
            if start_node_id == fk_holder_node_id: return {'start_entity_link_fields': [fk_field_name], 'target_entity_link_fields': [pk_field_name], 'start_entity_link_is_array': is_array}
            else: return {'start_entity_link_fields': [pk_field_name], 'target_entity_link_fields': [fk_field_name], 'start_entity_link_is_array': False}
        elif direction == "in":
            if start_node_id == pk_holder_node_id: return {'start_entity_link_fields': [pk_field_name], 'target_entity_link_fields': [fk_field_name], 'start_entity_link_is_array': False}
            else: return {'start_entity_link_fields': [fk_field_name], 'target_entity_link_fields': [pk_field_name], 'start_entity_link_is_array': is_array}
        return None

    def _build_projection(self, requested_fields: list | None, all_schema_fields: list) -> dict:
        """Builds MongoDB projection. Excludes _id unless requested."""
        projection = {}; fields_to_project = requested_fields if requested_fields is not None else all_schema_fields
        if not fields_to_project: projection = {"_id": 0}
        else:
            projection = {field: 1 for field in fields_to_project}
            if "_id" not in fields_to_project: projection["_id"] = 0
            else: projection["_id"] = 1
        return projection

    def _get_fields_scope(self, final_fields_request: dict, entity_label: str, entity_node_id: str) -> list:
        """Gets fields to include: requested or all."""
        requested = final_fields_request.get(entity_label)
        if requested is not None: return requested # Can be empty list []
        else: return self.get_entity_fields(entity_node_id) # None means all

    def _get_expected_headers(self, final_fields_request: dict, relations: list, start_entity_label: str) -> set:
        """Calculates expected prefixed headers for final output."""
        headers = set(); entities_in_query = {start_entity_label} | {rel['target_entity'] for rel in relations}
        for entity_label in entities_in_query:
            fields_scope = []; node = self.find_entity_node(entity_label)
            if not node: continue; node_id = node['_id']
            if entity_label in final_fields_request:
                if final_fields_request[entity_label] is not None: fields_scope = final_fields_request[entity_label]
                else: fields_scope = self.get_entity_fields(node_id) # None means all
            elif not final_fields_request: fields_scope = self.get_entity_fields(node_id) # Empty request dict implies all
            headers.update(f"{entity_label}.{f}" for f in fields_scope)
        return headers