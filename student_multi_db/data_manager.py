import logging
from pymongo.database import Database
from pymongo.errors import CollectionInvalid, OperationFailure
from bson import ObjectId # If using default Mongo ObjectIds for data
from tabulate import tabulate
import json # For alternative output

import config
from data_models import SampleData, get_mongo_validation_schemas
from graph_models import GraphNode # For type hinting if needed

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s') # DEBUG level

class DataManager:
    """Handles data insertion and retrieval across multiple collections using metadata."""

    def __init__(self, db: Database):
        self.db = db
        self.nodes_collection = db[config.NODES_COLLECTION]
        self.edges_collection = db[config.EDGES_COLLECTION]

    def _get_data_collection(self, collection_name: str):
        """Gets a handle to a data collection."""
        return self.db[collection_name]

    def setup_collections_with_validation(self):
        """Creates collections (if they don't exist) and applies validation."""
        logging.info("Setting up data collections with validation...")
        validation_schemas = get_mongo_validation_schemas()
        collection_names = list(validation_schemas.keys())

        existing_collections = self.db.list_collection_names()

        for name in collection_names:
            schema = validation_schemas[name]
            if name not in existing_collections:
                try:
                    self.db.create_collection(name, validator=schema)
                    logging.info(f"  Created collection '{name}' with validation.")
                except CollectionInvalid as e:
                     logging.error(f"  Failed to create collection '{name}': {e}")
                except OperationFailure as e:
                    logging.error(f"  OperationFailure while creating collection '{name}' with validator: {e}")
            else:
                try:
                    # Attempt to update validation - may fail if incompatible with existing data
                    self.db.command('collMod', name, validator=schema)
                    logging.info(f"  Attempted to update validation for existing collection '{name}'.")
                except OperationFailure as e:
                     logging.warning(f"  Could not update validation for collection '{name}': {e}. Schema might be incompatible.")

            pk_field = self._find_primary_key_field_by_label(name)
            if pk_field:
                try:
                    # Ensure index exists, check existing indexes first
                    index_info = self.db[name].index_information()
                    pk_index_name = f"{pk_field}_1" # Default index name format
                    # More robust check needed if index name differs
                    if pk_index_name not in index_info:
                        self.db[name].create_index(pk_field, unique=True, name=pk_index_name)
                        logging.info(f"  Created unique index on PK '{pk_field}' for '{name}'.")
                    else:
                         # Check if existing index is unique
                         is_unique = index_info.get(pk_index_name, {}).get('unique', False)
                         if is_unique:
                              logging.info(f"  Unique index on PK '{pk_field}' already exists for '{name}'.")
                         else:
                              logging.warning(f"  Non-unique index on PK '{pk_field}' already exists for '{name}'. Cannot enforce uniqueness.")
                except Exception as idx_e:
                    logging.warning(f"  Could not ensure unique index on {pk_field} for {name}: {idx_e}")


    def insert_sample_data(self):
        """Inserts all sample data into the respective collections."""
        logging.info("Inserting sample data...")
        data_map = {
            "Students": SampleData.get_students_data(),
            "Courses": SampleData.get_courses_data(),
            "Enrollments": SampleData.get_enrollments_data(),
            "HackathonParticipations": SampleData.get_hackathon_participations_data(),
            "SportsParticipations": SampleData.get_sports_participations_data(),
            "StudentClubs": SampleData.get_student_clubs_data(),
        }

        for collection_name, data_func in data_map.items():
            # Check if collection exists before counting/inserting
            if collection_name in self.db.list_collection_names():
                collection = self._get_data_collection(collection_name)
                if collection.count_documents({}) == 0:
                    try:
                        data = data_func
                        if data:
                            collection.insert_many(data)
                            logging.info(f"  Inserted {len(data)} documents into '{collection_name}'.")
                        else:
                            logging.info(f"  No sample data provided for '{collection_name}'.")
                    except Exception as e:
                        logging.error(f"  Error inserting data into '{collection_name}': {e}")
                else:
                    logging.info(f"  Collection '{collection_name}' already contains data, skipping sample data insertion.")
            else:
                 logging.warning(f"  Collection '{collection_name}' does not exist. Skipping sample data insertion.")


    def insert_single_record(self, entity_label: str, record: dict):
        """Inserts a single record into the appropriate collection."""
        logging.info(f"Attempting to insert record into '{entity_label}'...")
        entity_node_doc = self.nodes_collection.find_one({"label": entity_label, "type": {"$in": ["table", "collection"]}})
        if not entity_node_doc:
            logging.error(f"  Entity '{entity_label}' not found in metadata graph.")
            return False
        collection_name = entity_node_doc.get("collection_name")
        if not collection_name:
             logging.error(f"  Metadata node for '{entity_label}' does not specify a collection name.")
             return False
        data_collection = self._get_data_collection(collection_name)
        try:
            result = data_collection.insert_one(record)
            logging.info(f"  Successfully inserted record into '{collection_name}' with id: {result.inserted_id}")
            return True
        except Exception as e:
            logging.error(f"  Error inserting record into '{collection_name}': {e}")
            return False

    def find_entity_node(self, label: str) -> dict | None:
        """Finds an entity node (table/collection) by its label."""
        return self.nodes_collection.find_one({"label": label, "type": {"$in": ["table", "collection"]}})

    def get_entity_fields(self, entity_node_id: str) -> list[str]:
        """Gets the labels of fields/columns belonging to an entity node."""
        field_edges = self.edges_collection.find({
            "source": entity_node_id,
            "relation": {"$in": ["HAS_COLUMN", "HAS_FIELD"]}
        })
        field_node_ids = [edge["target"] for edge in field_edges]
        if not field_node_ids: return []
        # Sort fields alphabetically for consistent header order
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
             # Catch unexpected errors in the retrieval logic itself
             logging.exception(f"An unexpected error occurred during retrieve_data action '{action}': {e}")
             print(f"❌ An unexpected error occurred during retrieval: {e}")
             return None


    def _retrieve_single_entity(self, query_details: dict):
        """Handles simple retrieval from a single entity."""
        try:
            entity_label = query_details.get("entity")
            requested_fields = query_details.get("fields")
            filters = query_details.get("filters", {})

            entity_node = self.find_entity_node(entity_label)
            if not entity_node:
                logging.error(f"Entity '{entity_label}' not found.")
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
                empty_headers = requested_fields if requested_fields is not None else metadata_headers
                print(tabulate([], headers=empty_headers, tablefmt="grid"))
            else:
                # Use "keys" - generally reliable for single entity results
                print(tabulate(results, headers="keys", tablefmt="grid"))

            return results
        except Exception as e:
            logging.exception(f"Error in _retrieve_single_entity for '{query_details.get('entity')}': {e}")
            print(f"❌ Error displaying data for {query_details.get('entity')}.")
            return None # Indicate failure

    def _retrieve_related_data(self, query_details: dict):
        """Handles retrieval involving graph traversal and joins (with fallback display)."""
        try:
            start_entity_label = query_details.get("start_entity")
            start_filters = query_details.get("start_filters", {})
            relations = query_details.get("relations", [])
            final_fields = query_details.get("final_fields", {})

            start_node = self.find_entity_node(start_entity_label)
            if not start_node or not start_node.get("collection_name"):
                logging.error(f"Start entity '{start_entity_label}' not found/misconfigured.")
                print(f"❌ Cannot start traversal from '{start_entity_label}'.")
                return None
            start_collection = self._get_data_collection(start_node["collection_name"])
            start_node_id = start_node["_id"]

            # --- Fetch initial data ---
            start_proj_fields = final_fields.get(start_entity_label)
            start_metadata_fields = self.get_entity_fields(start_node_id)
            start_projection = self._build_projection(start_proj_fields, start_metadata_fields)
            if relations:
                first_link_info = self._get_relationship_link_fields(start_node_id, relations[0])
                if first_link_info:
                    for field in first_link_info['start_entity_link_fields']: start_projection[field] = 1
                else:
                    logging.error(f"Cannot determine link fields for first relation: {relations[0]}")
                    print(f"❌ Cannot determine how {start_entity_label} relates for first step.")
                    return None

            logging.info(f"Fetching initial data from {start_node['collection_name']} with filters: {start_filters}")
            logging.debug(f"DEBUG: Starting projection: {start_projection}")
            current_data = list(start_collection.find(start_filters, start_projection))
            if not current_data:
                print(f"ℹ️ No starting data found in '{start_entity_label}' matching filters.")
                return []
            logging.info(f"Found {len(current_data)} starting records.")
            processed_data = {start_entity_label: current_data}
            current_entity_node_id = start_node_id
            current_entity_label = start_entity_label

            # --- Traverse relationships ---
            for i, rel in enumerate(relations):
                target_entity_label = rel.get("target_entity")
                logging.info(f"Traversing step {i+1}: {current_entity_label} {rel.get('direction', 'out')} {rel.get('relation')} {target_entity_label}")

                target_node = self.find_entity_node(target_entity_label)
                if not target_node or not target_node.get("collection_name"):
                    logging.error(f"Target entity '{target_entity_label}' not found/misconfigured.")
                    processed_data[target_entity_label] = []
                    current_entity_label = target_entity_label
                    current_entity_node_id = None
                    continue

                target_collection = self._get_data_collection(target_node["collection_name"])
                target_node_id = target_node["_id"]

                link_info = self._get_relationship_link_fields(current_entity_node_id, rel)
                if not link_info:
                    logging.error(f"Could not determine link fields for relation: {rel} from {current_entity_label}")
                    processed_data[target_entity_label] = []
                    current_entity_label = target_entity_label
                    current_entity_node_id = target_node_id
                    continue

                current_link_fields = link_info['start_entity_link_fields']
                target_link_fields = link_info['target_entity_link_fields']
                link_values = set()
                if current_entity_label in processed_data and processed_data[current_entity_label]:
                    for doc in processed_data[current_entity_label]:
                        if current_link_fields[0] in doc and doc[current_link_fields[0]] is not None:
                            link_values.add(doc[current_link_fields[0]])

                if not link_values:
                    logging.warning(f"No linking values found in '{current_entity_label}' to traverse to '{target_entity_label}'.")
                    processed_data[target_entity_label] = []
                    current_entity_label = target_entity_label
                    current_entity_node_id = target_node_id
                    continue

                target_filter = {target_link_fields[0]: {"$in": list(link_values)}}
                logging.info(f"Querying {target_node['collection_name']} with filter: {target_filter}")

                target_proj_fields = final_fields.get(target_entity_label)
                target_metadata_fields = self.get_entity_fields(target_node_id)
                target_projection = self._build_projection(target_proj_fields, target_metadata_fields)
                for field in target_link_fields: target_projection[field] = 1
                if i + 1 < len(relations):
                    next_link_info = self._get_relationship_link_fields(target_node_id, relations[i+1])
                    if next_link_info:
                        for field in next_link_info['start_entity_link_fields']: target_projection[field] = 1

                logging.debug(f"DEBUG: Target projection for {target_entity_label}: {target_projection}")
                target_data = list(target_collection.find(target_filter, target_projection))
                logging.info(f"Found {len(target_data)} related records in '{target_entity_label}'.")

                processed_data[target_entity_label] = target_data
                current_entity_label = target_entity_label
                current_entity_node_id = target_node_id

            # --- Combine and Format Results ---
            logging.info("Joining results...")
            expected_headers = set() # Expected prefixed headers from final_fields

            if not processed_data or start_entity_label not in processed_data or not processed_data[start_entity_label]:
                print(f"ℹ️ No data found to join.")
                if start_entity_label in final_fields: expected_headers.update([f"{start_entity_label}.{f}" for f in final_fields[start_entity_label]])
                for rel in relations:
                    target_label = rel.get("target_entity")
                    if target_label in final_fields: expected_headers.update([f"{target_label}.{f}" for f in final_fields[target_label]])
                print(tabulate([], headers=sorted(list(expected_headers)), tablefmt="grid"))
                return []

            # Initialize join results
            current_joined_results = []
            start_fields_to_keep = final_fields.get(start_entity_label)
            start_all_fields = self.get_entity_fields(start_node_id)
            fields_scope_start = start_fields_to_keep if start_fields_to_keep is not None else start_all_fields

            for doc in processed_data[start_entity_label]:
                record = {}
                for f in fields_scope_start:
                    if f in doc:
                        key = f"{start_entity_label}.{f}"
                        record[key] = doc.get(f)
                        expected_headers.add(key)
                if relations:
                    first_link_info = self._get_relationship_link_fields(start_node_id, relations[0])
                    if first_link_info:
                        for link_f in first_link_info['start_entity_link_fields']:
                            prefixed_link_f = f"{start_entity_label}.{link_f}"
                            if link_f in doc: record[prefixed_link_f] = doc.get(link_f)
                current_joined_results.append(record)

            # Iteratively join
            last_entity_label = start_entity_label
            last_entity_node_id = start_node_id
            for i, rel in enumerate(relations):
                target_entity_label = rel.get("target_entity")
                if target_entity_label not in processed_data or not processed_data[target_entity_label]:
                    logging.warning(f"Data for '{target_entity_label}' is empty. Join yields empty result.")
                    current_joined_results = []
                    break

                logging.info(f"Joining {last_entity_label} with {target_entity_label}...")
                target_data = processed_data[target_entity_label]
                target_node = self.find_entity_node(target_entity_label)
                if not target_node: break

                link_info = self._get_relationship_link_fields(last_entity_node_id, rel)
                if not link_info:
                    logging.error(f"Cannot join {last_entity_label} and {target_entity_label}: Link fields unresolved.")
                    current_joined_results = []
                    break

                current_link_field_prefixed = f"{last_entity_label}.{link_info['start_entity_link_fields'][0]}"
                target_link_field = link_info['target_entity_link_fields'][0]
                target_lookup = {}
                for target_doc in target_data:
                    key = target_doc.get(target_link_field)
                    if key is not None:
                        if key not in target_lookup: target_lookup[key] = []
                        target_lookup[key].append(target_doc)

                next_joined_results = []
                target_fields_to_keep = final_fields.get(target_entity_label)
                target_all_fields = self.get_entity_fields(target_node["_id"])
                fields_scope_target = target_fields_to_keep if target_fields_to_keep is not None else target_all_fields

                for current_record in current_joined_results:
                    link_val = current_record.get(current_link_field_prefixed)
                    matched_targets = target_lookup.get(link_val, [])
                    if matched_targets:
                        for target_doc in matched_targets:
                            new_record = current_record.copy()
                            for f in fields_scope_target:
                                if f in target_doc:
                                    key = f"{target_entity_label}.{f}"
                                    new_record[key] = target_doc.get(f)
                                    expected_headers.add(key)
                            if i + 1 < len(relations):
                                next_link_info = self._get_relationship_link_fields(target_node["_id"], relations[i+1])
                                if next_link_info:
                                    for next_link_f in next_link_info['start_entity_link_fields']:
                                        prefixed_next_link_f = f"{target_entity_label}.{next_link_f}"
                                        if next_link_f in target_doc: new_record[prefixed_next_link_f] = target_doc.get(next_link_f)
                            next_joined_results.append(new_record)

                current_joined_results = next_joined_results
                last_entity_label = target_entity_label
                last_entity_node_id = target_node["_id"]
                if not current_joined_results:
                    logging.warning(f"Join produced no results after including {target_entity_label}.")
                    break

            final_results = current_joined_results

            # Clean results: Remove internal linking fields not in final_fields
            final_results_cleaned = []
            requested_prefixed_headers = set()
            for entity, fields in final_fields.items():
                if fields is None:
                    entity_node_temp = self.find_entity_node(entity)
                    if entity_node_temp:
                        all_ent_fields = self.get_entity_fields(entity_node_temp['_id'])
                        requested_prefixed_headers.update([f"{entity}.{f}" for f in all_ent_fields])
                else:
                    requested_prefixed_headers.update([f"{entity}.{f}" for f in fields])

            for record in final_results:
                cleaned_record = {k: v for k, v in record.items() if k in requested_prefixed_headers}
                if cleaned_record:
                    final_results_cleaned.append(cleaned_record)

            # --- Display Results with Fallback ---
            print(f"\n📄 Combined Data from Traversal:\n")
            if not final_results_cleaned:
                # If empty after cleaning, use the requested headers for empty table
                print(tabulate([], headers=sorted(list(requested_prefixed_headers)), tablefmt="grid"))
            else:
                try:
                    # Try tabulate with headers="keys" first for joined data
                    print(tabulate(final_results_cleaned, headers="keys", tablefmt="grid"))
                except Exception as tab_e:
                    # Fallback to simple JSON-like printing if tabulate still fails
                    logging.error(f"Tabulate failed for combined results (even with 'keys'): {tab_e}")
                    print("--- Error Displaying Table, Showing Raw Results ---")
                    # Use json dumps for potentially better handling of complex types
                    try:
                        print(json.dumps(final_results_cleaned, indent=2, default=str)) # Use default=str for non-serializable types like datetime
                    except Exception as json_e:
                         logging.error(f"JSON dump also failed: {json_e}")
                         # Final fallback: simple print
                         for item in final_results_cleaned:
                              print(item)
                    print("--- End Raw Results ---")


            return final_results_cleaned

        except Exception as e:
            logging.exception(f"Error in _retrieve_related_data for '{query_details.get('start_entity')}': {e}")
            print(f"❌ Error processing related data retrieval for {query_details.get('start_entity')}.")
            return None # Indicate failure

    # --- Helper methods (_find_primary_key_field, etc.) ---
    # Keep the implementations from the previous successful response

    def _find_primary_key_field(self, entity_node_id: str) -> str | None:
        """Find the label of the primary key field for an entity node using its ID."""
        field_edges = list(self.edges_collection.find({
            "source": entity_node_id, "relation": {"$in": ["HAS_COLUMN", "HAS_FIELD"]}
        }))
        if not field_edges: return None
        field_node_ids = [edge["target"] for edge in field_edges]
        pk_node = self.nodes_collection.find_one({
            "_id": {"$in": field_node_ids}, "properties.is_primary_key": True
            })
        return pk_node.get("label") if pk_node else None

    def _find_primary_key_field_by_label(self, entity_label: str) -> str | None:
        """Find the label of the primary key field for an entity node using its label."""
        entity_node = self.find_entity_node(entity_label)
        return self._find_primary_key_field(entity_node["_id"]) if entity_node else None

    def _find_referencing_foreign_key(self, entity_node_id: str, referenced_entity_label: str) -> str | None:
         """Find the label of the foreign key field in entity A that references entity B."""
         field_edges = list(self.edges_collection.find({
            "source": entity_node_id, "relation": {"$in": ["HAS_COLUMN", "HAS_FIELD"]}
         }))
         if not field_edges: return None
         field_node_ids = [edge["target"] for edge in field_edges]
         fk_node = self.nodes_collection.find_one({
             "_id": {"$in": field_node_ids},
             "properties.is_foreign_key": True,
             "properties.references": referenced_entity_label
         })
         return fk_node.get("label") if fk_node else None

    def _get_relationship_link_fields(self, start_node_id: str | None, rel_info: dict) -> dict | None:
        """Determines link fields for a traversal step."""
        if start_node_id is None:
            logging.error("_get_relationship_link_fields called with start_node_id=None")
            return None

        target_entity_label = rel_info.get("target_entity")
        relation_type = rel_info.get("relation")
        direction = rel_info.get("direction", "out")
        target_node = self.find_entity_node(target_entity_label)
        if not target_node:
            logging.error(f"Cannot find target entity node for label: {target_entity_label}")
            return None
        target_node_id = target_node["_id"]

        # Find the edge defined in the graph (assume REFERENCES goes FROM FK holder TO PK holder)
        query1 = {"relation": relation_type, "source": start_node_id, "target": target_node_id}
        query2 = {"relation": relation_type, "source": target_node_id, "target": start_node_id}
        edge = self.edges_collection.find_one(query1) or self.edges_collection.find_one(query2)

        if not edge:
            logging.error(f"Metadata edge not found for relation: {rel_info} between nodes {start_node_id} and {target_node_id}")
            return None

        edge_props = edge.get("properties", {})
        on_field = edge_props.get("on_field")

        if on_field and relation_type == "REFERENCES":
            fk_holder_node_id = edge["source"]
            pk_holder_node_id = edge["target"]
            pk_field = self._find_primary_key_field(pk_holder_node_id)
            if not pk_field:
                logging.error(f"Cannot resolve relationship: PK field missing for node {pk_holder_node_id} referenced by edge {edge['_id']}")
                return None

            if direction == "out": # e.g., Enrollment -> Student (Start=Enrollment)
                return {'start_entity_link_fields': [on_field], 'target_entity_link_fields': [pk_field]}
            else: # direction == "in" # e.g., Student <- Enrollment (Start=Student)
                return {'start_entity_link_fields': [pk_field], 'target_entity_link_fields': [on_field]}
        else:
            logging.error(f"Cannot determine link fields for relation {rel_info}. Edge found, but 'on_field' property missing or type not REFERENCES.")
            return None


    def _build_projection(self, requested_fields: list | None, all_schema_fields: list) -> dict:
        """Builds a MongoDB projection dictionary."""
        projection = {}
        fields_to_project = requested_fields if requested_fields is not None else all_schema_fields

        if not fields_to_project:
            projection = {"_id": 0} # Exclude ID if no fields known/requested
        else:
            projection = {field: 1 for field in fields_to_project}
            # Decide on _id inclusion: include ONLY if explicitly requested
            if "_id" not in fields_to_project:
                 projection["_id"] = 0
            else:
                 projection["_id"] = 1 # Keep it if it was in the list

        return projection