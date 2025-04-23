# main.py
import logging
import os
import json
from pymongo import MongoClient
# Use python-dateutil for flexible parsing if needed, install via pip
# import dateutil.parser

import config
from metadata_manager import MetadataManager
from data_manager import DataManager
from query_parser import parse_query # Still conceptual

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')


# Define a base directory for data files
BASE_DATA_DIR = "/Users/dhruvasharma/Documents/SEM2/DM/MULTI-DB/multidb_university_unified/sample_data"

# Automatically map file extensions to their common types
FILE_TYPE_MAP = {
    '.csv': 'table',
    '.xml': 'xml_structure',
    '.json': 'json_objects'
}

SCHEMA_FILE = os.path.join(BASE_DATA_DIR, "schema_file.json")
QUERY_FILE = os.path.join(BASE_DATA_DIR, "queries.json")

def resolve_file_path(relative_path):
    """Resolve a relative file path to an absolute path based on the base directory."""
    return os.path.join(BASE_DATA_DIR, relative_path)

def load_schemas():
    """Load schemas from the external schema file."""
    if not os.path.exists(SCHEMA_FILE):
        logging.error(f"Schema file '{SCHEMA_FILE}' not found.")
        return {}
    try:
        with open(SCHEMA_FILE, "r") as file:
            schemas = json.load(file)
            
            # Process schemas into a structured format
            result = {}
            for source in schemas:
                for entity in source.get("entities", []):
                    entity_label = entity.get("label")
                    if entity_label:
                        # Add source metadata to entity
                        entity["source_type"] = source.get("source_type")
                        entity["source_name"] = source.get("source_name")
                        
                        # Add file path based on entity label if not present
                        if "file_path" not in entity:
                            for ext in ['.csv', '.xml', '.json']:
                                potential_path = os.path.join(BASE_DATA_DIR, f"{entity_label.lower()}{ext}")
                                if os.path.exists(potential_path):
                                    entity["file_path"] = f"{entity_label.lower()}{ext}"
                                    if "type" not in entity:
                                        entity["type"] = FILE_TYPE_MAP.get(ext)
                                    break
                            
                        result[entity_label] = entity
            
            return result
    except Exception as e:
        logging.error(f"Failed to load schemas from '{SCHEMA_FILE}': {e}")
        return {}

def find_schema_for_entity(entity_label):
    """Retrieve the schema for a given entity label."""
    return SCHEMAS.get(entity_label)

# Define file mappings for entities
ENTITY_FILE_MAPPING = {
    "Students": "students.csv",
    "Courses": "courses.csv",
    "Enrollments": "enrollments.csv",
    "StudentClubs": "student_clubs.xml",
    "SportsParticipations": "sports.json",
    "HackathonParticipations": "hackathons.json"
}

def flush_collection(mongo_db, collection_name):
    """Flush (clear) the specified MongoDB collection."""
    try:
        mongo_db[collection_name].delete_many({})
        logging.info(f"Flushed collection: {collection_name}")
    except Exception as e:
        logging.error(f"Failed to flush collection '{collection_name}': {e}")

def run_setup_and_ingestion(metadata_mgr: MetadataManager, data_mgr: DataManager, force_reingest=False):
    """Runs the metadata build, collection setup, and data ingestion."""
    logging.info("--- Running Setup and Ingestion ---")

    # Convert SCHEMAS dictionary to the format expected by metadata_mgr.build_graph_from_schema
    source_schemas = {}
    for entity_label, entity in SCHEMAS.items():
        source_key = (entity.get("source_name", "unknown"), entity.get("source_type", "unknown"))
        if source_key not in source_schemas:
            source_schemas[source_key] = {
                "source_name": source_key[0],
                "source_type": source_key[1],
                "entities": []
            }
        source_schemas[source_key]["entities"].append(entity)
    
    schema_list = list(source_schemas.values())
    
    # 1. Build Metadata Graph
    logging.info("Step 1: Building/Rebuilding Metadata Graph...")
    metadata_mgr.build_graph_from_schema(schema_list)

    # 2. Setup MongoDB Collections
    logging.info("Step 2: Setting up MongoDB collections...")
    collections_to_flush = [
        "Students",
        "Courses",
        "Enrollments",
        "HackathonParticipations",
        "SportsParticipations",
        "StudentClubs"
    ]
    for collection in collections_to_flush:
        flush_collection(data_mgr.db, collection)
    data_mgr.setup_collections_with_validation()

    # 3. Register parsers
    data_mgr.register_parser("table", data_mgr._parse_csv)
    data_mgr.register_parser("xml_structure", data_mgr._parse_xml)
    data_mgr.register_parser("json_objects", data_mgr._parse_json)

    # 4. Ingest Data from Files
    logging.info("Step 3: Ingesting data from source files...")
    
    for entity_label, schema in SCHEMAS.items():
        # Use direct mapping from entity to filename
        if entity_label in ENTITY_FILE_MAPPING:
            file_name = ENTITY_FILE_MAPPING[entity_label]
            file_path = os.path.join(BASE_DATA_DIR, file_name)
            
            if not os.path.exists(file_path):
                logging.warning(f"File for entity '{entity_label}' not found at '{file_path}'. Skipping ingestion.")
                continue
            
            try:
                data_mgr.ingest_file(entity_label, file_path)
                logging.info(f"Successfully ingested data for entity '{entity_label}' from file: {file_path}")
            except Exception as e:
                logging.error(f"Failed to ingest data for entity '{entity_label}': {e}")
        else:
            logging.warning(f"No file mapping found for entity '{entity_label}'. Skipping ingestion.")
    
    logging.info("--- Setup and Ingestion Complete ---")

def run():
    """Main execution function."""
    # --- 1. Database Connection ---
    try:
        client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client[config.DATABASE_NAME]
        logging.info(f"Successfully connected to MongoDB: {config.DATABASE_NAME}")
    except Exception as e:
        logging.error(f"Failed to connect to MongoDB: {e}")
        return

    # --- 2. Initialize Managers ---
    metadata_mgr = MetadataManager(db)
    data_mgr = DataManager(db)

    # --- 3. Run Setup and Ingestion ---
    run_setup_and_ingestion(metadata_mgr, data_mgr, force_reingest=False)

    # --- 4. Process Queries from JSON File ---
    print("\n" + "="*10 + " RUNNING QUERIES " + "="*10)
    if not os.path.exists(QUERY_FILE):
        logging.error(f"Query file '{QUERY_FILE}' not found. Exiting.")
        return

    try:
        with open(QUERY_FILE, "r") as file:
            queries = json.load(file)
    except Exception as e:
        logging.error(f"Failed to load queries from '{QUERY_FILE}': {e}")
        return

    for i, query in enumerate(queries, start=1):
        print(f"\n--- Query {i}: {query.get('description', 'No Description')} ---")
        data_mgr.retrieve_data(query["query"])

    print("\n" + "="*10 + " QUERIES COMPLETE " + "="*10)

    # --- 5. Close Connection ---
    client.close()
    logging.info("MongoDB connection closed.")

if __name__ == "__main__":
    SCHEMAS = load_schemas()  # Load schemas dynamically
    run()