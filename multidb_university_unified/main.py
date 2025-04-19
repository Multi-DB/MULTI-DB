# main.py
import logging
import os
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

# --- Define Sample Data Files ---
SAMPLE_DATA_DIR = "sample_data"
# Map Logical Entity Label to File Name
SAMPLE_FILES = {
    "Students": "students.csv",
    "Courses": "courses.csv",
    "Enrollments": "enrollments.csv",
    "HackathonParticipations": "hackathons.json",
    "SportsParticipations": "sports.json",
    "StudentClubs": "student_clubs.xml",
}

def run_setup_and_ingestion(metadata_mgr: MetadataManager, data_mgr: DataManager, force_reingest=False):
    """Runs the metadata build, collection setup, and data ingestion."""
    logging.info("--- Running Setup and Ingestion ---")

    # 1. Build Metadata Graph
    logging.info("Step 1: Building/Rebuilding Metadata Graph...")
    metadata_mgr.build_graph_from_schema()

    # 2. Setup MongoDB Collections
    logging.info("Step 2: Setting up MongoDB collections...")
    data_mgr.setup_collections_with_validation()

    # 3. Ingest Data from Files
    logging.info("Step 3: Ingesting data from source files...")
    if not os.path.exists(SAMPLE_DATA_DIR):
        logging.warning(f"Sample data directory '{SAMPLE_DATA_DIR}' not found. Skipping ingestion.")
        return

    for entity_label, filename in SAMPLE_FILES.items():
        filepath = os.path.join(SAMPLE_DATA_DIR, filename)
        if os.path.exists(filepath):
             collection = data_mgr._get_data_collection(entity_label)
             # Ingest only if collection is empty OR force_reingest is True
             if force_reingest or collection.count_documents({}) == 0:
                 if force_reingest and collection.count_documents({}) > 0:
                      logging.warning(f"Forcing re-ingestion for '{entity_label}'. Clearing existing data...")
                      collection.delete_many({})
                 data_mgr.ingest_and_process_file(entity_label, filepath)
             else:
                 logging.info(f"Skipping ingestion for '{entity_label}', collection already contains data.")
        else:
            logging.warning(f"Sample file not found for entity '{entity_label}': {filepath}. Skipping ingestion.")

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
    # Set force_reingest=True ONLY if you want to clear collections and reload data
    run_setup_and_ingestion(metadata_mgr, data_mgr, force_reingest=False)


    # --- 4. Retrieval Examples ---
    print("\n" + "="*10 + " RUNNING UNIVERSITY QUERIES " + "="*10)

    # Example 1: Get data from CSV source (Students) with filter
    print("\n--- Query 1: Get Computer Science Students ---")
    query1 = {"action": "get_entity", "entity": "Students", "filters": {"Major": "Computer Science"}}
    data_mgr.retrieve_data(query1)

    # Example 2: Get data from JSON source (Hackathons) with specific fields
    print("\n--- Query 2: Get Hackathon Names and Project Titles ---")
    query2 = {"action": "get_entity", "entity": "HackathonParticipations", "fields": ["HackathonName", "ProjectTitle", "ActivityID"]}
    data_mgr.retrieve_data(query2)

    # Example 3: Get data from XML source (Clubs)
    print("\n--- Query 3: Get all Student Club Memberships ---")
    query3 = {"action": "get_entity", "entity": "StudentClubs"} # Request all default fields
    data_mgr.retrieve_data(query3)

    # Example 4: Cross-Source Query (Students(CSV) -> Enrollments(CSV) -> Courses(CSV))
    print("\n--- Query 4: Find Courses taken by Student John Smith (ID 1001) ---")
    query4 = {
        "action": "get_related",
        "start_entity": "Students",
        "start_filters": {"StudentID": 1001},
        "relations": [
            # Students (PK: StudentID) <- Enrollments (FK: StudentID)
            {"relation": "REFERENCES", "direction": "in", "target_entity": "Enrollments"},
            # Enrollments (FK: CourseID) -> Courses (PK: CourseID)
            {"relation": "REFERENCES", "direction": "out", "target_entity": "Courses"}
        ],
        "final_fields": {
            "Students": ["FirstName", "LastName"],
            "Enrollments": ["Semester", "Year", "Grade"], # Fields from intermediate entity
            "Courses": ["CourseName", "CourseCode"]
        }
    }
    data_mgr.retrieve_data(query4)

    # Example 5: Cross-Source Query (Students(CSV) -> HackathonParticipations(JSON))
    print("\n--- Query 5: Find Hackathons participated in by Biology students ---")
    query5 = {
        "action": "get_related",
        "start_entity": "Students",
        "start_filters": {"Major": "Biology"},
        "relations": [
            # Students (PK: StudentID) <- HackathonParticipations (FK: StudentID)
            {"relation": "REFERENCES", "direction": "in", "target_entity": "HackathonParticipations"}
        ],
        "final_fields": {
            "Students": ["FirstName", "LastName", "Major"],
            "HackathonParticipations": ["HackathonName", "ProjectTitle", "AwardsWon"]
        }
    }
    data_mgr.retrieve_data(query5)

    # Example 6: Cross-Source Query (Students(CSV) -> StudentClubs(XML))
    print("\n--- Query 6: Find Clubs joined by Student Sophia Lee (ID 1004) ---")
    query6 = {
        "action": "get_related",
        "start_entity": "Students",
        "start_filters": {"StudentID": 1004},
        "relations": [
            # Students (PK: StudentID) <- StudentClubs (FK: StudentID)
            {"relation": "REFERENCES", "direction": "in", "target_entity": "StudentClubs"}
        ],
        "final_fields": {
            "Students": ["FirstName", "LastName"],
            "StudentClubs": ["ClubName", "Role", "JoinDate"]
        }
    }
    data_mgr.retrieve_data(query6)

    # Example 7: More Complex Query (Courses(CSV) -> Enrollments(CSV) -> Students(CSV) -> SportsParticipations(JSON))
    print("\n--- Query 7: Find Sports activities of Students enrolled in 'CS101' ---")
    query7 = {
        "action": "get_related",
        "start_entity": "Courses",
        "start_filters": {"CourseCode": "CS101"},
        "relations": [
            # Courses (PK) <- Enrollments (FK)
            {"relation": "REFERENCES", "direction": "in", "target_entity": "Enrollments"},
            # Enrollments (FK) -> Students (PK)
            {"relation": "REFERENCES", "direction": "out", "target_entity": "Students"},
             # Students (PK) <- SportsParticipations (FK)
            {"relation": "REFERENCES", "direction": "in", "target_entity": "SportsParticipations"}
        ],
        "final_fields": {
            "Courses": ["CourseName"],
            # "Enrollments": [], # Optionally skip intermediate fields
            "Students": ["FirstName", "LastName", "Major"],
            "SportsParticipations": ["SportName", "TeamName", "Achievements"]
        }
    }
    data_mgr.retrieve_data(query7)


    print("\n" + "="*10 + " QUERIES COMPLETE " + "="*10)

    # --- 5. Close Connection ---
    client.close()
    logging.info("MongoDB connection closed.")

if __name__ == "__main__":
    run()