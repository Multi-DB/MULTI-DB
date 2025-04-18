import logging
from pymongo import MongoClient
from tabulate import tabulate # Explicit import

import config
from metadata_manager import MetadataManager
from data_manager import DataManager
from query_parser import parse_query # Using the conceptual parser

# Configure logging (consider INFO for production, DEBUG for development)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s') # DEBUG level

def run():
    """Main execution function."""
    # --- 1. Database Connection ---
    try:
        client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=5000) # Add timeout
        client.admin.command('ping') # Verify connection
        db = client[config.DATABASE_NAME]
        logging.info(f"Successfully connected to MongoDB database: {config.DATABASE_NAME}")
    except Exception as e:
        logging.error(f"Failed to connect to MongoDB: {e}")
        return

    # --- 2. Initialize Managers ---
    metadata_mgr = MetadataManager(db)
    data_mgr = DataManager(db)

    # --- 3. Setup (Run these on first setup or when schema changes) ---
    # IMPORTANT: Uncomment these lines ONLY for the very first run,
    # or if you modify data_models.py and want to rebuild everything.
    # Re-comment them afterwards to avoid deleting data on every run.
    # --------------------------------------------------------------------
    # logging.info("--- Running Initial Setup ---")
    metadata_mgr.build_graph_from_schema() # Clears and rebuilds metadata graph
    data_mgr.setup_collections_with_validation() # Creates/updates collections, validation, indexes
    data_mgr.insert_sample_data() # Inserts data ONLY if collections are empty
    logging.info("--- Initial Setup Complete ---")
    # --------------------------------------------------------------------

    # --- 4. Insert Example (Optional - uncomment to test) ---
    # new_course = {
    #     "CourseID": 111, "CourseCode": "AI101", "CourseName": "Intro to AI",
    #     "CreditHours": 3, "Department": "Computer Science"
    # }
    # if db["Courses"].count_documents({"CourseID": 111}) == 0:
    #     data_mgr.insert_single_ṛecord("Courses", new_course)
    # else:
    #     logging.info("Skipping insert: Course AI101 already exists.")

    # --- 5. Retrieval Examples ---
    print("\n" + "="*10 + " RUNNING RETRIEVAL QUERIES " + "="*10)

    # Example 1: Get all Courses (implicitly all fields)
    print("\n--- Query 1: Get all Courses ---")
    query1 = {"action": "get_entity", "entity": "Courses", "fields": None} # Explicitly None for all fields
    data_mgr.retrieve_data(query1)

    # Example 2: Get specific fields from Students
    print("\n--- Query 2: Get Student Names and GPA ---")
    query2 = {"action": "get_entity", "entity": "Students", "fields": ["FirstName", "LastName", "GPA"]}
    data_mgr.retrieve_data(query2)

    # Example 3: Get entity with filters (all fields)
    print("\n--- Query 3: Get Computer Science Students ---")
    query3 = {"action": "get_entity", "entity": "Students", "filters": {"Major": "Computer Science"}, "fields": None}
    data_mgr.retrieve_data(query3)

    # Example 4: Cross-Collection - Courses for John Smith
    print("\n--- Query 4: Find Courses for Student John Smith ---")
    query4 = {
        "action": "get_related",
        "start_entity": "Students",
        "start_filters": {"FirstName": "John", "LastName": "Smith"},
        "relations": [
            {"relation": "REFERENCES", "direction": "in", "target_entity": "Enrollments"},
            {"relation": "REFERENCES", "direction": "out", "target_entity": "Courses"}
        ],
        "final_fields": { # Specify exactly what to show
            "Students": ["FirstName", "LastName"],
            "Courses": ["CourseName", "CourseCode"]
        }
    }
    data_mgr.retrieve_data(query4)

    # Example 5: Cross-Collection - Hackathons for Biology majors
    print("\n--- Query 5: Find Hackathons for Biology Students ---")
    query5 = {
        "action": "get_related",
        "start_entity": "Students",
        "start_filters": {"Major": "Biology"},
        "relations": [
            {"relation": "REFERENCES", "direction": "in", "target_entity": "HackathonParticipations"}
        ],
        "final_fields": {
            "Students": ["FirstName", "LastName", "Major"],
            "HackathonParticipations": ["HackathonName", "ProjectTitle", "AwardsWon"]
        }
    }
    data_mgr.retrieve_data(query5)

    # Example 6: Conceptual Parser (Simple Get)
    print("\n--- Query 6: Parse 'get student names and gpa' ---")
    parsed_query_details = parse_query("get students name gpa")
    if parsed_query_details:
        data_mgr.retrieve_data(parsed_query_details)
    else:
        print("Skipping query 6 - parsing failed.")

    # Example 7: Conceptual Parser (Relation)
    print("\n--- Query 7: Parse 'find student john smith courses' ---")
    parsed_query_details_2 = parse_query("find student john smith courses")
    if parsed_query_details_2:
        logging.info(f"Manual Query 4: {query4}") # Compare definitions
        logging.info(f"Parsed Query 7: {parsed_query_details_2}")
        data_mgr.retrieve_data(parsed_query_details_2)
    else:
        print("Skipping query 7 - parsing failed.")

    print("\n" + "="*10 + " QUERIES COMPLETE " + "="*10)

    # --- 6. Close Connection ---
    client.close()
    logging.info("MongoDB connection closed.")

if __name__ == "__main__":
    run()