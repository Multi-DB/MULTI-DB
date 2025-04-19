# data_models.py

from datetime import datetime

# --- Schema Definitions for Data Sources ---

def get_collection_schemas():
    """
    Return schema definitions for University data sources (CSV, JSON, XML)
    and how they map to logical entities and MongoDB collections.
    Includes Foreign Key definitions and parsing hints.
    """
    return [
        # --- Source 1: UniversityDB (Simulated Relational via CSV) ---
        {
            "source_type": "relational",
            "source_name": "UniversityDB",
            "entities": [
                {
                    "type": "table",
                    "label": "Students", # Target Collection Name
                    "columns": [ # Corresponds to CSV headers
                        {"label": "StudentID", "data_type": "INT", "is_primary_key": True},
                        {"label": "FirstName", "data_type": "VARCHAR(50)"},
                        {"label": "LastName", "data_type": "VARCHAR(50)"},
                        {"label": "DateOfBirth", "data_type": "DATE"},
                        {"label": "Gender", "data_type": "VARCHAR(1)"},
                        {"label": "Email", "data_type": "VARCHAR(100)"},
                        {"label": "Phone", "data_type": "VARCHAR(15)"},
                        {"label": "Address", "data_type": "VARCHAR(255)"},
                        {"label": "EnrollmentDate", "data_type": "DATE"},
                        {"label": "Major", "data_type": "VARCHAR(100)"},
                        {"label": "GPA", "data_type": "DECIMAL(3,2)"} # Stored as double
                    ]
                },
                {
                    "type": "table",
                    "label": "Courses", # Target Collection Name
                    "columns": [
                        {"label": "CourseID", "data_type": "INT", "is_primary_key": True},
                        {"label": "CourseCode", "data_type": "VARCHAR(10)"},
                        {"label": "CourseName", "data_type": "VARCHAR(100)"},
                        {"label": "CreditHours", "data_type": "INT"},
                        {"label": "Department", "data_type": "VARCHAR(100)"}
                    ]
                },
                {
                    "type": "table",
                    "label": "Enrollments", # Target Collection Name
                    "columns": [
                        {"label": "EnrollmentID", "data_type": "INT", "is_primary_key": True},
                        # Foreign Key Definitions:
                        {"label": "StudentID", "data_type": "INT", "is_foreign_key": True, "references": "Students"},
                        {"label": "CourseID", "data_type": "INT", "is_foreign_key": True, "references": "Courses"},
                        {"label": "Semester", "data_type": "VARCHAR(6)"},
                        {"label": "Year", "data_type": "INT"},
                        {"label": "Grade", "data_type": "VARCHAR(2)"}
                    ]
                }
            ]
        },
        # --- Source 2: UniversityActivities (Document Sources via JSON/XML) ---
        {
            "source_type": "document", # Grouping logical source type
            "source_name": "UniversityActivities",
            "entities": [
                {
                    "type": "json_objects", # From hackathons.json
                    "label": "HackathonParticipations", # Target Collection Name
                    "fields": [ # Map JSON structure to target fields
                        {"label": "ActivityID", "data_type": "STRING", "is_primary_key": True, "json_path": "activityId"},
                        {"label": "StudentID", "data_type": "INT", "is_foreign_key": True, "references": "Students", "json_path": "studentRef"},
                        {"label": "HackathonName", "data_type": "STRING", "json_path": "eventName"},
                        {"label": "TeamName", "data_type": "STRING", "json_path": "team"},
                        {"label": "ProjectTitle", "data_type": "STRING", "json_path": "project.title"},
                        {"label": "ProjectDescription", "data_type": "STRING", "json_path": "project.desc"},
                        {"label": "ParticipationDate", "data_type": "DATE", "json_path": "date"}, # Will be parsed as date
                        {"label": "AwardsWon", "data_type": "ARRAY<STRING>", "json_path": "results.awards"},
                        {"label": "SkillsUsed", "data_type": "ARRAY<STRING>", "json_path": "results.skills"}
                    ]
                },
                {
                    "type": "json_objects", # From sports.json
                    "label": "SportsParticipations", # Target Collection Name
                    "fields": [
                        {"label": "ActivityID", "data_type": "STRING", "is_primary_key": True, "json_path": "_id"},
                        {"label": "StudentID", "data_type": "INT", "is_foreign_key": True, "references": "Students", "json_path": "studentIdentifier"},
                        {"label": "SportName", "data_type": "STRING", "json_path": "details.sport"},
                        {"label": "TeamName", "data_type": "STRING", "json_path": "details.team"},
                        {"label": "Position", "data_type": "STRING", "json_path": "details.pos"},
                        {"label": "Season", "data_type": "STRING", "json_path": "stats.season"},
                        {"label": "MatchesPlayed", "data_type": "INT", "json_path": "stats.played"},
                        {"label": "Achievements", "data_type": "ARRAY<STRING>", "json_path": "notes"} # Map 'notes' array
                    ]
                },
                {
                    "type": "xml_structure", # From student_clubs.xml
                    "label": "StudentClubs", # Target Collection Name
                    "xpath_base": "//Membership", # Base element for each record
                    "fields": [
                        {"label": "MembershipID", "data_type": "STRING", "is_primary_key": True, "xpath": "@id"}, # Attribute
                        {"label": "StudentID", "data_type": "INT", "is_foreign_key": True, "references": "Students", "xpath": "@studentId"}, # Attribute
                        {"label": "ClubName", "data_type": "STRING", "xpath": "ClubName"}, # Child element text
                        {"label": "Role", "data_type": "STRING", "xpath": "Role"},
                        {"label": "JoinDate", "data_type": "DATE", "xpath": "Joined"},
                        {"label": "Active", "data_type": "BOOLEAN", "xpath": "@active"}, # Attribute
                        {"label": "MeetingsAttended", "data_type": "INT", "xpath": "Attendance/@count"} # Attribute of child
                    ]
                }
                # Removed Metagraph definition here
            ]
        }
    ]


# --- MongoDB Validation Schemas (For Target Collections) ---

def get_mongo_validation_schemas():
    """Return MongoDB JSON Schema validations for all target collections."""
    schemas = {
        # --- UniversityDB Collections ---
        "Students": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["StudentID", "FirstName", "LastName", "DateOfBirth", "EnrollmentDate", "Major"],
                "properties": {
                    "StudentID": {"bsonType": "int"},
                    "FirstName": {"bsonType": "string", "maxLength": 50},
                    "LastName": {"bsonType": "string", "maxLength": 50},
                    "DateOfBirth": {"bsonType": "date"},
                    "Gender": {"bsonType": ["string", "null"], "maxLength": 1},
                    "Email": {"bsonType": ["string", "null"], "maxLength": 100},
                    "Phone": {"bsonType": ["string", "null"], "maxLength": 15},
                    "Address": {"bsonType": ["string", "null"], "maxLength": 255},
                    "EnrollmentDate": {"bsonType": "date"},
                    "Major": {"bsonType": "string", "maxLength": 100},
                    "GPA": {"bsonType": ["double", "null"], "minimum": 0, "maximum": 4.0}
                }
            }
        },
        "Courses": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["CourseID", "CourseCode", "CourseName", "CreditHours", "Department"],
                "properties": {
                    "CourseID": {"bsonType": "int"},
                    "CourseCode": {"bsonType": "string", "maxLength": 10},
                    "CourseName": {"bsonType": "string", "maxLength": 100},
                    "CreditHours": {"bsonType": "int"},
                    "Department": {"bsonType": "string", "maxLength": 100}
                }
            }
        },
        "Enrollments": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["EnrollmentID", "StudentID", "CourseID", "Semester", "Year"],
                "properties": {
                    "EnrollmentID": {"bsonType": "int"},
                    "StudentID": {"bsonType": "int"}, # FK
                    "CourseID": {"bsonType": "int"}, # FK
                    "Semester": {"bsonType": "string", "maxLength": 6},
                    "Year": {"bsonType": "int"},
                    "Grade": {"bsonType": ["string", "null"], "maxLength": 2}
                }
            }
        },
         # --- UniversityActivities Collections ---
        "HackathonParticipations": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["ActivityID", "StudentID", "HackathonName", "ParticipationDate"],
                "properties": {
                    "ActivityID": {"bsonType": "string"},
                    "StudentID": {"bsonType": "int"}, # FK
                    "HackathonName": {"bsonType": "string"},
                    "TeamName": {"bsonType": ["string", "null"]},
                    "ProjectTitle": {"bsonType": ["string", "null"]},
                    "ProjectDescription": {"bsonType": ["string", "null"]},
                    "ParticipationDate": {"bsonType": "date"},
                    "AwardsWon": {"bsonType": "array", "items": {"bsonType": "string"}},
                    "SkillsUsed": {"bsonType": "array", "items": {"bsonType": "string"}}
                }
            }
        },
        "SportsParticipations": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["ActivityID", "StudentID", "SportName", "Season"],
                "properties": {
                    "ActivityID": {"bsonType": "string"},
                    "StudentID": {"bsonType": "int"}, # FK
                    "SportName": {"bsonType": "string"},
                    "TeamName": {"bsonType": ["string", "null"]},
                    "Position": {"bsonType": ["string", "null"]},
                    "Season": {"bsonType": "string"},
                    "MatchesPlayed": {"bsonType": ["int", "null"]},
                    "Achievements": {"bsonType": "array", "items": {"bsonType": "string"}}
                }
            }
        },
        "StudentClubs": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["MembershipID", "StudentID", "ClubName", "JoinDate", "Active"],
                "properties": {
                    "MembershipID": {"bsonType": "string"},
                    "StudentID": {"bsonType": "int"}, # FK
                    "ClubName": {"bsonType": "string"},
                    "Role": {"bsonType": ["string", "null"]},
                    "JoinDate": {"bsonType": "date"},
                    "Active": {"bsonType": "bool"},
                    "MeetingsAttended": {"bsonType": ["int", "null"]}
                }
            }
        }
    }
    return schemas


# --- Helper function to find schema by label ---
_schema_cache = None
def find_schema_for_entity(entity_label: str) -> dict | None:
    """Finds the schema definition dictionary for a given entity label."""
    global _schema_cache
    if _schema_cache is None:
        _schema_cache = {}
        all_schemas = get_collection_schemas()
        for source_schema in all_schemas:
            for entity in source_schema.get("entities", []):
                _schema_cache[entity["label"]] = {
                    "schema": entity, # The specific entity definition
                    "source_type": source_schema["source_type"],
                    "source_name": source_schema["source_name"]
                }
    return _schema_cache.get(entity_label)