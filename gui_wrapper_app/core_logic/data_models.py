def get_collection_schemas():
    """
    Returns a list of dictionaries representing the schemas for each data source.
    Each dictionary contains the source type, source name, and entity definitions.
    """
    return [
        {
            "source_type": "csv",
            "source_name": "students",
            "entities": [
                {
                    "label": "Students",
                    "type": "table",
                    "fields": [
                        {"label": "StudentID", "data_type": "INT", "is_primary_key": True},
                        {"label": "FirstName", "data_type": "STRING"},
                        {"label": "LastName", "data_type": "STRING"},
                        {"label": "DateOfBirth", "data_type": "DATE"}
                    ]
                }
            ]
        },
        {
            "source_type": "xml",
            "source_name": "student_clubs",
            "entities": [
                {
                    "label": "StudentClubs",
                    "type": "xml_structure",
                    "fields": [
                        {"label": "MembershipID", "data_type": "STRING", "is_primary_key": True},
                        {"label": "StudentID", "data_type": "INT", "is_foreign_key": True, "references": "Students"},
                        {"label": "ClubName", "data_type": "STRING"}
                    ]
                }
            ]
        },
        {
            "source_type": "json",
            "source_name": "courses",
            "entities": [
                {
                    "label": "Courses",
                    "type": "json_objects",
                    "fields": [
                        {"label": "CourseID", "data_type": "INT", "is_primary_key": True},
                        {"label": "CourseName", "data_type": "STRING"},
                        {"label": "Credits", "data_type": "INT"}
                    ]
                }
            ]
        }
    ]

def get_mongo_validation_schemas():
    """
    Returns a dictionary mapping entity labels to MongoDB JSON Schema validation rules.
    This defines the target structure enforced in MongoDB.
    """
    return {
        "Students": {
            "bsonType": "object",
            "required": ["StudentID", "FirstName", "LastName"],
            "properties": {
                "StudentID": {"bsonType": "int"},
                "FirstName": {"bsonType": "string"},
                "LastName": {"bsonType": "string"},
                "DateOfBirth": {"bsonType": "date"}
            }
        },
        "StudentClubs": {
            "bsonType": "object",
            "required": ["MembershipID", "StudentID", "ClubName"],
            "properties": {
                "MembershipID": {"bsonType": "string"},
                "StudentID": {"bsonType": "int"},
                "ClubName": {"bsonType": "string"}
            }
        },
        "Courses": {
            "bsonType": "object",
            "required": ["CourseID", "CourseName"],
            "properties": {
                "CourseID": {"bsonType": "int"},
                "CourseName": {"bsonType": "string"},
                "Credits": {"bsonType": "int"}
            }
        }
    }

def find_schema_for_entity(entity_label):
    """
    Finds and returns the schema definition for a given entity label.
    """
    schemas = get_collection_schemas()
    for schema in schemas:
        for entity in schema["entities"]:
            if entity["label"] == entity_label:
                return entity
    return None