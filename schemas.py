def get_collection_schemas():
    """Return schema definitions for all collections in the specified format."""
    return [
        {
            "source_type": "relational",
            "source_name": "UniversityDB",
            "entities": [
                {
                    "type": "table",
                    "label": "Students",
                    "columns": [
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
                        {"label": "GPA", "data_type": "DECIMAL(3,2)"}
                    ]
                },
                {
                    "type": "table",
                    "label": "Courses",
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
                    "label": "Enrollments",
                    "columns": [
                        {"label": "EnrollmentID", "data_type": "INT", "is_primary_key": True},
                        {"label": "StudentID", "data_type": "INT"},
                        {"label": "CourseID", "data_type": "INT"},
                        {"label": "Semester", "data_type": "VARCHAR(6)"},
                        {"label": "Year", "data_type": "INT"},
                        {"label": "Grade", "data_type": "VARCHAR(2)"}
                    ]
                }
            ]
        },
        {
            "source_type": "document",
            "source_name": "UniversityActivities",
            "entities": [
                {
                    "type": "collection",
                    "label": "HackathonParticipations",
                    "fields": [
                        {"label": "ActivityID", "data_type": "STRING", "is_primary_key": True},
                        {"label": "StudentID", "data_type": "INT"},
                        {"label": "HackathonName", "data_type": "STRING"},
                        {"label": "TeamName", "data_type": "STRING"},
                        {"label": "ProjectTitle", "data_type": "STRING"},
                        {"label": "ProjectDescription", "data_type": "STRING"},
                        {"label": "ParticipationDate", "data_type": "DATE"},
                        {"label": "AwardsWon", "data_type": "ARRAY<STRING>"},
                        {"label": "SkillsUsed", "data_type": "ARRAY<STRING>"}
                    ]
                },
                {
                    "type": "collection",
                    "label": "SportsParticipations",
                    "fields": [
                        {"label": "ActivityID", "data_type": "STRING", "is_primary_key": True},
                        {"label": "StudentID", "data_type": "INT"},
                        {"label": "SportName", "data_type": "STRING"},
                        {"label": "TeamName", "data_type": "STRING"},
                        {"label": "Position", "data_type": "STRING"},
                        {"label": "Season", "data_type": "STRING"},
                        {"label": "MatchesPlayed", "data_type": "INT"},
                        {"label": "Achievements", "data_type": "ARRAY<STRING>"}
                    ]
                },
                {
                    "type": "collection",
                    "label": "StudentClubs",
                    "fields": [
                        {"label": "MembershipID", "data_type": "STRING", "is_primary_key": True},
                        {"label": "StudentID", "data_type": "INT"},
                        {"label": "ClubName", "data_type": "STRING"},
                        {"label": "Role", "data_type": "STRING"},
                        {"label": "JoinDate", "data_type": "DATE"},
                        {"label": "Active", "data_type": "BOOLEAN"},
                        {"label": "MeetingsAttended", "data_type": "INT"}
                    ]
                },
                {
                    "type": "collection",
                    "label": "Metagraph",
                    "fields": [
                        {
                            "label": "nodes",
                            "data_type": "ARRAY<OBJECT>",
                            "structure": {
                                "id": "STRING",
                                "type": "STRING",
                                "label": "STRING",
                                "datasource": "STRING"
                            }
                        },
                        {
                            "label": "edges",
                            "data_type": "ARRAY<OBJECT>",
                            "structure": {
                                "source": "STRING",
                                "target": "STRING",
                                "relation": "STRING"
                            }
                        }
                    ]
                }
            ]
        }
    ]

def get_mongo_validation_schemas():
    """Return MongoDB JSON Schema validations for all collections."""
    return {
        "Students": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["StudentID", "FirstName", "LastName", "DateOfBirth", "EnrollmentDate", "Major"],
                "properties": {
                    "StudentID": {"bsonType": "int"},
                    "FirstName": {"bsonType": "string", "maxLength": 50},
                    "LastName": {"bsonType": "string", "maxLength": 50},
                    "DateOfBirth": {"bsonType": "date"},
                    "Gender": {"bsonType": "string", "maxLength": 1},
                    "Email": {"bsonType": "string", "maxLength": 100},
                    "Phone": {"bsonType": "string", "maxLength": 15},
                    "Address": {"bsonType": "string", "maxLength": 255},
                    "EnrollmentDate": {"bsonType": "date"},
                    "Major": {"bsonType": "string", "maxLength": 100},
                    "GPA": {"bsonType": "double", "minimum": 0, "maximum": 4}
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
                    "StudentID": {"bsonType": "int"},
                    "CourseID": {"bsonType": "int"},
                    "Semester": {"bsonType": "string", "maxLength": 6},
                    "Year": {"bsonType": "int"},
                    "Grade": {"bsonType": "string", "maxLength": 2}
                }
            }
        },
        "HackathonParticipations": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["ActivityID", "StudentID", "HackathonName", "ParticipationDate"],
                "properties": {
                    "ActivityID": {"bsonType": "string"},
                    "StudentID": {"bsonType": "int"},
                    "HackathonName": {"bsonType": "string"},
                    "TeamName": {"bsonType": "string"},
                    "ProjectTitle": {"bsonType": "string"},
                    "ProjectDescription": {"bsonType": "string"},
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
                    "StudentID": {"bsonType": "int"},
                    "SportName": {"bsonType": "string"},
                    "TeamName": {"bsonType": "string"},
                    "Position": {"bsonType": "string"},
                    "Season": {"bsonType": "string"},
                    "MatchesPlayed": {"bsonType": "int"},
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
                    "StudentID": {"bsonType": "int"},
                    "ClubName": {"bsonType": "string"},
                    "Role": {"bsonType": "string"},
                    "JoinDate": {"bsonType": "date"},
                    "Active": {"bsonType": "bool"},
                    "MeetingsAttended": {"bsonType": "int"}
                }
            }
        },
        "Metagraph": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["nodes", "edges"],
                "properties": {
                    "nodes": {
                        "bsonType": "array",
                        "items": {
                            "bsonType": "object",
                            "required": ["id", "type", "label", "datasource"],
                            "properties": {
                                "id": {"bsonType": "string"},
                                "type": {"bsonType": "string"},
                                "label": {"bsonType": "string"},
                                "datasource": {"bsonType": "string"}
                            }
                        }
                    },
                    "edges": {
                        "bsonType": "array",
                        "items": {
                            "bsonType": "object",
                            "required": ["source", "target", "relation"],
                            "properties": {
                                "source": {"bsonType": "string"},
                                "target": {"bsonType": "string"},
                                "relation": {"bsonType": "string"}
                            }
                        }
                    }
                }
            }
        }
    }