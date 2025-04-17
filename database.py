from pymongo import MongoClient
from config import DBConfig
from schemas import get_mongo_validation_schemas
from models import SampleData
import pprint

class DatabaseManager:
    def __init__(self):
        self.client = DBConfig.get_client()
        self.relational_db = DBConfig.get_relational_db()
        self.document_db = DBConfig.get_document_db()
        self.schemas = get_mongo_validation_schemas()
        self.sample_data = SampleData()

    def drop_all_collections(self):
        """Drop all existing collections in both databases"""
        # Relational DB collections
        self.relational_db.Students.drop()
        self.relational_db.Courses.drop()
        self.relational_db.Enrollments.drop()
        
        # Document DB collections
        self.document_db.HackathonParticipations.drop()
        self.document_db.SportsParticipations.drop()
        self.document_db.StudentClubs.drop()
        self.document_db.Metagraph.drop()

    def create_collections(self):
        """Create all collections with schema validation"""
        # Relational collections
        self._create_collection_with_validation(
            db=self.relational_db,
            collection_name="Students",
            schema=self.schemas["Students"]
        )
        self._create_collection_with_validation(
            db=self.relational_db,
            collection_name="Courses",
            schema=self.schemas["Courses"]
        )
        self._create_collection_with_validation(
            db=self.relational_db,
            collection_name="Enrollments",
            schema=self.schemas["Enrollments"]
        )
        
        # Document collections
        self._create_collection_with_validation(
            db=self.document_db,
            collection_name="HackathonParticipations",
            schema=self.schemas["HackathonParticipations"]
        )
        self._create_collection_with_validation(
            db=self.document_db,
            collection_name="SportsParticipations",
            schema=self.schemas["SportsParticipations"]
        )
        self._create_collection_with_validation(
            db=self.document_db,
            collection_name="StudentClubs",
            schema=self.schemas["StudentClubs"]
        )
        self._create_collection_with_validation(
            db=self.document_db,
            collection_name="Metagraph",
            schema=self.schemas["Metagraph"]
        )

    def _create_collection_with_validation(self, db, collection_name, schema):
        """Helper method to create a collection with validation"""
        # The key change is here - we don't need to nest the schema under $jsonSchema
        db.create_collection(collection_name, validator=schema)

    def insert_sample_data(self):
        """Insert sample data into all collections"""
        # Relational data
        self.relational_db.Students.insert_many(self.sample_data.get_students_data())
        self.relational_db.Courses.insert_many(self.sample_data.get_courses_data())
        self.relational_db.Enrollments.insert_many(self.sample_data.get_enrollments_data())
        
        # Document data
        self.document_db.HackathonParticipations.insert_many(self.sample_data.get_hackathon_participations())
        self.document_db.SportsParticipations.insert_many(self.sample_data.get_sports_participations())
        self.document_db.StudentClubs.insert_many(self.sample_data.get_student_clubs())
        self.document_db.Metagraph.insert_one(self.sample_data.get_metagraph_data())

    def print_sample_data(self):
        """Print sample records from each collection for verification"""
        print("\nSample Student:")
        pprint.pprint(self.relational_db.Students.find_one())
        
        print("\nSample Course:")
        pprint.pprint(self.relational_db.Courses.find_one())
        
        print("\nSample Enrollment:")
        pprint.pprint(self.relational_db.Enrollments.find_one())
        
        print("\nSample Hackathon Participation:")
        pprint.pprint(self.document_db.HackathonParticipations.find_one())
        
        print("\nSample Sports Participation:")
        pprint.pprint(self.document_db.SportsParticipations.find_one())
        
        print("\nSample Student Club:")
        pprint.pprint(self.document_db.StudentClubs.find_one())
        
        print("\nMetagraph Data:")
        pprint.pprint(self.document_db.Metagraph.find_one())