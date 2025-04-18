from pymongo import MongoClient

class DBConfig:
    MONGO_URI = "mongodb://localhost:27017/"
    RELATIONAL_DB_NAME = "UniversityDB"
    DOCUMENT_DB_NAME = "UniversityActivities"

    @classmethod
    def get_client(cls):
        return MongoClient(cls.MONGO_URI)
    
    @classmethod
    def get_relational_db(cls):
        client = cls.get_client()
        return client[cls.RELATIONAL_DB_NAME]
    
    @classmethod
    def get_document_db(cls):
        client = cls.get_client()
        return client[cls.DOCUMENT_DB_NAME]