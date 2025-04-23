from pymongo import MongoClient

def get_mongo_connection(uri="mongodb://localhost:27017/", database_name="multi_db"):
    """
    Establish a connection to the MongoDB server and return the database object.
    
    :param uri: MongoDB connection URI
    :param database_name: Name of the database to connect to
    :return: Database object
    """
    try:
        client = MongoClient(uri)
        db = client[database_name]
        print(f"Connected to MongoDB database: {database_name}")
        return db
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        raise
