from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

def connect_to_mongodb(uri="mongodb://localhost:27017/", db_name="multidb_snapshot"):
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=2000)
        client.server_info()  # Check connection
        db = client[db_name]
        return client, db, None
    except ServerSelectionTimeoutError as e:
        return None, None, f"MongoDB connection failed: {str(e)}"
    except Exception as e:
        return None, None, f"Connection error: {str(e)}"
