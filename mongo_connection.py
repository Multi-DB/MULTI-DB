from pymongo import MongoClient

mongo_connection_string = "mongodb://localhost:27017/"
db_name = "multidb_snapshot"

try:

    client = MongoClient(mongo_connection_string)

    # Access the database
    db = client[db_name]
    print(f"Successfully connected to MongoDB database: {db_name}")

    # Access collections
    metadata_collection = db["metadata_collection"]
    relational_data_collection = db["relational_data_collection"]
    xml_data_collection = db["xml_data_collection"]
    json_data_collection = db["json_data_collection"]

    print(f"Collections accessed: metadata_collection, relational_data_collection, xml_data_collection, json_data_collection")

    # Test Insertion/Retrieval/Deletion
    test_metadata_document = {
        "source_type": "test",
        "source_name": "test_source",
        "description": "Test document from Python"
    }
    inserted_result = metadata_collection.insert_one(test_metadata_document)
    inserted_id = inserted_result.inserted_id
    print(f"\nInserted document ID: {inserted_id}")

    retrieved_document = metadata_collection.find_one({"_id": inserted_id})
    print("\nRetrieved document:")
    print(retrieved_document)

    delete_result = metadata_collection.delete_one({"_id": inserted_id})
    print(f"\nDocuments deleted: {delete_result.deleted_count}")


except Exception as e:
    print(f"Error connecting to MongoDB: {e}")
finally:
    if client:
        client.close() # Close the connection when done (best practice)
        print("\nMongoDB connection closed.")

print("\nScript execution finished.")