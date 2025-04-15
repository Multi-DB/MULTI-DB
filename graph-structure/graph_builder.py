from pymongo import MongoClient
from metadata_handler import MetadataHandler

client = MongoClient("mongodb://localhost:27017/")
db = client["multidb_snapshot"]
db["nodes"].delete_many({})
db["edges"].delete_many({})

handler = MetadataHandler(db)
handler.build_graph()
print("✅ Graph built from metadata.")
