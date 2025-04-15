from pymongo import MongoClient
from metadata_handler import MetadataHandler

client = MongoClient("mongodb://localhost:27017/")
db = client["multidb_snapshot"]
handler = MetadataHandler(db)

handler.retrieve_data("relational", "Customers")
handler.retrieve_data("xml", "PurchaseOrder")
handler.retrieve_data("json", "Vendor")
