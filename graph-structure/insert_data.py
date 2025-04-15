from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["multidb_snapshot"]

# Clean data collections
db["relational_data_collection"].delete_many({})
db["xml_data_collection"].delete_many({})
db["json_data_collection"].delete_many({})

# Relational
db.relational_data_collection.insert_one({
    "table_name": "Customers",
    "source": "CustomerDB",
    "rows": [
        { "CustomerID": 101, "Name": "Alice", "Email": "alice@example.com" },
        { "CustomerID": 102, "Name": "Bob", "Email": "bob@example.com" }
    ]
})

# XML
db.xml_data_collection.insert_one({
    "element_name": "PurchaseOrder",
    "source": "PurchaseOrders.xml",
    "entries": [
        { "OrderID": "PO001", "CustomerName": "Charlie", "Amount": 200.5 },
        { "OrderID": "PO002", "CustomerName": "Dave", "Amount": 340.0 }
    ]
})

# JSON
db.json_data_collection.insert_one({
    "object_name": "Vendor",
    "source": "VendorData",
    "records": [
        { "VendorID": 1, "VendorName": "Acme Corp", "ContactEmail": "acme@example.com" },
        { "VendorID": 2, "VendorName": "Globex Inc", "ContactEmail": "globex@example.com" }
    ]
})
print("✅ Inserted data.")
