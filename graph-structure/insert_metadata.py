from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["multidb_snapshot"]

# Clean collections
db["metadata_collection"].delete_many({})

# Insert metadata for relational, xml, and json
db.metadata_collection.insert_many([
    {
        "source_type": "relational",
        "source_name": "CustomerDB",
        "entities": [
            {
                "type": "table",
                "label": "Customers",
                "columns": [
                    { "label": "CustomerID", "data_type": "INT", "is_primary_key": True },
                    { "label": "Name", "data_type": "VARCHAR" },
                    { "label": "Email", "data_type": "VARCHAR" }
                ]
            }
        ]
    },
    {
        "source_type": "xml",
        "source_name": "PurchaseOrders.xml",
        "entities": [
            {
                "type": "element",
                "label": "PurchaseOrder",
                "children": [
                    { "label": "OrderID", "data_type": "string" },
                    { "label": "CustomerName", "data_type": "string" },
                    { "label": "Amount", "data_type": "float" }
                ]
            }
        ]
    },
    {
        "source_type": "json",
        "source_name": "VendorData",
        "entities": [
            {
                "type": "object",
                "label": "Vendor",
                "fields": [
                    { "label": "VendorID", "data_type": "int" },
                    { "label": "VendorName", "data_type": "string" },
                    { "label": "ContactEmail", "data_type": "string" }
                ]
            }
        ]
    }
])
print("✅ Inserted metadata.")
