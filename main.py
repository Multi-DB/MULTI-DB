from pymongo import MongoClient
from pymongo.errors import OperationFailure, WriteError
from bson import ObjectId

class MultiModelDB:
    def __init__(self, db_name="multidb_snapshot"):
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client[db_name]
        self._setup_collections_with_validators()

    def _setup_collections_with_validators(self):
        """Create collections with validators in one operation"""
        # Define all collections with their validators
        collections = {
            "relational_data_collection": {
                "validator": {
                    "$jsonSchema": {
                        "bsonType": "object",
                        "required": ["id", "type", "label", "properties"],
                        "properties": {
                            "id": {"bsonType": "string"},
                            "type": {"enum": ["table", "column", "record"]},
                            "label": {"bsonType": "string"},
                            "properties": {
                                "bsonType": "object",
                                "properties": {
                                    "CustomerID": {"bsonType": "int"},
                                    "Name": {"bsonType": "string"},
                                    "Email": {"bsonType": "string"}
                                }
                            }
                        }
                    }
                }
            },
            "xml_data_collection": {
                "validator": {
                    "$jsonSchema": {
                        "bsonType": "object",
                        "required": ["id", "type", "label", "properties"],
                        "properties": {
                            "id": {"bsonType": "string"},
                            "type": {"enum": ["element", "order"]},
                            "label": {"bsonType": "string"},
                            "properties": {
                                "bsonType": "object",
                                "properties": {
                                    "order_id": {"bsonType": "string"},
                                    "order_date": {"bsonType": "string"}
                                }
                            }
                        }
                    }
                }
            },
            "json_data_collection": {
                "validator": {
                    "$jsonSchema": {
                        "bsonType": "object",
                        "required": ["id", "type", "label", "properties"],
                        "properties": {
                            "id": {"bsonType": "string"},
                            "type": {"enum": ["object", "vendor"]},
                            "label": {"bsonType": "string"},
                            "properties": {
                                "bsonType": "object",
                                "properties": {
                                    "vendor_id": {"bsonType": "string"},
                                    "vendor_name": {"bsonType": "string"}
                                }
                            }
                        }
                    }
                }
            },
            "metadata_graph_collection": {
                "validator": {
                    "$jsonSchema": {
                        "bsonType": "object",
                        "required": ["nodes", "edges"],
                        "properties": {
                            "nodes": {
                                "bsonType": "array",
                                "items": {
                                    "required": ["id", "type", "label"],
                                    "properties": {
                                        "id": {"bsonType": "string"},
                                        "type": {"enum": ["table", "xml", "json", "column", "element", "property"]},
                                        "label": {"bsonType": "string"}
                                    }
                                }
                            },
                            "edges": {
                                "bsonType": "array",
                                "items": {
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
        }

        # Create collections with validators
        self.collections = {}
        for coll_name, options in collections.items():
            if coll_name not in self.db.list_collection_names():
                self.db.create_collection(coll_name, validator=options["validator"])
                print(f"✅ Created collection {coll_name} with validator")
            else:
                try:
                    self.db.command({
                        "collMod": coll_name,
                        "validator": options["validator"]
                    })
                    print(f"✅ Updated validator for {coll_name}")
                except OperationFailure as e:
                    print(f"⚠️ Could not update validator for {coll_name}: {e}")
            
            self.collections[coll_name.replace("_collection", "").replace("_data", "")] = self.db[coll_name]

    def insert_data(self, collection_name, document):
        """Insert document with schema validation"""
        try:
            collection = self.collections[collection_name]
            result = collection.insert_one(document)
            print(f"✅ Inserted into {collection_name}: {result.inserted_id}")
            return result.inserted_id
        except WriteError as e:
            print(f"❌ Validation failed for {collection_name}:")
            print(f"Code: {e.code}")
            print(f"Details: {e.details.get('errInfo', e.details)}")
            return None
        except Exception as e:
            print(f"⚠️ Error inserting to {collection_name}: {str(e)}")
            return None

    def initialize_metadata_graph(self):
        """Insert the unified metadata graph with embedded edges"""
        metadata_graph = {
            "nodes": [
                {"id": "Customers", "type": "table", "label": "Customers"},
                {"id": "PurchaseOrders", "type": "xml", "label": "PurchaseOrders"},
                {"id": "VendorData", "type": "json", "label": "VendorData"},
                {"id": "CustomerID", "type": "column", "label": "CustomerID"},
                {"id": "OrderID", "type": "element", "label": "OrderID"},
                {"id": "VendorID", "type": "property", "label": "VendorID"}
            ],
            "edges": [
                {"source": "Customers", "target": "CustomerID", "relation": "HAS_COLUMN"},
                {"source": "PurchaseOrders", "target": "OrderID", "relation": "HAS_ELEMENT"},
                {"source": "VendorData", "target": "VendorID", "relation": "HAS_PROPERTY"},
                {"source": "CustomerID", "target": "OrderID", "relation": "FOREIGN_KEY"}
            ]
        }
        return self.insert_data("metadata_graph", metadata_graph)

    def insert_sample_data(self):
        """Insert sample data records that conform to the schemas"""
        # Insert relational data (Customers)
        customer_id = self.insert_data("relational", {
            "id": "cust_1001",
            "type": "record",
            "label": "Customer Record",
            "properties": {
                "CustomerID": 1001,
                "Name": "John Doe",
                "Email": "john@example.com"
            }
        })

        # Insert XML data (Orders)
        order_id = self.insert_data("xml", {
            "id": "order_1001",
            "type": "order",
            "label": "Purchase Order",
            "properties": {
                "order_id": "PO1001",
                "order_date": "2024-03-25",
                "customer_ref": "cust_1001"
            }
        })

        # Insert JSON data (Vendors)
        vendor_id = self.insert_data("json", {
            "id": "vendor_1001",
            "type": "vendor",
            "label": "Vendor Record",
            "properties": {
                "vendor_id": "V1001",
                "vendor_name": "Tech Supplies Inc."
            }
        })

        return customer_id, order_id, vendor_id

    def close(self):
        """Close database connection"""
        self.client.close()
        print("Database connection closed")

if __name__ == "__main__":
    db = MultiModelDB()
    
    try:
        # Initialize metadata graph
        db.initialize_metadata_graph()
        
        # Insert sample data records
        customer_id, order_id, vendor_id = db.insert_sample_data()
        
        # Verify data
        print("\nInserted Documents:")
        print(f"Customers: {db.collections['relational'].count_documents({})}")
        print(f"Orders: {db.collections['xml'].count_documents({})}")
        print(f"Vendors: {db.collections['json'].count_documents({})}")
        
        # Show metadata graph
        graph = db.collections["metadata_graph"].find_one()
        print("\nMetadata Graph Relationships:")
        for edge in graph["edges"]:
            print(f"{edge['source']} --{edge['relation']}--> {edge['target']}")
        
    finally:
        db.close()