from pymongo.errors import WriteError, OperationFailure
from .connection import connect_to_mongodb
from .validators import get_collection_validators

class MultiModelDB:
    def __init__(self):
        self.client, self.db, self.error = connect_to_mongodb()
        self.connection_status = {
            'mongodb_connected': self.client is not None,
            'collections_initialized': False,
            'error': self.error
        }
        if self.client is not None and self.db is not None:
            self._setup_collections_with_validators()

    def _setup_collections_with_validators(self):
        collections = get_collection_validators()
        self.collections = {}
        initialized = []

        for name, conf in collections.items():
            short = name.replace("_collection", "").replace("_data", "")
            try:
                if name not in self.db.list_collection_names():
                    self.db.create_collection(name, validator=conf["validator"])
                    self.db[name].create_index("id", unique=True)  # ⬅️ Enforce uniqueness
                    print(f"✅ Created collection {name}")
                    initialized.append({'name': name, 'status': 'created'})
                else:
                    self.db.command({"collMod": name, "validator": conf["validator"]})
                    print(f"✅ Updated validator for {name}")
                    initialized.append({'name': name, 'status': 'exists'})
                self.collections[short] = self.db[name]
            except OperationFailure as e:
                print(f"⚠️ Could not update validator for {name}: {e}")
                initialized.append({'name': name, 'status': 'exists', 'error': str(e)})
            except Exception as e:
                print(f"❌ Failed to initialize collection {name}: {str(e)}")
                initialized.append({'name': name, 'status': 'failed', 'error': str(e)})

        self.connection_status['collections_initialized'] = True
        self.connection_status['initialized_collections'] = initialized
        self.connection_status['ready'] = all(col['status'] in ['created', 'exists'] for col in initialized)

    def insert_data(self, name, doc):
        try:
            # ✅ Prevent redundant insert by checking existing 'id'
            if self.collections[name].find_one({"id": doc["id"]}):
                print(f"⚠️ Document with id '{doc['id']}' already exists in {name}")
                return None

            result = self.collections[name].insert_one(doc)
            print(f"✅ Inserted into {name}: {result.inserted_id}")
            return result.inserted_id
        except WriteError as e:
            print(f"❌ Validation failed for {name}: {e.details}")
            return None
        except Exception as e:
            print(f"⚠️ Error inserting to {name}: {str(e)}")
            return None

    def initialize_metadata_graph(self):
        graph = {
            "nodes": [
                {"id": "Customers", "type": "table", "label": "Customers", "datasource": "relational"},
                {"id": "PurchaseOrders", "type": "xml", "label": "PurchaseOrders", "datasource": "xml"},
                {"id": "VendorData", "type": "json", "label": "VendorData", "datasource": "json"},
                {"id": "CustomerID", "type": "column", "label": "CustomerID", "datasource": "relational"},
                {"id": "OrderID", "type": "element", "label": "OrderID", "datasource": "xml"},
                {"id": "VendorID", "type": "property", "label": "VendorID", "datasource": "json"},
                {"id": "MetadataGraph", "type": "graph", "label": "Metadata Graph", "datasource": "metadata"},
            ],
            "edges": [
                {"source": "Customers", "target": "CustomerID", "relation": "HAS_COLUMN"},
                {"source": "PurchaseOrders", "target": "OrderID", "relation": "HAS_ELEMENT"},
                {"source": "VendorData", "target": "VendorID", "relation": "HAS_PROPERTY"},
                {"source": "CustomerID", "target": "OrderID", "relation": "FOREIGN_KEY"},
                {"source": "MetadataGraph", "target": "Customers", "relation": "DESCRIBES"},
                {"source": "MetadataGraph", "target": "PurchaseOrders", "relation": "DESCRIBES"},
                {"source": "MetadataGraph", "target": "VendorData", "relation": "DESCRIBES"}
            ]
        }
        return self.insert_data("metadata_graph", graph)

    def insert_sample_data(self):
        self.insert_data("relational", {
            "id": "cust_1001",
            "type": "record",
            "label": "Customer Record",
            "properties": {
                "CustomerID": 1001,
                "Name": "John Doe",
                "Email": "john@example.com"
            }
        })
        self.insert_data("xml", {
            "id": "order_1001",
            "type": "order",
            "label": "Purchase Order",
            "properties": {
                "order_id": "PO1001",
                "order_date": "2024-03-25",
                "customer_ref": "cust_1001"
            }
        })
        self.insert_data("json", {
            "id": "vendor_1001",
            "type": "vendor",
            "label": "Vendor Record",
            "properties": {
                "vendor_id": "V1001",
                "vendor_name": "Tech Supplies Inc."
            }
        })

    def close(self):
        if self.client:
            self.client.close()
            print("Database connection closed")
