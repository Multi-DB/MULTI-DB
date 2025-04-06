# insert_data_from_metadata.py

from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["multidb_snapshot"]

nodes = db["nodes"]
edges = db["edges"]
metadata_collection = db["metadata_collection"]
relational_data = db["relational_data_collection"]

# Step 1: Find the metadata document for source "CustomerDB"
metadata = metadata_collection.find_one({
    "sources.source_name": "CustomerDB",
    "sources.source_type": "relational"
})

if not metadata:
    print("No metadata found for CustomerDB")
    exit()

# Step 2: Find the table node labeled "Customers"
table_node = nodes.find_one({
    "type": "table",
    "label": "Customers"
})

if not table_node:
    print("Table 'Customers' not found in metadata.")
    exit()

table_id = table_node["id"]

# Step 3: Get all columns using HAS_COLUMN edges
column_edges = edges.find({ "source": table_id, "relation": "HAS_COLUMN" })
column_ids = [edge["target"] for edge in column_edges]

# Get column metadata
columns = []
for col_id in column_ids:
    col_node = nodes.find_one({ "id": col_id })
    columns.append(col_node["label"])

# Step 4: Create dummy row data matching column names
sample_rows = [
    { "CustomerID": 101, "Name": "John Doe", "Email": "john@example.com" },
    { "CustomerID": 102, "Name": "Jane Smith", "Email": "jane@example.com" },
    { "CustomerID": 103, "Name": "Sam Wilson", "Email": "sam@example.com" }
]

# Step 5: Insert into relational_data_collection
relational_data.insert_one({
    "table_name": table_node["label"],
    "source": "CustomerDB",
    "rows": sample_rows
})

print("✅ Inserted sample data into relational_data_collection.")
