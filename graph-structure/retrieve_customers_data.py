# retrieve_customers_data.py

from pymongo import MongoClient
from tabulate import tabulate  # Optional, for pretty printing

client = MongoClient("mongodb://localhost:27017/")
db = client["multidb_snapshot"]

nodes = db["nodes"]
edges = db["edges"]
relational_data = db["relational_data_collection"]

# Step 1: Find the table node for "Customers"
table_node = nodes.find_one({ "type": "table", "label": "Customers" })
if not table_node:
    print("❌ Table 'Customers' not found.")
    exit()

table_id = table_node["id"]

# Step 2: Find all HAS_COLUMN edges from this table
column_edges = edges.find({ "source": table_id, "relation": "HAS_COLUMN" })
column_ids = [edge["target"] for edge in column_edges]

# Step 3: Retrieve column names in order
columns = []
for col_id in column_ids:
    col_node = nodes.find_one({ "id": col_id })
    if col_node:
        columns.append(col_node["label"])

# Step 4: Get data from relational_data_collection for this table
rel_data_doc = relational_data.find_one({ "table_name": table_node["label"] })

if not rel_data_doc or "rows" not in rel_data_doc:
    print("❌ No data found in relational_data_collection.")
    exit()

rows = rel_data_doc["rows"]

# Step 5: Print results in tabular format
print(f"\n📄 Data from '{table_node['label']}' table:\n")
print(tabulate(rows, headers="keys", tablefmt="grid"))
