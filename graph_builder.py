from pymongo import MongoClient
from graph_models import GraphNode, GraphEdge

def build_graph_from_metadata(metadata_collection, nodes_collection, edges_collection):
    documents = metadata_collection.find()
    
    for doc in documents:
        source_type = doc.get("source_type")
        source_name = doc.get("source_name")
        entities = doc.get("entities", [])

        for entity in entities:
            entity_type = entity.get("type")  # table, element, object
            entity_label = entity.get("label")
            entity_id = f"{entity_type}_{entity_label.lower()}"
            entity_node = GraphNode(entity_id, entity_type, entity_label)
            entity_node.save(nodes_collection)

            if entity_type == "table":  # relational
                for col in entity.get("columns", []):
                    col_label = col.get("label")
                    col_id = f"column_{col_label.lower()}_{entity_label.lower()}"
                    col_node = GraphNode(col_id, "column", col_label, properties=col)
                    col_node.save(nodes_collection)

                    edge = GraphEdge(entity_id, col_id, "HAS_COLUMN")
                    edge.save(edges_collection)

            elif entity_type == "element":  # xml
                for child in entity.get("children", []):
                    child_label = child.get("label")
                    child_id = f"element_{child_label.lower()}_{entity_label.lower()}"
                    child_node = GraphNode(child_id, "element", child_label, properties=child)
                    child_node.save(nodes_collection)

                    edge = GraphEdge(entity_id, child_id, "PARENT_CHILD")
                    edge.save(edges_collection)

            elif entity_type == "object":  # json
                for prop in entity.get("properties", []):
                    prop_label = prop.get("label")
                    prop_id = f"property_{prop_label.lower()}_{entity_label.lower()}"
                    prop_node = GraphNode(prop_id, "primitive", prop_label, properties=prop)
                    prop_node.save(nodes_collection)

                    edge = GraphEdge(entity_id, prop_id, "PROPERTY_OF")
                    edge.save(edges_collection)

    print("✅ Metadata graph built successfully.")

# MongoDB Setup
mongo_connection_string = "mongodb://localhost:27017/"
db_name = "multidb_snapshot"

client = MongoClient(mongo_connection_string)
db = client[db_name]

metadata_collection = db["metadata_collection"]
nodes_collection = db["nodes"]
edges_collection = db["edges"]

# Run graph builder
build_graph_from_metadata(metadata_collection, nodes_collection, edges_collection)
