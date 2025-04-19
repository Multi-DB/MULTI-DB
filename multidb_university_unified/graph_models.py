# graph_models.py
# (Keep the existing GraphNode and GraphEdge classes as they are)
from uuid import uuid4
from pymongo.collection import Collection

class GraphNode:
    """Represents a node in the metadata graph."""
    def __init__(self, node_id=None, node_type="", label="", properties=None, datasource=None, collection_name=None):
        _id_label_part = label.lower().replace(' ', '_').replace('.', '_')
        default_id = f"{node_type}_{_id_label_part}_{str(uuid4())[:8]}"
        self.id = node_id or default_id
        self.type = node_type  # e.g., 'table', 'collection', 'xml_entity', 'json_object' (can map to 'collection' type)
        self.label = label      # Logical Entity Name (e.g., "Customers", "Books")
        self.properties = properties or {} # Specific attributes of the *entity* if any
        self.datasource = datasource # Name of the original source system (e.g., "SalesDB", "BookstoreFeed")
        self.collection_name = collection_name # Actual *target* MongoDB collection name (e.g., "Customers", "Books")

    def to_dict(self):
        data = {
            "_id": self.id,
            "type": self.type,
            "label": self.label,
            "properties": self.properties,
        }
        if self.datasource: data["datasource"] = self.datasource
        if self.collection_name: data["collection_name"] = self.collection_name
        return data

    def save(self, collection: Collection):
        collection.update_one({"_id": self.id}, {"$set": self.to_dict()}, upsert=True)

    @staticmethod
    def from_dict(data: dict):
        return GraphNode(
            node_id=data.get("_id"), node_type=data.get("type"), label=data.get("label"),
            properties=data.get("properties"), datasource=data.get("datasource"),
            collection_name=data.get("collection_name")
        )

class GraphEdge:
    """Represents an edge (relationship) in the metadata graph."""
    def __init__(self, source_id: str, target_id: str, relation: str, properties=None):
        self.source = source_id
        self.target = target_id
        self.relation = relation # e.g., "HAS_FIELD", "REFERENCES"
        self.properties = properties or {} # e.g., {"on_field": "CustomerID"} for REFERENCES

    def to_dict(self):
        return {
            "source": self.source, "target": self.target,
            "relation": self.relation, "properties": self.properties
        }

    def save(self, collection: Collection):
        # Ensure only one edge of a specific type exists between source and target
        collection.update_one(
            {"source": self.source, "target": self.target, "relation": self.relation},
            {"$set": self.to_dict()},
            upsert=True
        )

    @staticmethod
    def from_dict(data: dict):
        return GraphEdge(
            source_id=data.get("source"), target_id=data.get("target"),
            relation=data.get("relation"), properties=data.get("properties")
        )