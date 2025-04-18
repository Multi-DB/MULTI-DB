# GraphNode and GraphEdge classes
from uuid import uuid4
from pymongo.collection import Collection

class GraphNode:
    """Represents a node in the metadata graph."""
    def __init__(self, node_id=None, node_type="", label="", properties=None, datasource=None, collection_name=None):
        self.id = node_id or f"{node_type}_{label.lower().replace(' ', '_')}_{str(uuid4())[:8]}" # More descriptive default ID
        self.type = node_type  # e.g., 'database', 'schema', 'table', 'collection', 'column', 'field', 'relationship_endpoint'
        self.label = label      # User-friendly name (e.g., "Students", "FirstName")
        self.properties = properties or {} # Specific attributes (data_type, is_primary_key, etc.)
        self.datasource = datasource # Name of the source system/DB (e.g., "UniversityDB")
        self.collection_name = collection_name # Actual collection/table name in the source (e.g., "Students")

    def to_dict(self):
        # Exclude None values for cleaner storage
        data = {
            "_id": self.id, # Use id as MongoDB's _id
            "type": self.type,
            "label": self.label,
            "properties": self.properties,
        }
        if self.datasource:
            data["datasource"] = self.datasource
        if self.collection_name:
            data["collection_name"] = self.collection_name
        return data

    def save(self, collection: Collection):
        """Saves or updates the node in the specified MongoDB collection."""
        collection.update_one({"_id": self.id}, {"$set": self.to_dict()}, upsert=True)

    @staticmethod
    def from_dict(data: dict):
        """Creates a GraphNode instance from a dictionary (e.g., MongoDB doc)."""
        return GraphNode(
            node_id=data.get("_id"),
            node_type=data.get("type"),
            label=data.get("label"),
            properties=data.get("properties"),
            datasource=data.get("datasource"),
            collection_name=data.get("collection_name")
        )

class GraphEdge:
    """Represents an edge (relationship) in the metadata graph."""
    def __init__(self, source_id: str, target_id: str, relation: str, properties=None):
        self.source = source_id # ID of the source node
        self.target = target_id # ID of the target node
        self.relation = relation # Type of relationship (e.g., "HAS_COLUMN", "REFERENCES", "CONTAINS_DATA")
        self.properties = properties or {} # Additional properties of the relationship

    def to_dict(self):
        # Use a composite key or let MongoDB generate _id if uniqueness isn't strictly source-target-relation
        return {
            "source": self.source,
            "target": self.target,
            "relation": self.relation,
            "properties": self.properties
        }

    def save(self, collection: Collection):
        """Saves or updates the edge in the specified MongoDB collection."""
        # Using update_one with upsert ensures we don't create duplicates for the exact same relationship
        collection.update_one(
            {"source": self.source, "target": self.target, "relation": self.relation},
            {"$set": self.to_dict()},
            upsert=True
        )

    @staticmethod
    def from_dict(data: dict):
        """Creates a GraphEdge instance from a dictionary."""
        return GraphEdge(
            source_id=data.get("source"),
            target_id=data.get("target"),
            relation=data.get("relation"),
            properties=data.get("properties")
        )