from uuid import uuid4

class GraphNode:
    def __init__(self, node_id=None, node_type="", label="", properties=None):
        self.id = node_id or str(uuid4())
        self.type = node_type
        self.label = label
        self.properties = properties or {}

    def to_dict(self):
        return {
            "id": self.id,
            "type": self.type,
            "label": self.label,
            "properties": self.properties
        }

    def save(self, collection):
        collection.update_one({"id": self.id}, {"$set": self.to_dict()}, upsert=True)


class GraphEdge:
    def __init__(self, source: str, target: str, relation: str):
        self.source = source
        self.target = target
        self.relation = relation

    def to_dict(self):
        return {
            "source": self.source,
            "target": self.target,
            "relation": self.relation
        }

    def save(self, collection):
        collection.update_one(
            {"source": self.source, "target": self.target},
            {"$set": self.to_dict()},
            upsert=True
        )
