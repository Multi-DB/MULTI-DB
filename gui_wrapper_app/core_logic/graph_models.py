class GraphNode:
    def __init__(self, id, type, label, properties=None, datasource=None, collection_name=None):
        self.id = id
        self.type = type
        self.label = label
        self.properties = properties if properties is not None else {}
        self.datasource = datasource
        self.collection_name = collection_name

    def to_dict(self):
        return {
            "id": self.id,
            "type": self.type,
            "label": self.label,
            "properties": self.properties,
            "datasource": self.datasource,
            "collection_name": self.collection_name
        }

    def save(self, db_collection):
        db_collection.update_one({"id": self.id}, {"$set": self.to_dict()}, upsert=True)


class GraphEdge:
    def __init__(self, source, target, relation, properties=None):
        self.source = source
        self.target = target
        self.relation = relation
        self.properties = properties if properties is not None else {}

    def to_dict(self):
        return {
            "source": self.source,
            "target": self.target,
            "relation": self.relation,
            "properties": self.properties
        }

    def save(self, db_collection):
        db_collection.update_one({"source": self.source, "target": self.target}, {"$set": self.to_dict()}, upsert=True)