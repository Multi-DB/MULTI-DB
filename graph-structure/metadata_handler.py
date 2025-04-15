from graph_models import GraphNode, GraphEdge
from tabulate import tabulate

class MetadataHandler:
    def __init__(self, db):
        self.metadata_collection = db["metadata_collection"]
        self.nodes = db["nodes"]
        self.edges = db["edges"]
        self.relational_data = db["relational_data_collection"]
        self.xml_data = db["xml_data_collection"]
        self.json_data = db["json_data_collection"]

    def build_graph(self):
        documents = self.metadata_collection.find()
        for doc in documents:
            source_type = doc.get("source_type")
            source_name = doc.get("source_name")
            entities = doc.get("entities", [])

            for entity in entities:
                entity_type = entity.get("type")
                entity_label = entity.get("label")
                entity_id = f"{entity_type}_{entity_label.lower()}"
                entity_node = GraphNode(entity_id, entity_type, entity_label)
                entity_node.save(self.nodes)

                if entity_type == "table":
                    for col in entity.get("columns", []):
                        self._create_field_node(col, "column", entity_id, entity_label, "HAS_COLUMN")

                elif entity_type == "element":
                    for child in entity.get("children", []):
                        self._create_field_node(child, "element", entity_id, entity_label, "HAS_CHILD")

                elif entity_type == "object":
                    for field in entity.get("fields", []):
                        self._create_field_node(field, "field", entity_id, entity_label, "HAS_FIELD")

    def _create_field_node(self, field, field_type, parent_id, parent_label, relation):
        field_label = field.get("label")
        field_id = f"{field_type}_{field_label.lower()}_{parent_label.lower()}"
        field_node = GraphNode(field_id, field_type, field_label, properties=field)
        field_node.save(self.nodes)
        edge = GraphEdge(parent_id, field_id, relation)
        edge.save(self.edges)

    def retrieve_data(self, source_type, entity_label):
        entity_type = {
            "relational": "table",
            "xml": "element",
            "json": "object"
        }[source_type]

        relation = {
            "relational": "HAS_COLUMN",
            "xml": "HAS_CHILD",
            "json": "HAS_FIELD"
        }[source_type]

        field_key = {
            "relational": "rows",
            "xml": "entries",
            "json": "records"
        }[source_type]

        data_collection = {
            "relational": self.relational_data,
            "xml": self.xml_data,
            "json": self.json_data
        }[source_type]

        node = self.nodes.find_one({ "type": entity_type, "label": entity_label })
        if not node:
            print(f"❌ Entity '{entity_label}' not found.")
            return

        node_id = node["id"]
        edges = self.edges.find({ "source": node_id, "relation": relation })
        field_ids = [edge["target"] for edge in edges]

        fields = []
        for fid in field_ids:
            field_node = self.nodes.find_one({ "id": fid })
            if field_node:
                fields.append(field_node["label"])

        doc = data_collection.find_one({ f"{entity_type}_name": entity_label })
        if not doc or field_key not in doc:
            print(f"❌ No data found for '{entity_label}' in {source_type}_data_collection.")
            return

        print(f"\n📄 Data from '{entity_label}' ({source_type}):\n")
        print(tabulate(doc[field_key], headers="keys", tablefmt="grid"))
