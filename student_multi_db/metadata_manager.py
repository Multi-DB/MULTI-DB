# Handles building and querying the metadata graph
import logging
from pymongo.database import Database
from graph_models import GraphNode, GraphEdge
from data_models import get_collection_schemas
import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MetadataManager:
    """Manages the creation and updating of the metadata graph."""

    def __init__(self, db: Database):
        self.db = db
        self.nodes_collection = db[config.NODES_COLLECTION]
        self.edges_collection = db[config.EDGES_COLLECTION]
        # Optional: Store the original schema definitions
        # self.schema_collection = db[config.METADATA_SCHEMA_COLLECTION]
        self._entity_node_cache = {} # Cache entity nodes during build {label: node_id}

    def clear_graph(self):
        """Clears existing nodes and edges."""
        logging.info("Clearing existing metadata graph...")
        self.nodes_collection.delete_many({})
        self.edges_collection.delete_many({})
        self._entity_node_cache = {}
        logging.info("Metadata graph cleared.")

    def build_graph_from_schema(self):
        """Builds the metadata graph based on schema definitions in data_models.py."""
        logging.info("Building metadata graph from schema...")
        self.clear_graph() # Start fresh
        schemas = get_collection_schemas()
        # Optional: Store schemas
        # self.schema_collection.insert_many(schemas)

        entity_nodes_to_process_fk = [] # Store (entity_node, entity_schema) for FK processing

        # --- Pass 1: Create Nodes for Entities and Fields/Columns ---
        for source_schema in schemas:
            source_name = source_schema.get("source_name")
            source_type = source_schema.get("source_type") # relational, document etc.

            # Optional: Create a node for the data source itself
            # source_node = GraphNode(node_type='datasource', label=source_name, properties={'type': source_type})
            # source_node.save(self.nodes_collection)

            for entity in source_schema.get("entities", []):
                entity_label = entity.get("label")
                entity_type = entity.get("type") # table, collection
                collection_name = entity_label # Assuming label is collection name

                # Create Entity Node
                entity_node = GraphNode(
                    node_type=entity_type,
                    label=entity_label,
                    datasource=source_name,
                    collection_name=collection_name,
                    properties={"source_type": source_type} # Store original source type if needed
                )
                entity_node.save(self.nodes_collection)
                self._entity_node_cache[entity_label] = entity_node.id
                entity_nodes_to_process_fk.append((entity_node, entity))
                logging.info(f"  Created Entity Node: {entity_node.label} (ID: {entity_node.id})")

                # Create Field/Column Nodes and Edges
                fields = entity.get("columns") or entity.get("fields", [])
                relation_type = "HAS_COLUMN" if entity_type == "table" else "HAS_FIELD"

                for field in fields:
                    field_label = field.get("label")
                    # Make field node ID unique using entity context
                    field_node_id = f"{entity_type}_{entity_label}_{field_label}".lower().replace(' ', '_')
                    field_node = GraphNode(
                        node_id=field_node_id,
                        node_type="column" if entity_type == "table" else "field",
                        label=field_label,
                        properties={k: v for k, v in field.items() if k != "label"} # Store all other props like data_type
                    )
                    field_node.save(self.nodes_collection)

                    # Create Edge: Entity -> Field
                    edge = GraphEdge(
                        source_id=entity_node.id,
                        target_id=field_node.id,
                        relation=relation_type
                    )
                    edge.save(self.edges_collection)
                    # logging.debug(f"    Created Field Node: {field_node.label} and Edge: {entity_node.label} -> {field_node.label}")


        # --- Pass 2: Create Edges for Foreign Key Relationships ---
        logging.info("Processing Foreign Key relationships...")
        for entity_node, entity_schema in entity_nodes_to_process_fk:
             fields = entity_schema.get("columns") or entity_schema.get("fields", [])
             for field in fields:
                 if field.get("is_foreign_key") and field.get("references"):
                     target_entity_label = field.get("references")
                     target_entity_node_id = self._entity_node_cache.get(target_entity_label)
                     if target_entity_node_id:
                         fk_edge = GraphEdge(
                             source_id=entity_node.id,
                             target_id=target_entity_node_id,
                             relation="REFERENCES",
                             properties={"on_field": field.get("label")}
                         )
                         fk_edge.save(self.edges_collection)
                         logging.info(f"  Created FK Edge: {entity_node.label} -> {target_entity_label} (on field: {field.get('label')})")
                     else:
                         logging.warning(f"  Could not find target entity node '{target_entity_label}' for FK relationship from '{entity_node.label}'.")


        logging.info(f"Metadata graph built successfully. Nodes: {self.nodes_collection.count_documents({})}, Edges: {self.edges_collection.count_documents({})}")

    # Potential future methods:
    # def add_entity(...)
    # def update_entity(...)
    # def add_relationship(...)