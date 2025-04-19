# metadata_manager.py
import logging
from pymongo.database import Database
from graph_models import GraphNode, GraphEdge
from data_models import get_collection_schemas
import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MetadataManager:
    """Manages the creation and updating of the logical entity metadata graph."""

    def __init__(self, db: Database):
        self.db = db
        self.nodes_collection = db[config.NODES_COLLECTION]
        self.edges_collection = db[config.EDGES_COLLECTION]
        self._entity_node_cache = {} # Cache {entity_label: node_id}

    def clear_graph(self):
        logging.info("Clearing existing metadata graph...")
        self.nodes_collection.delete_many({})
        self.edges_collection.delete_many({})
        self._entity_node_cache = {}
        logging.info("Metadata graph cleared.")

    def build_graph_from_schema(self):
        """Builds the metadata graph based on logical entities defined in data_models.py."""
        logging.info("Building metadata graph from schema...")
        self.clear_graph()
        schemas = get_collection_schemas()

        entity_nodes_to_process_fk = [] # Store (entity_node, entity_schema)

        # --- Pass 1: Create Nodes for Logical Entities and their Fields ---
        for source_schema in schemas:
            source_name = source_schema.get("source_name")
            source_type = source_schema.get("source_type")

            for entity in source_schema.get("entities", []):
                entity_label = entity.get("label") # e.g., "Customers", "Books", "UserProfiles"
                # Use 'collection' as the generic node type for queryable entities in MongoDB
                entity_node_type = "collection"
                collection_name = entity_label # Target MongoDB collection name

                # Create Entity Node (representing the MongoDB collection)
                entity_node = GraphNode(
                    node_id=f"{entity_node_type}_{entity_label}".lower().replace(' ', '_'),
                    node_type=entity_node_type,
                    label=entity_label,
                    datasource=source_name,
                    collection_name=collection_name,
                    properties={"source_system_type": source_type, "original_entity_type": entity.get("type")}
                )
                entity_node.save(self.nodes_collection)
                self._entity_node_cache[entity_label] = entity_node.id
                entity_nodes_to_process_fk.append((entity_node, entity))
                logging.info(f"  Created Entity Node: {entity_node.label} (Collection: {collection_name}, Source: {source_type}, ID: {entity_node.id})")

                # Determine fields based on entity type (columns or fields)
                fields = entity.get("columns", []) or entity.get("fields", [])
                relation_type = "HAS_FIELD" # Unified relation type for entity->field
                field_node_type = "field"   # Unified node type for fields in collections

                for field in fields:
                    field_label = field.get("label") # This is the target field name in MongoDB
                    field_node_id = f"{entity_node.id}_{field_label}".lower().replace(' ', '_')

                    # Store all schema properties except label (data_type, is_pk, is_fk, references, xpath, json_path etc.)
                    field_props = {k: v for k, v in field.items() if k != "label"}

                    field_node = GraphNode(
                        node_id=field_node_id,
                        node_type=field_node_type,
                        label=field_label,
                        properties=field_props
                    )
                    field_node.save(self.nodes_collection)

                    # Create Edge: Entity -> Field
                    edge = GraphEdge(
                        source_id=entity_node.id,
                        target_id=field_node.id,
                        relation=relation_type
                    )
                    edge.save(self.edges_collection)

        # --- Pass 2: Create Edges for Foreign Key Relationships (REFERENCES) ---
        logging.info("Processing Foreign Key relationships...")
        for entity_node, entity_schema in entity_nodes_to_process_fk:
            fields = entity_schema.get("columns", []) or entity_schema.get("fields", [])
            for field in fields:
                 # Look for the FK definition in the field's schema
                 if field.get("is_foreign_key") and field.get("references"):
                     fk_field_label = field.get("label") # Field holding the FK value (e.g., CustomerID in Orders)
                     target_entity_label = field.get("references") # Target entity label (e.g., Customers)
                     target_entity_node_id = self._entity_node_cache.get(target_entity_label)

                     if target_entity_node_id:
                         # Edge goes FROM the entity containing the FK TO the referenced entity's PK
                         fk_edge = GraphEdge(
                             source_id=entity_node.id, # e.g., Orders node
                             target_id=target_entity_node_id, # e.g., Customers node
                             relation="REFERENCES",
                             # Store the name of the field in the source entity that holds the FK value
                             properties={"on_field": fk_field_label}
                         )
                         fk_edge.save(self.edges_collection)
                         logging.info(f"  Created FK Edge: {entity_node.label} -> {target_entity_label} (on field: {fk_field_label})")
                     else:
                         logging.warning(f"  Could not find target entity node '{target_entity_label}' for FK relationship from '{entity_node.label}' on field '{fk_field_label}'.")

        logging.info(f"Metadata graph built successfully. Nodes: {self.nodes_collection.count_documents({})}, Edges: {self.edges_collection.count_documents({})}")