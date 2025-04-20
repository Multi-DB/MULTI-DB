def build_graph_from_schema(metadata_mgr, data_models):
    metadata_mgr.clear_graph()
    schemas = data_models.get_collection_schemas()

    # Pass 1: Create nodes for entities and fields
    for schema in schemas:
        entity_label = schema['source_name']
        entity_node = GraphNode(id=entity_label, type='collection', label=entity_label)
        metadata_mgr.save_node(entity_node)

        for field in schema['entities']:
            field_label = field['label']
            field_node = GraphNode(id=field_label, type='field', label=field_label)
            metadata_mgr.save_node(field_node)

            # Create edge between entity and field
            edge = GraphEdge(source=entity_label, target=field_label, relation='HAS_FIELD')
            metadata_mgr.save_edge(edge)

    # Pass 2: Create edges for relationships
    for schema in schemas:
        for field in schema['entities']:
            if field.get('is_foreign_key'):
                source_entity = schema['source_name']
                target_entity = field['references']
                edge = GraphEdge(source=source_entity, target=target_entity, relation='REFERENCES', properties={'on_field': field['label']})
                metadata_mgr.save_edge(edge)