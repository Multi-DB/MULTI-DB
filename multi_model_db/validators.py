def get_collection_validators():
    return {
        "relational_data_collection": {
            "validator": {
                "$jsonSchema": {
                    "bsonType": "object",
                    "required": ["id", "type", "label", "properties"],
                    "properties": {
                        "id": {"bsonType": "string"},
                        "type": {"enum": ["table", "column", "record"]},
                        "label": {"bsonType": "string"},
                        "properties": {
                            "bsonType": "object",
                            "properties": {
                                "CustomerID": {"bsonType": "int"},
                                "Name": {"bsonType": "string"},
                                "Email": {"bsonType": "string"}
                            }
                        }
                    }
                }
            }
        },
        "xml_data_collection": {
            "validator": {
                "$jsonSchema": {
                    "bsonType": "object",
                    "required": ["id", "type", "label", "properties"],
                    "properties": {
                        "id": {"bsonType": "string"},
                        "type": {"enum": ["element", "order"]},
                        "label": {"bsonType": "string"},
                        "properties": {
                            "bsonType": "object",
                            "properties": {
                                "order_id": {"bsonType": "string"},
                                "order_date": {"bsonType": "string"},
                                "customer_ref": {"bsonType": "string"}
                            }
                        }
                    }
                }
            }
        },
        "json_data_collection": {
            "validator": {
                "$jsonSchema": {
                    "bsonType": "object",
                    "required": ["id", "type", "label", "properties"],
                    "properties": {
                        "id": {"bsonType": "string"},
                        "type": {"enum": ["object", "vendor"]},
                        "label": {"bsonType": "string"},
                        "properties": {
                            "bsonType": "object",
                            "properties": {
                                "vendor_id": {"bsonType": "string"},
                                "vendor_name": {"bsonType": "string"}
                            }
                        }
                    }
                }
            }
        },
        "metadata_graph_collection": {
            "validator": {
                "$jsonSchema": {
                    "bsonType": "object",
                    "required": ["nodes", "edges"],
                    "properties": {
                        "nodes": {
                            "bsonType": "array",
                            "items": {
                                "required": ["id", "type", "label", "datasource"],
                                "properties": {
                                    "id": {"bsonType": "string"},
                                    "type": {"enum": ["table", "xml", "json", "column", "element", "property", "graph"]},
                                    "label": {"bsonType": "string"},
                                    "datasource": {"enum": ["relational", "xml", "json", "metadata"]}
                                }
                            }
                        },
                        "edges": {
                            "bsonType": "array",
                            "items": {
                                "required": ["source", "target", "relation"],
                                "properties": {
                                    "source": {"bsonType": "string"},
                                    "target": {"bsonType": "string"},
                                    "relation": {"bsonType": "string"}
                                }
                            }
                        }
                    }
                }
            }
        }
    }
