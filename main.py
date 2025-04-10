from multi_model_db.db_manager import MultiModelDB

if __name__ == "__main__":
    db = MultiModelDB()
    status = db.connection_status

    print("\nDatabase Status:")
    print(f"MongoDB Connected: {'✅' if status['mongodb_connected'] else '❌'}")
    print(f"Collections Initialized: {'✅' if status['collections_initialized'] else '❌'}")
    print(f"System Ready: {'✅' if status.get('ready') else '❌'}")

    if status['mongodb_connected']:
        print("\nCollection Status:")
        for col in status['initialized_collections']:
            emoji = '✅' if col['status'] in ['created', 'exists'] else '❌'
            print(f"{col['name']}: {emoji} ({col['status']})")
            if 'error' in col:
                print(f"   Error: {col['error']}")

    if status.get('ready'):
        db.initialize_metadata_graph()
        db.insert_sample_data()

        print("\nInserted Documents:")
        print(f"Customers: {db.collections['relational'].count_documents({})}")
        print(f"Orders: {db.collections['xml'].count_documents({})}")
        print(f"Vendors: {db.collections['json'].count_documents({})}")

        graph = db.collections["metadata_graph"].find_one()
        if graph:
            print("\nMetadata Graph Relationships:")
            for edge in graph.get("edges", []):
                print(f"{edge['source']} --{edge['relation']}--> {edge['target']}")

            print("\nNode Datasources:")
            for node in graph.get("nodes", []):
                print(f"{node['label']} ({node['type']}): {node.get('datasource', 'MISSING')}")
        else:
            print("⚠️ Metadata graph not found.")
    db.close()
