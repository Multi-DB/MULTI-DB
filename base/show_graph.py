from connection import get_mongo_connection

def show_graph():
    """
    Fetch and display the single dynamic graph stored in the 'Graph' collection.
    """
    try:
        db = get_mongo_connection()
        graph = db["Graph"].find_one()

        if graph:
            print("Dynamic Graph:")
            print("\nNodes:")
            for node in graph.get("nodes", []):
                print(f"  ID: {node['id']}, Entity: {node['entity']}, Data: {node['data']}")

            print("\nEdges:")
            for edge in graph.get("edges", []):
                print(f"  Source: {edge['source']}, Target: {edge['target']}, Relationship: {edge['relationship']}")
        else:
            print("No graph found in the 'Graph' collection.")
    except Exception as e:
        print(f"Error displaying graph: {e}")
        raise

if __name__ == "__main__":
    show_graph()
