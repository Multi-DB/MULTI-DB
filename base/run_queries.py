from query_engine import QueryEngine
from graph_builder import GraphBuilder

if __name__ == "__main__":
    # --- Use the market_data schema ---
    schema_file_path = "/Users/dhruvasharma/Documents/SEM2/DM/MULTI-DB/base/market_data/schema_file.json"
    graph_builder = GraphBuilder(schema_file_path)

    # Ensure data is loaded only once
    print(f"Clearing and loading data into MongoDB using schema: {schema_file_path}...")
    graph_builder.load_data_from_schema()

    # Build the graph
    print("Building the graph representation...")
    graph_builder.build_graph()

    # --- Execute queries from the market_data queries file ---
    print("Executing queries...")
    query_engine = QueryEngine()
    queries_file_path = "/Users/dhruvasharma/Documents/SEM2/DM/MULTI-DB/base/market_data/queries.json"
    print(f"Using queries file: {queries_file_path}...")
    query_engine.execute_queries_from_file(queries_file_path)
