from query_engine import QueryEngine

def run_example_queries():
    query_engine = QueryEngine()

    # Example query within the same graph
    print("Querying within the 'relational' graph:")
    within_graph_results = query_engine.query_within_graph(
        "relational", {"metadata.source_name": "example.csv"}
    )
    for result in within_graph_results:
        print(result)

    # Example query across multiple graphs
    print("\nQuerying across 'relational' and 'json' graphs:")
    across_graphs_results = query_engine.query_across_graphs(
        ["relational", "json"],
        [{"metadata.source_name": "example.csv"}, {"metadata.source_name": "example.json"}]
    )
    for result in across_graphs_results:
        print(result)

if __name__ == "__main__":
    run_example_queries()
