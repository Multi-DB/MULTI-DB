# Parses queries and orchestrates retrieval using graph traversal
# Basic conceptual query parser. A real implementation would be more complex.
# For now, we construct the query_details dictionary directly in main.py

def parse_query(query_string: str) -> dict | None:
    """
    (Conceptual) Parses a natural language or simple structured query.
    This is a placeholder. Actual implementation requires NLP or a defined syntax.
    """
    # Example pseudo-parsing:
    query_string = query_string.lower().strip()

    if query_string.startswith("get students"):
         details = {"action": "get_entity", "entity": "Students"}
         if "name" in query_string and "gpa" in query_string:
              details["fields"] = ["FirstName", "LastName", "GPA"]
         elif "name" in query_string:
              details["fields"] = ["FirstName", "LastName"]
         return details

    if query_string.startswith("get courses"):
        return {"action": "get_entity", "entity": "Courses"}

    if "find student" in query_string and "courses" in query_string:
         # Example: "find student john smith courses"
         parts = query_string.split()
         # Very basic name extraction
         first_name = parts[2].capitalize() if len(parts) > 2 else None
         last_name = parts[3].capitalize() if len(parts) > 3 else None

         if first_name and last_name:
              return {
                  "action": "get_related",
                  "start_entity": "Students",
                  "start_filters": {"FirstName": first_name, "LastName": last_name},
                  "relations": [
                      {"relation": "REFERENCES", "direction": "in", "target_entity": "Enrollments"},
                      {"relation": "REFERENCES", "direction": "out", "target_entity": "Courses"}
                  ],
                  "final_fields": {"Students": ["FirstName", "LastName"], "Courses": ["CourseName", "CourseCode"]}
              }

    # Add more parsing rules here...

    print(f"❌ Could not parse query: '{query_string}'")
    return None