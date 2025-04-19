# Code Logic and Flow Explanation

## 1. Overview

This document details the internal logic and execution flow of the Unified Multi-Source Data Querying Framework. The primary goal is to ingest data from various formats (CSV, XML, JSON), store it consistently in MongoDB, and enable unified querying across these datasets using a metadata graph.

The core components interact as follows:

*   **`data_models.py`**: Defines the expected structure of source data, target MongoDB structure, relationships, and provides hints for parsing.
*   **`metadata_manager.py`**: Reads schemas from `data_models.py` and builds a structural map (metadata graph) in MongoDB.
*   **`data_manager.py`**: Uses schemas for parsing, interacts with MongoDB to store/retrieve actual data, and uses the metadata graph to plan and execute queries (especially joins).
*   **`main.py`**: Orchestrates the entire process from setup and ingestion to running example queries.
*   **`config.py`**: Holds database connection details.
*   **`graph_models.py`**: Defines Python classes for graph nodes/edges (used by `MetadataManager`).

## 2. High-Level Execution Flow (`main.py`)

When `python main.py` is executed:

1.  **Connect to MongoDB**: Establishes a connection using details from `config.py`.
2.  **Initialize Managers**: Creates instances of `MetadataManager` and `DataManager`, passing the DB connection.
3.  **Run Setup & Ingestion (`run_setup_and_ingestion`):**
    *   **Build Metadata Graph**: Calls `metadata_mgr.build_graph_from_schema()`. This clears existing graph data and rebuilds it based on `data_models.py`.
    *   **Setup Collections**: Calls `data_mgr.setup_collections_with_validation()`. This creates MongoDB collections (if needed), applies validation rules (from `data_models.py`), and ensures unique indexes on primary keys (identified via metadata graph).
    *   **Ingest Data**: Iterates through `SAMPLE_FILES` defined in `main.py`. For each file:
        *   Checks if the corresponding target MongoDB collection is empty *or* if `force_reingest` is `True`.
        *   If ingestion is needed, calls `data_mgr.ingest_and_process_file()`.
        *   If data exists and `force_reingest` is `False`, skips ingestion for that file.
4.  **Run Example Queries**:
    *   Defines several query dictionaries specifying actions (`get_entity`, `get_related`), targets, filters, relations, etc.
    *   Calls `data_mgr.retrieve_data()` for each query.
    *   Prints the formatted results.
5.  **Close Connection**: Closes the MongoDB connection.

## 3. Module Breakdown and Logic

### 3.1. `config.py`

*   **Purpose**: Stores configuration constants.
*   **Logic**: Simple variable assignments for MongoDB URI, database name, and metadata collection names. Easily modified without changing core code.

### 3.2. `data_models.py`

*   **Purpose**: The central definition hub. Defines source structure, target structure, relationships, and validation rules.
*   **Key Functions**:
    *   `get_collection_schemas()`: Returns a list of dictionaries, each representing a data source (`source_type`, `source_name`). Inside each source are `entities` (logical data groups like "Students", "StudentClubs").
        *   **Entity Definition**: Specifies `label` (target collection name), `type` (original structure type like `table`, `xml_structure`, `json_objects`), and fields/columns.
        *   **Field/Column Definition**: Specifies `label` (target field name in MongoDB), `data_type` (used for parsing/validation), `is_primary_key`, `is_foreign_key`, `references` (for relationships), and **parsing hints** (`xpath`, `json_path`).
    *   `get_mongo_validation_schemas()`: Returns a dictionary mapping entity labels (collection names) to MongoDB JSON Schema validation rules. This defines the *target* structure enforced in MongoDB.
    *   `find_schema_for_entity()`: A helper to quickly look up the schema definition for a given entity label.

### 3.3. `graph_models.py`

*   **Purpose**: Defines Python classes representing nodes and edges before they are stored in MongoDB.
*   **Classes**:
    *   `GraphNode`: Represents entities (logical collections) or fields. Attributes include `id`, `type`, `label`, `properties`, `datasource`, `collection_name`.
    *   `GraphEdge`: Represents relationships (`HAS_FIELD`, `REFERENCES`). Attributes include `source` (node ID), `target` (node ID), `relation`, `properties` (e.g., `on_field` for FKs).
*   **Logic**: Simple data containers with `to_dict()` methods for serialization and `save()` methods for upserting into MongoDB collections.

### 3.4. `metadata_manager.py`

*   **Purpose**: Builds the metadata graph that describes the *logical structure* and *relationships* between the data stored in MongoDB.
*   **Key Functions**:
    *   `build_graph_from_schema()`:
        1.  Clears existing graph collections.
        2.  Reads schemas via `data_models.get_collection_schemas()`.
        3.  **Pass 1 (Nodes & Fields):**
            *   For each entity, creates a `GraphNode` (type `collection`) representing the target MongoDB collection. Stores source info in properties.
            *   For each field/column within the entity, creates a `GraphNode` (type `field`). Stores `data_type`, parsing hints, key info etc., in its properties.
            *   Creates `HAS_FIELD` `GraphEdge`s linking the entity node to its field nodes.
            *   Saves all nodes and `HAS_FIELD` edges to MongoDB (`metagraph_nodes`, `metagraph_edges`).
        4.  **Pass 2 (Relationships):**
            *   Iterates through entities and their fields again.
            *   If a field is marked `is_foreign_key: True` with a `references` attribute:
                *   Finds the source entity node ID and the target entity node ID.
                *   Creates a `REFERENCES` `GraphEdge` from the source (FK holder) entity node to the target (PK holder) entity node.
                *   Crucially, stores the name of the foreign key field itself in the edge's properties: `{"on_field": "StudentID"}`.
                *   Saves the `REFERENCES` edge to MongoDB.

### 3.5. `data_manager.py`

*   **Purpose**: The workhorse for interacting with actual data - parsing source files, storing data in MongoDB, and executing queries using the metadata graph.
*   **Key Functions**:
    *   `setup_collections_with_validation()`: Prepares the target MongoDB data collections. Ensures they exist, applies validation rules from `data_models`, and creates unique indexes on primary keys (PK field name is found by querying the metadata graph).
    *   `ingest_and_process_file()`: Orchestrates ingestion for one file.
        1.  Finds the entity schema using `find_schema_for_entity`.
        2.  Determines the correct parser (`_parse_csv`, `_parse_xml`, `_parse_json`) based on the `type` defined in the entity schema (`table`, `xml_structure`, `json_objects`).
        3.  Calls the selected parser.
        4.  If parsing succeeds, calls `bulk_write` with `UpdateOne` operations (using `upsert=True`) to efficiently insert/update data in the target MongoDB collection, handling duplicates based on the primary key.
    *   `_parse_csv()`, `_parse_xml()`, `_parse_json()`:
        1.  Open and read the respective file format.
        2.  Use the parsing hints (`columns`, `xpath_base`, `xpath`, `json_path`) from the entity schema to locate and extract data for each record/element.
        3.  For each extracted value, call `_convert_type` to standardize the data type.
        4.  Return a list of Python dictionaries, each representing a record formatted for MongoDB.
    *   `_convert_type()`: Takes a raw extracted value and the target `data_type` string from the schema. Uses `if/elif` logic and libraries like `dateutil.parser` to convert the value to the appropriate Python type (int, float, bool, datetime, list, str). Handles potential conversion errors.
    *   `retrieve_data()`: Entry point for queries. Delegates based on the `action`.
    *   `_retrieve_single_entity()`: Handles simple queries on one entity. Finds the collection name via metadata, builds projection, runs `collection.find()`, and formats output.
    *   `_retrieve_related_data()`: Handles multi-entity queries (joins).
        1.  **Plan:** Uses the metadata graph (`_get_relationship_link_fields`) to determine join keys (`on_field` from `REFERENCES` edge, PK from related entity) for each step in the `relations` list.
        2.  **Fetch:** Queries the MongoDB collection for the starting entity, projecting necessary fields. Then, iteratively fetches data from related collections using `$in` filters based on the join keys identified in the plan.
        3.  **Join:** Performs in-memory joins between the fetched datasets based on the identified join keys. Handles potential one-to-many relationships.
        4.  **Format:** Cleans the joined data to include only requested fields (prefixed with entity label) and displays using `tabulate`.
    *   Helper methods (`_find_primary_key...`, `_get_relationship_link_fields`, etc.): Internal functions to query the metadata graph or prepare query components.

### 3.6. `query_parser.py`

*   **Purpose**: (Conceptual) Intended to translate natural language or a DSL into the structured query dictionaries used by `DataManager`.
*   **Logic**: Currently contains placeholder logic and is not actively used. Queries are manually defined in `main.py`.

## 4. Data Flow Examples

### 4.1. Ingestion Flow (e.g., `student_clubs.xml`)

1.  `main.py` calls `data_mgr.ingest_and_process_file("StudentClubs", "sample_data/student_clubs.xml")`.
2.  `ingest_and_process_file` calls `find_schema_for_entity("StudentClubs")` -> Gets schema with `type: xml_structure`, `xpath_base: "//Membership"`, and field definitions with `xpath` hints.
3.  It selects and calls `_parse_xml("sample_data/student_clubs.xml", schema)`.
4.  `_parse_xml` opens the file, finds all `<Membership>` elements using `root.findall(".//Membership")`.
5.  For each `<Membership>` element:
    *   It iterates through the `fields` in the schema (MembershipID, StudentID, ClubName, etc.).
    *   For "MembershipID", it uses `xpath: "@id"` -> Extracts the "id" attribute value ("CLUB001"). Calls `_convert_type("CLUB001", "STRING")` -> returns "CLUB001".
    *   For "StudentID", it uses `xpath: "@studentId"` -> Extracts "1001". Calls `_convert_type("1001", "INT")` -> returns `1001`.
    *   For "ClubName", it uses `xpath: "ClubName"` -> Finds the child `<ClubName>` element, gets its text. Calls `_convert_type("Computer Science Society", "STRING")` -> returns "Computer Science Society".
    *   ...and so on for other fields using their respective `xpath` hints.
    *   Builds a dictionary like `{"MembershipID": "CLUB001", "StudentID": 1001, ...}`.
6.  `_parse_xml` returns a list of these dictionaries.
7.  `ingest_and_process_file` calls `_find_primary_key_field_by_label("StudentClubs")` -> Gets "MembershipID".
8.  It prepares `UpdateOne` operations for `bulk_write` using "MembershipID" as the filter key and `upsert=True`.
9.  `bulk_write` inserts/updates the records in the `StudentClubs` MongoDB collection.

### 4.2. Query Flow (e.g., Query 4: Student -> Enrollments -> Courses)

1.  `main.py` calls `data_mgr.retrieve_data(query4_dict)`.
2.  `retrieve_data` calls `_retrieve_related_data(query4_dict)`.
3.  **Fetch Start:**
    *   Finds `Students` node via metadata.
    *   Determines link fields for `Students` -> `Enrollments` (`direction: "in"`). `_get_relationship_link_fields` looks for `REFERENCES` edge between them. Finds `Enrollments -> Students (on StudentID)`. Since direction is "in", it knows `Students` uses its PK (`StudentID`) and `Enrollments` uses its FK (`StudentID`). Start entity link field is `StudentID`.
    *   Fetches `Students` data where `StudentID: 1001`, projecting requested fields (`FirstName`, `LastName`) plus the link field (`StudentID`). Result: `[{StudentID: 1001, FirstName: 'John', LastName: 'Smith'}]`. Stores this in `processed_results`.
4.  **Traverse Step 1 (-> Enrollments):**
    *   Current entity is `Students`. Target is `Enrollments`. Direction "in".
    *   Link info (as determined above): `start_field: StudentID`, `target_field: StudentID`.
    *   Collect link values from previous step: `{1001}`.
    *   Determine link fields for *next* step (`Enrollments` -> `Courses`, `direction: "out"`). Finds edge `Enrollments -> Courses (on CourseID)`. Since direction is "out", `Enrollments` uses FK (`CourseID`), `Courses` uses PK (`CourseID`). Next link field needed from `Enrollments` is `CourseID`.
    *   Query `Enrollments` collection where `StudentID: {"$in": [1001]}`, projecting requested fields (`Semester`, `Year`, `Grade`) + link field (`StudentID`) + next link field (`CourseID`). Result: `[{EnrollmentID: 1, StudentID: 1001, CourseID: 101, ...}, {EnrollmentID: 2, StudentID: 1001, CourseID: 109, ...}]`. Stores this.
5.  **Traverse Step 2 (-> Courses):**
    *   Current entity is `Enrollments`. Target is `Courses`. Direction "out".
    *   Link info (as determined above): `start_field: CourseID`, `target_field: CourseID`.
    *   Collect link values from previous step (`Enrollments` results): `{101, 109}`.
    *   No next step.
    *   Query `Courses` collection where `CourseID: {"$in": [101, 109]}`, projecting requested fields (`CourseName`, `CourseCode`) + link field (`CourseID`). Result: `[{CourseID: 101, CourseName: 'Intro...', ...}, {CourseID: 109, CourseName: 'Data Struct...', ...}]`. Stores this.
6.  **Join Results:**
    *   Start with formatted `Students` data: `[{"Students.FirstName": "John", "Students.LastName": "Smith", "Students.StudentID": 1001}]`.
    *   Join with `Enrollments` data on `Students.StudentID == Enrollments.StudentID`. Produces 2 intermediate records, adding `Enrollments` fields and the next link key `Enrollments.CourseID`.
    *   Join intermediate results with `Courses` data on `Enrollments.CourseID == Courses.CourseID`. Produces 2 final records, adding `Courses` fields.
7.  **Format & Display:** Cleans the final records to include only headers derived from `final_fields`, then prints using `tabulate`.
