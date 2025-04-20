# GUI Wrapper Application

## Overview

The GUI Wrapper Application is designed to provide a user-friendly interface around the Unified Multi-Source Data Querying Framework. This application allows users to interact with various data sources, ingest data, and perform queries through a graphical interface without modifying the underlying core logic.

## Project Structure

```
gui_wrapper_app
├── core_logic
│   ├── config.py
│   ├── data_manager.py
│   ├── data_models.py
│   ├── graph_models.py
│   ├── main.py
│   ├── metadata_manager.py
│   └── query_parser.py
├── gui
│   ├── __init__.py
│   ├── main_window.py
│   └── widgets
│       └── __init__.py
├── main_gui.py
├── requirements.txt
└── README.md
```

## Installation

To set up the project, clone the repository and install the required dependencies:

1. Clone the repository:
   ```
   git clone <repository-url>
   cd gui_wrapper_app
   ```

2. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

## Usage

To run the GUI application, execute the following command:

```
python main_gui.py
```

This will launch the main window of the application, where you can interact with the data ingestion and querying functionalities.

## Features

- Ingest data from various formats (CSV, XML, JSON) into MongoDB.
- Unified querying across multiple datasets using a metadata graph.
- User-friendly interface for executing queries and viewing results.

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue for any enhancements or bug fixes.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.