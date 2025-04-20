from gui.main_window import MainWindow
from core_logic.config import MongoDB_URI, Database_Name
from core_logic.metadata_manager import MetadataManager
from core_logic.data_manager import DataManager
from core_logic.data_models import get_collection_schemas
from pymongo import MongoClient
import sys
from PyQt5.QtWidgets import QApplication

def main():
    # Connect to MongoDB
    client = MongoClient(MongoDB_URI)
    db = client[Database_Name]

    # Initialize Managers
    metadata_manager = MetadataManager(db)
    data_manager = DataManager(db)

    # Run Setup & Ingestion
    metadata_manager.build_graph_from_schema()
    data_manager.setup_collections_with_validation()

    # Create the GUI application
    app = QApplication(sys.argv)
    main_window = MainWindow(data_manager)
    main_window.show()

    # Execute the application
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()