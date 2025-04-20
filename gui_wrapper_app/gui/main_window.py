from PyQt5.QtWidgets import QApplication, QMainWindow, QVBoxLayout, QWidget, QPushButton, QLabel
import sys
from core_logic.data_manager import DataManager
from core_logic.metadata_manager import MetadataManager

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Unified Multi-Source Data Querying Framework")
        self.setGeometry(100, 100, 800, 600)

        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)

        self.layout = QVBoxLayout()
        self.central_widget.setLayout(self.layout)

        self.label = QLabel("Welcome to the Unified Multi-Source Data Querying Framework")
        self.layout.addWidget(self.label)

        self.ingest_button = QPushButton("Ingest Data")
        self.ingest_button.clicked.connect(self.ingest_data)
        self.layout.addWidget(self.ingest_button)

        self.query_button = QPushButton("Run Example Queries")
        self.query_button.clicked.connect(self.run_example_queries)
        self.layout.addWidget(self.query_button)

        self.status_label = QLabel("")
        self.layout.addWidget(self.status_label)

        self.data_manager = DataManager()
        self.metadata_manager = MetadataManager()

    def ingest_data(self):
        self.status_label.setText("Ingesting data...")
        # Call the data ingestion logic here
        # Example: self.data_manager.ingest_and_process_file("StudentClubs", "sample_data/student_clubs.xml")
        self.status_label.setText("Data ingestion completed.")

    def run_example_queries(self):
        self.status_label.setText("Running example queries...")
        # Call the query execution logic here
        # Example: results = self.data_manager.retrieve_data(query_dict)
        self.status_label.setText("Example queries executed.")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    main_window = MainWindow()
    main_window.show()
    sys.exit(app.exec_())