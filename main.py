from database import DatabaseManager

def main():
    # Initialize database manager
    db_manager = DatabaseManager()
    
    # Reset database
    db_manager.drop_all_collections()
    
    # Create collections with schema validation
    db_manager.create_collections()
    
    # Insert sample data
    db_manager.insert_sample_data()
    
    # Print verification data
    print("Database setup complete with sample data.")
    db_manager.print_sample_data()

if __name__ == "__main__":
    main()