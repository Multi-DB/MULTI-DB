import tkinter as tk
from tkinter import scrolledtext
from tkinter import messagebox # For showing errors

def check_mongo_connection_status():
    """
    Checks the connection status to MongoDB.
    Replace this with a call to your MongoDBStorageManager.
    Returns True if connected, False otherwise.
    """
    print("Checking MongoDB connection (placeholder)...")
    # Simulate connection status (replace with actual check)
    # Example: return storage_manager.is_connected()
    import random
    return random.choice([True, False])

# Placeholder for your query processing logic
def process_the_query(query_string):
    """
    Processes the user's query string.
    Replace this with a call to your MetadataQueryEngine.process_query()
    Should return a string containing the formatted table result or an error message.
    """
    print(f"Processing query (placeholder): {query_string}")
    if not query_string.strip():
        return "Error: Please enter a query."

    # Simulate processing and result generation
    # Example:
    # try:
    #     query_result_obj = query_engine.process_query(query_string)
    #     # Assuming QueryResult has a method to get formatted string
    #     # or implement formatting here based on query_result_obj.get_headers() / .get_rows()
    #     return query_result_obj.get_formatted_table_string() # You'll need this method
    # except Exception as e:
    #     return f"Error processing query: {e}"

    # Simple placeholder response:
    headers = ["Col1", "Col2", "Col3"]
    rows = [
        ["Data1", "Data2", "Data3"],
        ["MoreData4", "MoreData5", "MoreData6"],
        ["Example7", "Example8", "Example9 " * 3], # Example long data
    ]

    # Basic string formatting for the table (improve formatting as needed)
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))

    header_line = " | ".join(f"{h:<{col_widths[i]}}" for i, h in enumerate(headers))
    separator = "-+-".join("-" * col_widths[i] for i in range(len(headers)))
    data_lines = [" | ".join(f"{str(cell):<{col_widths[i]}}" for i, cell in enumerate(row)) for row in rows]

    return f"{header_line}\n{separator}\n" + "\n".join(data_lines)

# --- Tkinter UI Class ---

class MinimalApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Multi-DB Query Tool (Minimal)")
        self.root.geometry("700x500") # Adjust size as needed

        # --- 1. Connection Status ---
        self.status_frame = tk.Frame(root, pady=5)
        self.status_frame.pack(side=tk.TOP, fill=tk.X)

        self.status_label_text = tk.Label(self.status_frame, text="MongoDB Status:", font=('Arial', 10, 'bold'))
        self.status_label_text.pack(side=tk.LEFT, padx=(10, 5))

        self.status_label_value = tk.Label(self.status_frame, text="Checking...", fg="orange", font=('Arial', 10))
        self.status_label_value.pack(side=tk.LEFT)

        # --- 2. Query Input ---
        self.input_frame = tk.Frame(root, pady=10)
        self.input_frame.pack(side=tk.TOP, fill=tk.X, padx=10)

        self.query_label = tk.Label(self.input_frame, text="Enter Query:")
        self.query_label.pack(side=tk.LEFT)

        self.query_entry = tk.Entry(self.input_frame, width=60) # Adjust width
        self.query_entry.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.X)
        self.query_entry.bind("<Return>", self.on_submit_query) # Allow Enter key submission

        self.submit_button = tk.Button(self.input_frame, text="Submit", command=self.on_submit_query)
        self.submit_button.pack(side=tk.LEFT, padx=5)

        # --- 3. Output Area ---
        self.output_frame = tk.Frame(root, pady=10)
        # fill=tk.BOTH and expand=True make the frame and text area resize with the window
        self.output_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=10)

        self.output_text = scrolledtext.ScrolledText(
            self.output_frame,
            wrap=tk.WORD, # Wrap lines by word
            state=tk.DISABLED, # Start as read-only
            font=('Courier New', 9) # Use a fixed-width font for table alignment
        )
        self.output_text.pack(fill=tk.BOTH, expand=True)

        # --- Initial Status Check ---
        self.update_connection_status()

    def update_connection_status(self):
        """Checks MongoDB connection and updates the status label."""
        is_connected = check_mongo_connection_status() # Call the placeholder/real function
        if is_connected:
            self.status_label_value.config(text="Connected", fg="green")
        else:
            self.status_label_value.config(text="Disconnected", fg="red")

    def on_submit_query(self, event=None): # event=None allows calling without keybind event
        """Handles the query submission."""
        query = self.query_entry.get()
        print(f"UI: Submitting query: {query}")

        # --- Interaction with Backend ---
        try:
            result_string = process_the_query(query) # Call the placeholder/real function
        except Exception as e:
            # Catch unexpected errors during processing
            print(f"UI Error: Exception during query processing - {e}")
            messagebox.showerror("Processing Error", f"An unexpected error occurred:\n{e}")
            result_string = f"INTERNAL ERROR: {e}"

        # Enable text area, clear it, insert result, disable it again
        self.output_text.config(state=tk.NORMAL)
        self.output_text.delete('1.0', tk.END) # Clear previous output
        self.output_text.insert(tk.END, result_string)
        self.output_text.config(state=tk.DISABLED)


# --- Main Execution ---
if __name__ == "__main__":
    root = tk.Tk()
    app = MinimalApp(root)
    root.mainloop()