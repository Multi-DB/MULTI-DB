import tkinter as tk
from tkinter import filedialog, scrolledtext, messagebox, ttk # Import ttk
import json
import threading
import sys
from io import StringIO
import collections # Import collections for flattening

# Assuming graph_builder and query_engine are in the same directory or accessible via PYTHONPATH
try:
    from graph_builder import GraphBuilder
    from query_engine import QueryEngine
except ImportError as e:
    messagebox.showerror("Import Error", f"Failed to import required modules: {e}\nPlease ensure graph_builder.py and query_engine.py are in the correct path.")
    sys.exit(1)

class RedirectText:
    """Helper class to redirect stdout/stderr to a Tkinter Text widget."""
    def __init__(self, text_widget):
        self.text_space = text_widget
        self.buffer = StringIO()

    def write(self, string):
        # Write to internal buffer first
        self.buffer.write(string)
        # Schedule update in the main Tkinter thread
        self.text_space.after_idle(self._update_widget)

    def _update_widget(self):
        # Get content from buffer and clear it
        content = self.buffer.getvalue()
        self.buffer.seek(0)
        self.buffer.truncate(0)

        if content:
            # Ensure the widget is updated in the main thread
            current_state = self.text_space.cget('state')
            self.text_space.config(state=tk.NORMAL)
            self.text_space.insert(tk.END, content)
            self.text_space.see(tk.END) # Scroll to the end
            self.text_space.config(state=current_state) # Restore original state

    def flush(self):
        # Tkinter Text widget doesn't need explicit flushing like a file
        pass

class MultiDbUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Multi-DB Graph Query UI")
        self.geometry("900x700") # Increased size for table

        self.schema_file_path = tk.StringVar()
        self.query_engine = QueryEngine() # Instantiate QueryEngine

        self._create_widgets()
        self._redirect_output() # Redirect after log_text is created

    def _create_widgets(self):
        # --- Schema File Section ---
        schema_frame = tk.Frame(self)
        schema_frame.pack(pady=5, padx=10, fill=tk.X)

        tk.Label(schema_frame, text="Schema File:").pack(side=tk.LEFT)
        schema_entry = tk.Entry(schema_frame, textvariable=self.schema_file_path, width=60)
        schema_entry.pack(side=tk.LEFT, expand=True, fill=tk.X, padx=5)
        browse_button = tk.Button(schema_frame, text="Browse...", command=self._browse_schema_file)
        browse_button.pack(side=tk.LEFT)

        # --- Action Buttons ---
        action_frame = tk.Frame(self)
        action_frame.pack(pady=5, padx=10, fill=tk.X)

        self.load_button = tk.Button(action_frame, text="Load Data & Build Graph", command=self._run_load_build_threaded)
        self.load_button.pack(side=tk.LEFT, padx=5)

        self.query_button = tk.Button(action_frame, text="Execute Query", command=self._run_query_threaded)
        self.query_button.pack(side=tk.LEFT, padx=5)

        # --- Query Input Section ---
        tk.Label(self, text="Enter Query (JSON):").pack(pady=(10, 0), padx=10, anchor=tk.W)
        self.query_input_text = scrolledtext.ScrolledText(self, height=8, wrap=tk.WORD) # Reduced height
        self.query_input_text.pack(pady=5, padx=10, fill=tk.BOTH) # Removed expand=True

        # --- Log/Status Output Section ---
        tk.Label(self, text="Logs & Status:").pack(pady=(10, 0), padx=10, anchor=tk.W)
        self.log_text = scrolledtext.ScrolledText(self, height=8, wrap=tk.WORD, state='normal') # Start normal for redirection
        self.log_text.pack(pady=5, padx=10, fill=tk.BOTH) # Removed expand=True

        # --- Results Table Section ---
        tk.Label(self, text="Query Results:").pack(pady=(10, 0), padx=10, anchor=tk.W)
        tree_frame = ttk.Frame(self)
        tree_frame.pack(pady=5, padx=10, fill=tk.BOTH, expand=True)

        # Create Treeview
        self.results_tree = ttk.Treeview(tree_frame, columns=[], show='headings')

        # Scrollbars
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.results_tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.results_tree.xview)
        self.results_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        # Grid layout for Treeview and scrollbars
        self.results_tree.grid(row=0, column=0, sticky='nsew')
        vsb.grid(row=0, column=1, sticky='ns')
        hsb.grid(row=1, column=0, sticky='ew')

        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)

    def _redirect_output(self):
        """Redirect stdout and stderr to the log text widget."""
        redirector = RedirectText(self.log_text)
        sys.stdout = redirector
        sys.stderr = redirector
        print("--- UI Initialized ---") # Initial message

    def _browse_schema_file(self):
        filepath = filedialog.askopenfilename(
            title="Select Schema File",
            filetypes=(("JSON files", "*.json"), ("All files", "*.*"))
        )
        if filepath:
            self.schema_file_path.set(filepath)
            print(f"Schema file selected: {filepath}")

    def _enable_buttons(self, enable=True):
        """Enable or disable action buttons."""
        state = tk.NORMAL if enable else tk.DISABLED
        self.load_button.config(state=state)
        self.query_button.config(state=state)

    def _run_load_build_threaded(self):
        """Run the load/build process in a separate thread."""
        schema_path = self.schema_file_path.get()
        if not schema_path:
            messagebox.showerror("Error", "Please select a schema file first.")
            return

        self._enable_buttons(False)
        print("Starting data loading and graph building...")
        thread = threading.Thread(target=self._load_and_build_task, args=(schema_path,), daemon=True)
        thread.start()

    def _load_and_build_task(self, schema_path):
        """Task for loading data and building the graph."""
        try:
            graph_builder = GraphBuilder(schema_path)
            graph_builder.load_data_from_schema()
            graph_builder.build_graph()
            print("\nData loading and graph building completed successfully.")
        except Exception as e:
            print(f"\nError during load/build: {e}")
        finally:
            self.after(0, self._enable_buttons, True)

    def _run_query_threaded(self):
        """Run the query execution in a separate thread."""
        query_string = self.query_input_text.get("1.0", tk.END).strip()
        if not query_string:
            messagebox.showerror("Error", "Please enter a query.")
            return

        try:
            query_json = json.loads(query_string)
        except json.JSONDecodeError as e:
            messagebox.showerror("Invalid JSON", f"The entered query is not valid JSON:\n{e}")
            return

        self._enable_buttons(False)
        print("\nExecuting query...")
        thread = threading.Thread(target=self._execute_query_task, args=(query_json,), daemon=True)
        thread.start()

    def _execute_query_task(self, query_json):
        """Task for executing the query and displaying results in Treeview."""
        try:
            self.after(0, self._clear_treeview)
            results = self.query_engine.execute_query(query_json)
            query_type = query_json.get("type", "within")

            print("\nQuery Results:")
            if results:
                self.after(0, self._update_treeview, results, query_type)
            else:
                print("No results found.")
        except Exception as e:
            print(f"\nError executing query: {e}")
        finally:
            self.after(0, self._enable_buttons, True)

    def _clear_treeview(self):
        """Clears all items and columns from the results Treeview."""
        for item in self.results_tree.get_children():
            self.results_tree.delete(item)
        self.results_tree["columns"] = []

    def _flatten_dict(self, d, parent_key='', sep='.'):
        """Recursively flattens a nested dictionary."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def _update_treeview(self, results, query_type):
        """Updates the Treeview with query results (runs in main thread)."""
        self._clear_treeview()

        if not results:
            return

        columns = []
        flat_results_list = [] # Store flattened results for row insertion

        if query_type == "within":
            if isinstance(results[0], dict):
                columns = list(results[0].keys())
                flat_results_list = results # Already flat
            else: # Handle list of non-dict results
                 columns = ["Value"]
                 flat_results_list = [{"Value": r} for r in results]

        elif query_type == "across":
            # Flatten each result and collect all unique keys for columns
            all_keys = set()
            for result in results:
                if isinstance(result, dict):
                    # Flatten the {'Entity': {'field': val, 'nested.field': val2}} structure
                    flat_result = self._flatten_dict(result)
                    flat_results_list.append(flat_result)
                    all_keys.update(flat_result.keys())
            columns = sorted(list(all_keys))

        if not columns:
            print("Could not determine columns for results.")
            return

        # Configure Treeview columns
        self.results_tree["columns"] = columns
        for col in columns:
            self.results_tree.heading(col, text=col)
            self.results_tree.column(col, width=120, anchor=tk.W) # Increased default width

        # Populate Treeview rows using the flattened results
        for flat_result in flat_results_list:
            # Get values for the current row based on the determined column order
            row_values = [flat_result.get(col, "") for col in columns]
            # Convert all values to string for display
            str_values = [str(v) for v in row_values]
            self.results_tree.insert('', tk.END, values=str_values)

if __name__ == "__main__":
    app = MultiDbUI()
    app.mainloop()
