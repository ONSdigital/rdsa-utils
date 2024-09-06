"""All functions for app."""

import json
import tkinter as tk
from tkinter import filedialog, messagebox


def set_min_values(app):
    """Set all minimum values from the default min values entry."""
    # Retrieve and clean minimum values from the entry widget
    min_vals = [val.strip() for val in app.default_min_vals_entry.get().split(",")]

    # Apply a single value to all columns if only one value is provided
    if len(min_vals) == 1:
        min_val = min_vals[0]
        for column_widgets in app.column_entries:
            min_val_entry = column_widgets["min_val_entry"]
            min_val_entry.delete(0, tk.END)
            min_val_entry.insert(0, min_val)
    else:
        # Apply each value from the list to the respective column
        for column_widgets in app.column_entries:
            min_val_entry = column_widgets["min_val_entry"]
            if min_vals:
                min_val_entry.delete(0, tk.END)
                min_val_entry.insert(0, min_vals.pop(0))
            else:
                # Default value if not enough values are provided
                min_val_entry.delete(0, tk.END)
                min_val_entry.insert(0, "0")


def set_max_values(app):
    """Set all maximum values from the default max values entry."""
    # Retrieve and clean maximum values from the entry widget
    max_vals = [val.strip() for val in app.default_max_vals_entry.get().split(",")]

    # Apply a single value to all columns if only one value is provided
    if len(max_vals) == 1:
        max_val = max_vals[0]
        for column_widgets in app.column_entries:
            max_val_entry = column_widgets["max_val_entry"]
            max_val_entry.delete(0, tk.END)
            max_val_entry.insert(0, max_val)
    else:
        # Apply each value from the list to the respective column
        for column_widgets in app.column_entries:
            max_val_entry = column_widgets["max_val_entry"]
            if max_vals:
                max_val_entry.delete(0, tk.END)
                max_val_entry.insert(0, max_vals.pop(0))
            else:
                # Default value if not enough values are provided
                max_val_entry.delete(0, tk.END)
                max_val_entry.insert(0, "0")


def set_default_values(app):
    """Set default values for min and max value entries for all columns."""
    # Default values for min and max entries
    min_default = "0"
    max_default = "1000"

    for column_widgets in app.column_entries:
        min_val_entry = column_widgets["min_val_entry"]
        max_val_entry = column_widgets["max_val_entry"]

        # Set default values for min and max entries
        min_val_entry.delete(0, tk.END)
        min_val_entry.insert(0, min_default)
        max_val_entry.delete(0, tk.END)
        max_val_entry.insert(0, max_default)


def set_column_names(app):
    """Set all column names from the default column names entry."""
    # Retrieve and clean column names from the entry widget
    col_names = [name.strip() for name in app.default_col_names_entry.get().split(",")]

    for column_widgets in app.column_entries:
        col_name_entry = column_widgets["col_name_entry"]

        if col_names:
            # Set the column name from the list
            col_name_entry.delete(0, tk.END)
            col_name_entry.insert(0, col_names.pop(0))
        else:
            # If no more names are available, clear the entry
            col_name_entry.delete(0, tk.END)


def save_setup(app):
    """Save the current setup to a file."""
    setup_file = filedialog.asksaveasfilename(
        defaultextension=".json",
        filetypes=[("JSON files", "*.json")],
        title="Save Setup As",
    )
    if setup_file:

        # Prepare setup data
        setup_data = {
            "num_columns": get_widget_value(app.num_columns_entry),
            "num_rows": get_widget_value(app.num_rows_entry),
            "column_entries": [
                {
                    key: get_widget_value(widget)
                    for key, widget in entry.items()
                    if not key.endswith("_menu")  # Exclude widgets ending with '_menu'
                }
                for entry in app.column_entries
            ],
        }

        # Write to JSON file
        with open(setup_file, "w") as f:
            json.dump(setup_data, f, indent=4)

        messagebox.showinfo("Save Setup", f"Setup saved as {setup_file}")


def get_widget_value(widget):
    """Get the value from a widget."""
    if widget:
        # Handle both variables and widgets
        if isinstance(widget, tk.Variable):
            return widget.get()
        elif hasattr(widget, "get"):
            return widget.get()
    return ""


def load_setup(app):
    """Load a setup from a file."""
    setup_file = filedialog.askopenfilename(
        defaultextension=".json",
        filetypes=[("JSON files", "*.json")],
    )
    if setup_file:
        with open(setup_file, "r") as f:
            setup_data = json.load(f)

        # Load values into the GUI
        set_widget_value(app.num_columns_entry, setup_data["num_columns"])
        set_widget_value(app.num_rows_entry, setup_data["num_rows"])

        # Set columns and update fields
        app.set_columns()  # This will create the column entries

        for i, entry_data in enumerate(setup_data.get("column_entries", [])):
            if i < len(app.column_entries):
                entry_widgets = app.column_entries[i]
                update_entry_widgets(entry_widgets, entry_data)


def set_widget_value(widget, value):
    """Set the value of a widget."""
    if isinstance(widget, tk.Variable):
        widget.set(value)
    elif hasattr(widget, "delete") and hasattr(widget, "insert"):
        widget.delete(0, tk.END)
        widget.insert(0, value)


def update_entry_widgets(entry_widgets, entry_data):
    """Update the widgets for a specific column entry."""
    for key, value in entry_data.items():
        if key in entry_widgets:
            set_widget_value(entry_widgets[key], value)
