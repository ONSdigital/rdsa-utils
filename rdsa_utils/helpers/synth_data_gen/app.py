"""App for datagen."""

import tkinter as tk
from tkinter import messagebox

import ttkbootstrap as tb
from ttkbootstrap import Style

from rdsa_utils.helpers.synth_data_gen.app_functions import (
    load_setup,
    save_setup,
    set_column_names,
    set_default_values,
    set_max_values,
    set_min_values,
)
from rdsa_utils.helpers.synth_data_gen.gen_csv import create_csv


class DataGeneratorApp(tb.Frame):
    """A Tkinter application for generating synth data with dynamic columns and rows."""

    def __init__(self, parent: tk.Tk) -> None:
        """
        Initialize the DataGeneratorApp with its widgets and configuration.

        Args:
            parent (tk.Tk): The parent Tkinter window.
        """
        super().__init__(parent)
        self.create_widgets()
        self.pack(fill="both", expand=True, padx=10, pady=10)

    def create_widgets(self) -> None:
        """Create and arrange widgets within the DataGeneratorApp."""
        # Main Frame
        self.main_frame = tb.Frame(self)
        self.main_frame.pack(fill="both", expand=True, padx=10, pady=10)

        # Number of columns input
        tb.Label(self.main_frame, text="Number of Columns:").grid(
            row=0,
            column=0,
            padx=5,
            pady=5,
            sticky="w",
        )
        self.num_columns_entry = tb.Entry(self.main_frame, width=5)
        self.num_columns_entry.grid(row=0, column=1, padx=5, pady=5, sticky="ew")
        self.set_columns_button = tb.Button(
            self.main_frame,
            text="Set Columns",
            command=self.set_columns,
        )
        self.set_columns_button.grid(row=0, column=2, padx=5, pady=5, sticky="ew")

        # Number of rows input
        tb.Label(self.main_frame, text="Number of Rows:").grid(
            row=1,
            column=0,
            padx=5,
            pady=5,
            sticky="w",
        )
        self.num_rows_entry = tb.Entry(self.main_frame, width=5)
        self.num_rows_entry.grid(row=1, column=1, padx=5, pady=5, sticky="ew")

        # Placeholder for dynamic column fields
        self.column_frame = tb.Frame(self.main_frame)
        self.column_frame.grid(
            row=4,
            column=0,
            columnspan=3,
            padx=5,
            pady=5,
            sticky="nsew",
        )

        # Default values section (Initially hidden)
        self.default_values_frame = tb.Frame(self.main_frame)
        self.default_values_frame.grid(
            row=5,
            column=0,
            columnspan=3,
            padx=5,
            pady=5,
            sticky="w",
        )
        self.default_values_frame.grid_remove()  # Hide initially

        self.save_load_frame = tb.Frame(self.main_frame)
        self.save_load_frame.grid(
            row=5,
            column=1,
            columnspan=3,
            padx=5,
            pady=5,
            sticky="e",
        )

        tb.Label(
            self.default_values_frame,
            text="Set All Column Names (comma delimited list):",
        ).grid(row=0, column=0, padx=5, pady=5, sticky="w")
        self.default_col_names_entry = tb.Entry(self.default_values_frame, width=30)
        self.default_col_names_entry.grid(row=0, column=1, padx=5, pady=5, sticky="ew")
        self.set_col_names_button = tb.Button(
            self.default_values_frame,
            text="Set",
            command=lambda: set_column_names(self),
        )
        self.set_col_names_button.grid(row=0, column=2, padx=5, pady=5, sticky="ew")

        tb.Label(
            self.default_values_frame,
            text="Default Min Values (comma delimited list):",
        ).grid(row=1, column=0, padx=5, pady=5, sticky="w")
        self.default_min_vals_entry = tb.Entry(self.default_values_frame, width=30)
        self.default_min_vals_entry.grid(row=1, column=1, padx=5, pady=5, sticky="ew")
        self.set_min_vals_button = tb.Button(
            self.default_values_frame,
            text="Set",
            command=lambda: set_min_values(self),
        )
        self.set_min_vals_button.grid(row=1, column=2, padx=5, pady=5, sticky="ew")

        tb.Label(
            self.default_values_frame,
            text="Default Max Values (comma delimited list):",
        ).grid(row=2, column=0, padx=5, pady=5, sticky="w")
        self.default_max_vals_entry = tb.Entry(self.default_values_frame, width=30)
        self.default_max_vals_entry.grid(row=2, column=1, padx=5, pady=5, sticky="ew")
        self.set_max_vals_button = tb.Button(
            self.default_values_frame,
            text="Set",
            command=lambda: set_max_values(self),
        )
        self.set_max_vals_button.grid(row=2, column=2, padx=5, pady=5, sticky="ew")

        # Create CSV button
        self.create_csv_button = tb.Button(
            self.main_frame,
            text="Create CSV",
            command=lambda: create_csv(self),
        )
        self.create_csv_button.grid(row=6, column=0, columnspan=3, pady=10, sticky="ew")

        # Save and Load buttons
        self.save_button = tb.Button(
            self.save_load_frame,
            text="Save Setup",
            command=lambda: save_setup(self),
            style="success",
        )
        self.save_button.grid(row=1, column=1, padx=5, pady=5, sticky="e")

        self.load_button = tb.Button(
            self.save_load_frame,
            text="Load Setup",
            command=lambda: load_setup(self),
            style="success",
        )
        self.load_button.grid(row=2, column=1, padx=5, pady=5, sticky="e")

        # Configure grid weights to expand with window resizing
        self.main_frame.grid_rowconfigure(4, weight=1)
        self.main_frame.grid_columnconfigure(1, weight=1)
        self.main_frame.grid_columnconfigure(2, weight=1)

        self.column_frame.grid_rowconfigure(6, weight=1)
        self.column_frame.grid_columnconfigure(0, weight=1)

        self.save_load_frame.grid_columnconfigure(
            1,
            weight=1,
        )  # Ensure column 1 expands

        # Bind resize event
        self.master.bind("<Configure>", self.on_resize)

    def set_columns(self):
        """Create all widgets for parameter selection."""
        try:
            # Get the number of columns from the entry widget
            num_columns = int(self.num_columns_entry.get())
            if num_columns <= 0:
                error_message = "Number of columns must be positive"
                raise ValueError(error_message)

            # Clear existing widgets in the column_frame
            for widget in self.column_frame.winfo_children():
                widget.destroy()

            # Create canvas and scrollbar for scrolling
            self.canvas = tb.Canvas(self.column_frame)
            self.hscrollbar = tb.Scrollbar(
                self.column_frame,
                orient="horizontal",
                command=self.canvas.xview,
                style="warning",
            )
            self.hscrollbar.pack(side="bottom", fill="x")

            self.yscrollbar = tb.Scrollbar(
                self.column_frame,
                orient="vertical",
                command=self.canvas.yview,
                style="warning",
            )
            self.yscrollbar.pack(side="left", fill="y")

            self.canvas.configure(
                xscrollcommand=self.hscrollbar.set,
                yscrollcommand=self.yscrollbar.set,
            )
            self.canvas.pack(side="left", fill="both", expand=True)

            # Create a frame inside the canvas to hold column entries
            self.column_inner_frame = tb.Frame(self.canvas)
            self.canvas.create_window(
                (0, 0),
                window=self.column_inner_frame,
                anchor="nw",
            )

            # Update canvas scroll region when the inner frame is resized
            self.column_inner_frame.bind(
                "<Configure>",
                lambda e: self.canvas.config(scrollregion=self.canvas.bbox("all")),
            )

            # Dictionary for dropdown options
            dropdowns = {
                "data_types": [
                    "Numbers",
                    "Dates and Times",
                    "Booleans",
                    "Currency",
                    "Percentages",
                    "String",
                    "Geolocation",
                    "Unique ID",
                    "Phone Number",
                    "Postcode",
                ],
                "int_float": ["int", "float"],
                "distros": [
                    "Uniform",
                    "Normal",
                    "Exponential",
                    "Log Normal",
                    "Random Walk",
                ],
                "date_formats": [
                    "YYYY-MM-DD",
                    "MM-DD-YYYY",
                    "DD-MM-YYYY",
                    "YYYY-MM-DD HH:MM:SS",
                ],
                "intervals": ["Daily", "Weekly", "Monthly", "Quarterly", "Yearly"],
                "boolean_reps": ["True, False", "Yes, No", "T, F", "1, 0", "-1, +1"],
                "currency_symbols": ["$", "€", "£", "¥"],
                "thousands_sep": [",", ".", "(no sep)"],
                "percentage_symbol": ["75", "0.75", "75%"],
                "missings_vals": ["N/A", "Null", "Nan", "(blank_cells)"],
                "string_type": ["Email", "URL", "Names", "Short text", "Long text"],
                "geo_type": ["Lat,Long", "Longitude", "Latitude"],
                "unique_id_format": [
                    "uuid",
                    "numeric",
                    "alphanumeric",
                    "structured_id",
                    "hex_short_id",
                    "custom_alphanumeric_uuid",
                    "timestamp_numeric",
                    "invoice_custom_id",
                ],
                "country": [
                    "UK",
                    "US",
                    "Canada",
                    "Spain",
                    "China",
                    "Germany",
                    "France",
                ],
                "names_type": ["Full Name", "First", "Last"],
            }

            default_values = {
                "data_type": dropdowns["data_types"][0],
                "type": dropdowns["int_float"][0],
                "dist": dropdowns["distros"][0],
                "date_format": dropdowns["date_formats"][0],
                "interval": dropdowns["intervals"][0],
                "boolean_rep": dropdowns["boolean_reps"][0],
                "currency_symbol": dropdowns["currency_symbols"][0],
                "thousands_sep": dropdowns["thousands_sep"][0],
                "percentage_symbol": dropdowns["percentage_symbol"][0],
                "missings_val": dropdowns["missings_vals"][0],
                "string_type": dropdowns["string_type"][0],
                "geo_type": dropdowns["geo_type"][0],
                "unique_id_format": dropdowns["unique_id_format"][0],
                "country": dropdowns["country"][0],
                "names_type": dropdowns["names_type"][0],
            }

            self.column_entries = []

            for i in range(num_columns):
                # Label for the column
                label = tb.Label(self.column_inner_frame, text=f"Column {i + 1}")

                # Dropdown for data type selection
                data_type_var = tk.StringVar(value=default_values["data_type"])
                data_type_menu = tb.OptionMenu(
                    self.column_inner_frame,
                    data_type_var,
                    default_values["data_type"],
                    *dropdowns["data_types"],
                )

                # Entry widget for column name
                col_name_entry = tb.Entry(self.column_inner_frame, width=15)

                # Widgets for min and max values
                min_val_entry = tb.Entry(self.column_inner_frame, width=10)
                max_val_entry = tb.Entry(self.column_inner_frame, width=10)

                # Dropdown for selecting data type (int or float)
                type_var = tk.StringVar(value=default_values["type"])
                type_menu = tb.OptionMenu(
                    self.column_inner_frame,
                    type_var,
                    default_values["type"],
                    *dropdowns["int_float"],
                )

                # Dropdown for selecting distribution
                dist_var = tk.StringVar(value=default_values["dist"])
                dist_menu = tb.OptionMenu(
                    self.column_inner_frame,
                    dist_var,
                    default_values["dist"],
                    *dropdowns["distros"],
                )

                # Widgets for date format
                date_format_var = tk.StringVar(value=default_values["date_format"])
                date_format_menu = tb.OptionMenu(
                    self.column_inner_frame,
                    date_format_var,
                    default_values["date_format"],
                    *dropdowns["date_formats"],
                )
                start_date_entry = tb.Entry(self.column_inner_frame, width=15)
                start_date_entry.insert(0, default_values["date_format"])

                interval_var = tk.StringVar(value=default_values["interval"])
                interval_menu = tb.OptionMenu(
                    self.column_inner_frame,
                    interval_var,
                    default_values["interval"],
                    *dropdowns["intervals"],
                )

                # Widgets for boolean specific options
                boolean_rep_var = tk.StringVar(value=default_values["boolean_rep"])
                boolean_rep_menu = tb.OptionMenu(
                    self.column_inner_frame,
                    boolean_rep_var,
                    default_values["boolean_rep"],
                    *dropdowns["boolean_reps"],
                )

                # Widget for boolean positive/negatives ratio
                bool_slider = tb.Scale(
                    self.column_inner_frame,
                    from_=0,
                    to=100,
                    orient=tb.HORIZONTAL,
                )

                # Widgets for currency specific options
                currency_symbol_var = tk.StringVar(
                    value=default_values["currency_symbol"],
                )
                currency_symbol_menu = tb.OptionMenu(
                    self.column_inner_frame,
                    currency_symbol_var,
                    default_values["currency_symbol"],
                    *dropdowns["currency_symbols"],
                )

                decimal_places_entry = tb.Entry(self.column_inner_frame, width=5)
                decimal_places_entry.insert(0, "2")

                thousands_sep_var = tk.StringVar(value=default_values["thousands_sep"])
                thousands_sep_menu = tb.OptionMenu(
                    self.column_inner_frame,
                    thousands_sep_var,
                    default_values["thousands_sep"],
                    *dropdowns["thousands_sep"],
                )

                # Widgets for percentage specific options
                percentage_symbol_var = tk.StringVar(
                    value=default_values["percentage_symbol"],
                )
                percentage_symbol_menu = tb.OptionMenu(
                    self.column_inner_frame,
                    percentage_symbol_var,
                    default_values["percentage_symbol"],
                    *dropdowns["percentage_symbol"],
                )

                # Widgets for missing values
                missing_values_entry = tb.Entry(self.column_inner_frame, width=5)
                missing_values_entry.insert(0, "0")

                missing_values_var = tk.StringVar(value=default_values["missings_val"])
                missing_values_menu = tb.OptionMenu(
                    self.column_inner_frame,
                    missing_values_var,
                    default_values["missings_val"],
                    *dropdowns["missings_vals"],
                )

                string_type_var = tk.StringVar(value=default_values["string_type"])
                string_type_menu = tb.OptionMenu(
                    self.column_inner_frame,
                    string_type_var,
                    default_values["string_type"],
                    *dropdowns["string_type"],
                )

                string_comma_delim_entry = tb.Entry(self.column_inner_frame, width=10)

                geo_type_var = tk.StringVar(value=default_values["geo_type"])
                geo_type_menu = tb.OptionMenu(
                    self.column_inner_frame,
                    geo_type_var,
                    default_values["geo_type"],
                    *dropdowns["geo_type"],
                )

                unique_id_format_var = tk.StringVar(
                    value=default_values["unique_id_format"],
                )
                unique_id_format_menu = tb.OptionMenu(
                    self.column_inner_frame,
                    unique_id_format_var,
                    default_values["unique_id_format"],
                    *dropdowns["unique_id_format"],
                )

                country_var = tk.StringVar(value=default_values["country"])
                country_menu = tb.OptionMenu(
                    self.column_inner_frame,
                    country_var,
                    default_values["country"],
                    *dropdowns["country"],
                )

                names_type_var = tk.StringVar(value=default_values["names_type"])
                names_type_menu = tb.OptionMenu(
                    self.column_inner_frame,
                    names_type_var,
                    default_values["names_type"],
                    *dropdowns["names_type"],
                )

                label_widget_mapping = {
                    "": label,
                    "Data Type": data_type_menu,
                    "Column Name": col_name_entry,
                    "Missing Values %:": missing_values_entry,
                    "Missing Val Types:": missing_values_menu,
                    "Min Value:": min_val_entry,
                    "Max Value:": max_val_entry,
                    "Type:": type_menu,
                    "Decimal Places:": decimal_places_entry,
                    "Distribution:": dist_menu,
                    "Date Format:": date_format_menu,
                    "Start Date": start_date_entry,
                    "Interval:": interval_menu,
                    "Bool Format:": boolean_rep_menu,
                    "Frequency:": bool_slider,
                    "Currency Symbol:": currency_symbol_menu,
                    "Thousands Separator:": thousands_sep_menu,
                    "Percentage Format:": percentage_symbol_menu,
                    "Country:": country_menu,
                    "String Comma Delim:": string_comma_delim_entry,
                    "String Type:": string_type_menu,
                    "Geo Type:": geo_type_menu,
                    "Unique ID Format:": unique_id_format_menu,
                    "Names Type:": names_type_menu,
                }

                padx = pady = 5

                for j, (label_text, widget) in enumerate(
                    label_widget_mapping.items(),
                    start=0,
                ):
                    # Create and grid the label
                    tb.Label(self.column_inner_frame, text=label_text).grid(
                        row=j,
                        column=0,
                        padx=padx,
                        pady=pady,
                    )

                    # Grid the corresponding widget
                    widget.grid(row=j, column=i + 1, padx=padx, pady=pady)

                data_type_var.trace_add(
                    "write",
                    lambda name, index, mode, var=data_type_var, idx=i: self.update_fields(  # noqa: E501
                        var,
                        idx,
                    ),
                )

                self.column_entries.append(
                    {
                        "label": label,
                        "data_type_var": data_type_var,
                        "data_type_menu": data_type_menu,
                        "col_name_entry": col_name_entry,
                        "min_val_entry": min_val_entry,
                        "max_val_entry": max_val_entry,
                        "type_var": type_var,
                        "type_menu": type_menu,
                        "dist_var": dist_var,
                        "dist_menu": dist_menu,
                        "date_format_var": date_format_var,
                        "date_format_menu": date_format_menu,
                        "start_date_entry": start_date_entry,
                        "interval_var": interval_var,
                        "interval_menu": interval_menu,
                        "boolean_rep_var": boolean_rep_var,
                        "boolean_rep_menu": boolean_rep_menu,
                        "bool_slider": bool_slider,
                        "currency_symbol_var": currency_symbol_var,
                        "currency_symbol_menu": currency_symbol_menu,
                        "decimal_places_entry": decimal_places_entry,
                        "thousands_sep_var": thousands_sep_var,
                        "thousands_sep_menu": thousands_sep_menu,
                        "percentage_symbol_var": percentage_symbol_var,
                        "percentage_symbol_menu": percentage_symbol_menu,
                        "missing_values_entry": missing_values_entry,
                        "missing_values_var": missing_values_var,
                        "missing_values_menu": missing_values_menu,
                        "country_var": country_var,
                        "country_menu": country_menu,
                        "string_type_var": string_type_var,
                        "string_type_menu": string_type_menu,
                        "string_comma_delim_entry": string_comma_delim_entry,
                        "geo_type_var": geo_type_var,
                        "geo_type_menu": geo_type_menu,
                        "unique_id_format_var": unique_id_format_var,
                        "unique_id_format_menu": unique_id_format_menu,
                        "names_type_var": names_type_var,
                        "names_type_menu": names_type_menu,
                    },
                )

            # Update scrollregion to fit the size of the inner frame
            self.canvas.update_idletasks()
            self.canvas.config(scrollregion=self.canvas.bbox("all"))

            # Set default values for min and max values
            set_default_values(self)

            for idx in range(num_columns):
                self.update_fields(data_type_var, idx)

            # Display the default values frame
            self.default_values_frame.grid()

        except ValueError as e:
            # Show an error message if the input value is invalid
            messagebox.showerror("Invalid Input", str(e))

    def update_fields(self, data_type_var, col_idx):
        """Show or hide relevant parameters based on data type."""
        data_type = data_type_var.get()
        column_widgets = self.column_entries[col_idx]

        # Define mapping between data types and the widgets to show, + special cases
        widget_mapping = {
            "Numbers": [
                "min_val_entry",
                "max_val_entry",
                "type_menu",
                "dist_menu",
            ],
            "Currency": [
                "min_val_entry",
                "max_val_entry",
                "type_menu",
                "dist_menu",
                "currency_symbol_menu",
                "thousands_sep_menu",
            ],
            "Percentages": [
                "min_val_entry",
                "max_val_entry",
                "type_menu",
                "dist_menu",
                "percentage_symbol_menu",
            ],
            "Dates and Times": [
                "date_format_menu",
                "start_date_entry",
                "interval_menu",
            ],
            "Booleans": ["boolean_rep_menu", "bool_slider"],
            "String": [
                "string_type_menu",
                "string_comma_delim_entry",
                "names_type_menu",
            ],
            "Geolocation": ["geo_type_menu"],
            "Unique ID": ["unique_id_format_menu"],
        }
        always_visable = [
            "label",
            "data_type_menu",
            "col_name_entry",
            "missing_values_entry",
            "missing_values_menu",
        ]
        # Hide all widgets initially
        for key, widget in column_widgets.items():
            if not key.endswith("_var") and key not in always_visable:
                widget.grid_remove()

        # Show the relevant widgets based on the selected data type
        if data_type in widget_mapping:
            for widget_name in widget_mapping[data_type]:
                column_widgets[widget_name].grid()

        # Special handling for country options
        if data_type in ["String", "Phone Number", "Postcode"]:
            column_widgets["country_menu"].grid()

        # Special handling for decimal places
        if data_type in ["Numbers", "Currency", "Percentages"]:
            column_widgets["decimal_places_entry"].grid()

    def on_resize(self, event):
        """Resize the window."""
        # Optional: You can add more resize logic here if needed
        pass


def main():
    """Run app."""
    root = tk.Tk()
    style = Style(theme="cyborg")  # noqa: F841
    app = DataGeneratorApp(root)  # noqa: F841

    # Center and maximize the window
    root.update()
    root.minsize(root.winfo_width(), root.winfo_height())
    x_cordinate = int((root.winfo_screenwidth() / 2) - (root.winfo_width() / 2))
    y_cordinate = int((root.winfo_screenheight() / 2) - (root.winfo_height() / 2))
    root.geometry("+{}+{}".format(x_cordinate, y_cordinate - 20))

    root.mainloop()


if __name__ == "__main__":
    main()
