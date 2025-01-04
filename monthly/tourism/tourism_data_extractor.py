from re import S
import pandas as pd
import json
from pathlib import Path
import os
import numpy as np
from pandas.io.sql import _process_parse_dates_argument
import shutil
from datetime import datetime
import sys

# Add root directory to Python path
root_dir = Path(__file__).resolve().parents[2]
root_dir_str = str(root_dir)
if root_dir_str not in sys.path:
    sys.path.append(root_dir_str)

from database.mongodb import get_database

# Get MongoDB database connection
db = get_database()
monthly_collection = db["tourism_monthly"]
country_collection = db["tourism_country"]


def move_to_processed(file_path):
    # Create processed directory
    processed_dir = Path("data/tourism/processed")
    processed_dir.mkdir(parents=True, exist_ok=True)

    # Move file to processed directory
    file_path = Path(file_path)
    new_path = processed_dir / file_path.name

    # If file with same name exists, add timestamp
    if new_path.exists():
        timestamp = datetime.now().strftime("%H-%M-%S")
        new_path = processed_dir / f"{file_path.stem}_{timestamp}{file_path.suffix}"

    shutil.move(str(file_path), str(new_path))
    print(f"Moved {file_path.name} to {new_path}")


def clean_dataframe(df, available_months):
    # Skip the header rows and reset index
    df = df.iloc[2:].reset_index(drop=True)

    numeric_columns = available_months.copy()
    available_months.insert(0, "country")
    available_months.insert(0, "no")
    available_months.append("total")

    print("\nAvailable months:", available_months)

    # Rename columns
    df.columns = available_months

    numeric_columns.append("total")
    print("\nNumeric columns:", numeric_columns)
    # Convert numeric columns to float, handling any non-numeric values
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(
            obj,
            (
                np.int_,
                np.intc,
                np.intp,
                np.int8,
                np.int16,
                np.int32,
                np.int64,
                np.uint8,
                np.uint16,
                np.uint32,
                np.uint64,
            ),
        ):
            return int(obj)
        elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
            return float(obj)
        return super().default(obj)


def process_tourism_data(excel_file, year, starting_month_index, ending_month_index):
    # Read the Excel file
    df = pd.read_excel(excel_file)
    print("\nOriginal DataFrame columns:", df.columns.tolist())
    print("Original DataFrame shape:", df.shape)

    # List of all possible months
    all_months = [
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december"
    ]

    available_months = df.columns[2 : 2 + ending_month_index + 1].tolist()
    print("\nAvailable months:", available_months)

    # Process monthly data
    monthly_data = []
    for month in available_months:
        month_data = {
            "year": year,
            "month": all_months[available_months.index(month)],
            "countries": []
        }
        # Calculate total for current month
        calculated_total = 0

        # Process each country for the current month
        for _, row in df.iloc[:-1].iterrows():  # Skip the last row (total row)
            country_col = next((col for col in df.columns if col.upper() == "COUNTRY"), None)
            if country_col:
                country = str(row[country_col]).strip()
            if country and pd.notna(row["No"]):  # Valid country row
                value = row[month]
                if pd.notna(value):
                    calculated_total += float(value)
                    month_data["countries"].append({
                        "name": country.lower(),
                        "value": float(value)
                    })

        print(f"Calculated total for {month}: {calculated_total}")
        # Get total from the last row for this month
        total_from_row = (
            float(df.iloc[-1][month])
            if pd.notna(df.iloc[-1][month])
            else calculated_total
        )

        # Add totals to month_data
        month_data["total"] = total_from_row
        month_data["calculated_total"] = calculated_total

        monthly_data.append(month_data)

    # Process country-wise data (keeping this part unchanged for backward compatibility)
    country_data = []
    for _, row in df.iterrows():
        country_col = next((col for col in df.columns if col.upper() == "COUNTRY"), None)
        if country_col:
            country = str(row[country_col]).strip()
        if not country or (pd.notna(row["No"]) and str(row["No"]).lower() == "total"):
            continue

        total_from_column = (
            float(row["Total"])
            if "Total" in df.columns and pd.notna(row["Total"])
            else 0
        )

        calculated_total = float(
            sum(row[month] for month in available_months if pd.notna(row[month]))
        )

        if total_from_column == 0:
            total_from_column = calculated_total

        country_info = {
            "year": year,
            "country": country.lower(),
            "total": total_from_column,
            "calculated_total": calculated_total,
        }

        print(f"Country: {country_info}")

        for month in available_months:
            if pd.notna(row[month]):
                country_info[all_months[available_months.index(month)]] = float(row[month])
        country_data.append(country_info)

    return monthly_data, country_data


def save_to_mongodb(monthly_data, country_data, year):
    try:
        print(f"\nMonthly data count: {len(monthly_data)}")
        print(f"Country data count: {len(country_data)}")

        if not monthly_data or not country_data:
            print("Warning: No data to save!")
            return False

        # Delete existing data for the year
        monthly_result = monthly_collection.delete_many({"year": year})
        country_result = country_collection.delete_many({"year": year})
        print(f"Deleted existing monthly records: {monthly_result.deleted_count}")
        print(f"Deleted existing country records: {country_result.deleted_count}")

        # Insert new data
        if monthly_data:
            monthly_result = monthly_collection.insert_many(monthly_data)
            print(f"Inserted monthly records: {len(monthly_result.inserted_ids)}")

        if country_data:
            country_result = country_collection.insert_many(country_data)
            print(f"Inserted country records: {len(country_result.inserted_ids)}")

        print(f"Successfully saved data for {year} to MongoDB")
        return True
    except Exception as e:
        print(f"Error saving to MongoDB: {str(e)}")
        print(
            "Monthly data sample:",
            monthly_data[0] if monthly_data else "No monthly data",
        )
        print(
            "Country data sample:",
            country_data[0] if country_data else "No country data",
        )
        return False


def main():
    # Process all Excel files in the tourism directory
    tourism_dir = Path("data/tourism")
    for excel_file in tourism_dir.glob("*.xlsx"):
        print(f"Found file: {excel_file}")

        try:
            # Check if filename matches the required pattern
            file_name = excel_file.stem.split("_")
            if (
                len(file_name) != 5
                or file_name[0] != "All"
                or file_name[1] != "Countries"
            ):
                print(
                    f"Skipping {excel_file}: Does not match required format 'All_Countries_<YEAR>_<MONTH>_<MONTH>.xlsx'"
                )
                continue

            try:
                year = int(file_name[2])
            except ValueError:
                print(
                    f"Skipping {excel_file}: Year {file_name[2]} is not a valid number"
                )
                continue

            starting_month = file_name[3]
            ending_month = file_name[4]

            # month is in short format I need to get number of months from starting month to ending month. but we have string months
            valid_months = [
                "Jan",
                "Feb",
                "Mar",
                "Apr",
                "May",
                "Jun",
                "Jul",
                "Aug",
                "Sep",
                "Oct",
                "Nov",
                "Dec",
            ]
            if starting_month not in valid_months or ending_month not in valid_months:
                print(
                    f"Skipping {excel_file}: Months must be in short format (Jan, Feb, etc.)"
                )
                continue

            print(f"Processing {excel_file}")

            starting_month_index = valid_months.index(starting_month)
            ending_month_index = valid_months.index(ending_month)
            print(f"Starting month index: {starting_month_index}")
            print(f"Ending month index: {ending_month_index}")

            # Process the data
            monthly_data, country_data = process_tourism_data(
                excel_file, year, starting_month_index, ending_month_index
            )

            # Save to MongoDB
            if save_to_mongodb(monthly_data, country_data, year):
                # Move file to processed directory only if MongoDB save was successful
                move_to_processed(excel_file)
                print(f"Completed processing data for {year}")
            else:
                print(f"Failed to process {excel_file}")

        except Exception as e:
            print(f"Error processing {excel_file}: {str(e)}")


if __name__ == "__main__":
    main()
