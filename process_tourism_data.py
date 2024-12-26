from re import S
import pandas as pd
import json
from pathlib import Path
import os
import numpy as np
from pandas.io.sql import _process_parse_dates_argument
from pymongo import MongoClient
from dotenv import load_dotenv
import shutil
from datetime import datetime

# Load environment variables
load_dotenv()

# MongoDB connection
MONGO_URI = os.getenv("MONGODB_URI")
DB_NAME = os.getenv("MONGODB_DB_NAME", "data-visualizer")

if not MONGO_URI:
    raise ValueError("MONGO_URI environment variable is not set")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
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
    available_months.insert(0, "Country")
    available_months.insert(0, "No")
    available_months.append("Total")

    print("\nAvailable months:", available_months)

    # Rename columns
    df.columns = available_months

    numeric_columns.append("Total")
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
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]

    available_months = df.columns[2 : 2 + ending_month_index + 1].tolist()

    print("\nAvailable months:", available_months)
    # Process monthly data
    monthly_data = []
    for month in available_months:
        month_data = {
            "year": year,
            "Month": all_months[available_months.index(month)],
        }

        # Calculate total for current month
        calculated_total = 0
        # Skip the last row (total row)
        for _, row in df.iloc[:-1].iterrows():
            country = str(row["Country"]).strip()
            if country and pd.notna(row["No"]):  # Valid country row
                value = row[month]
                if pd.notna(value):
                    calculated_total += float(value)
                    month_data[country] = float(value)  # Add country data

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

    # Process country-wise data
    country_data = []
    for _, row in df.iterrows():
        country = str(row["Country"]).strip()
        # Skip the total row and empty countries
        if not country or (pd.notna(row["No"]) and str(row["No"]).lower() == "total"):
            continue

        # Get total from 'Total' column if it exists
        total_from_column = (
            float(row["Total"])
            if "Total" in df.columns and pd.notna(row["Total"])
            else 0
        )

        # Calculate total by summing available monthly values
        calculated_total = float(
            sum(row[month] for month in available_months if pd.notna(row[month]))
        )

        # If no total column or total is 0, use calculated total
        if total_from_column == 0:
            total_from_column = calculated_total

        country_info = {
            "Year": year,
            "Country": country,
            "total": total_from_column,  # Total from 'Total' column or calculated
            "calculated_total": calculated_total,  # Total calculated from monthly values
        }
        # Add monthly data for available months
        for month in available_months:
            if pd.notna(row[month]):
                country_info[all_months[available_months.index(month)]] = float(
                    row[month]
                )
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
        country_result = country_collection.delete_many({"Year": year})
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
        print(f"Processing {excel_file}")

        try:
            # Get the year from filename
            file_name = Path(excel_file).stem.split("_")
            year = int(file_name[2])
            starting_month = file_name[3]
            ending_month = file_name[4]

            # month is in short format I need to get number of months from starting month to ending month. but we have string months
            month = [
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
            starting_month_index = month.index(starting_month)
            ending_month_index = month.index(ending_month)
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
