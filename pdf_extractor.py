import os
import re
import pdfplumber
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta

# MongoDB connection
client = MongoClient("mongodb://root:secret@localhost:27017/")
db = client["central_bank"]
collection = db["row_data"]

should_process_pdf = None

# Dictionary to store order values

ORDER_VALUES = {
    "M_13": [2, 3, 4, 6, 7, 8, 9, 10, 11, 12],

    "M_14_1": [2, 3, 4, 6, 8, 9, 10, 11, 12, 13],
    "M_14_2": [2, 3, 4, 7, 8, 9, 10, 11, 12, 13],

    "M_15": [2, 3, 4, 7, 9, 10, 11, 12, 13, 14],

    "M_16": [2, 4, 5, 8, 10, 11, 12, 13, 14, 15],

    "M_18": [3, 5, 6, 8, 9, 10, 12, 13, 15, 17],

    "M_19": [3, 5, 6, 8, 10, 11, 13, 14, 16, 18],

    "M_20_1": [3, 5, 7, 9, 11, 12, 14, 15, 17, 19],
    "M_20_2": [2, 5, 6, 9, 11, 12, 14, 15, 17, 19],

    "M_21_1": [4, 6, 7, 10, 12, 13, 15, 17, 19, 20],
    "M_21_2": [3, 5, 6, 8, 11, 13, 15, 16, 18, 20],

    "O_13_1": [2, 3, 4, 6, 7, 8, 9, 10, 11, 12],
    "O_13_2": [2, 3, 4, 5, 7, 8, 9, 10, 11, 12],

    "O_14_1": [2, 3, 4, 6, 8, 9, 10, 11, 12, 13],
    "O_14_2": [2, 3, 4, 7, 8, 9, 10, 11, 12, 13],

    "O_15_1": [2, 3, 4, 6, 7, 8, 9, 10, 12, 14],
    "O_15_2": [2, 3, 4, 7, 9, 10, 11, 12, 13, 14],
    "O_15_3": [2, 3, 4, 6, 9, 10, 11, 12, 13, 14],
    
    "O_16_1": [3, 4, 5, 7, 8, 9, 10, 11, 13, 15],
    "O_16_2": [3, 5, 6, 8, 10, 11, 12, 13, 14, 15],

    "O_17_1": [3, 5, 6, 8, 7, 10, 11, 12, 14, 16],
    "O_17_2": [2, 3, 4, 6, 7, 9, 11, 13, 14, 16],
    "O_17_3": [3, 4, 6, 8, 10, 11, 12, 14, 15, 16],

    "O_18_1": [2, 3, 4, 7, 9, 10, 11, 13, 15, 17],
    "O_18_2": [2, 3, 4, 7, 8, 9, 11, 13, 15, 17],
    "O_18_3": [3, 5, 6, 8, 10, 11, 12, 13, 15, 17],
    "O_18_4": [3, 5, 6, 8, 9, 10, 11, 13, 15, 17],

    "O_19_1": [3, 5, 6, 8, 9, 10, 12, 14, 16, 18],
    "O_19_2": [3, 5, 7, 9, 11, 12, 14, 15, 16, 18],
    "O_19_3": [3, 5, 6, 9, 10, 11, 12, 14, 16, 18],

    "O_20_1": [2, 3, 4, 7, 9, 11, 13, 15, 17, 19],
    "O_20_2": [2, 3, 4, 7, 10, 12, 13, 15, 17, 19],
    "O_20_3": [3, 5, 6, 7, 10, 12, 13, 15, 17, 19],
    "O_20_4": [3, 5, 7, 9, 10, 12, 14, 15, 17, 19],

    "O_21_1": [3, 4, 7, 9, 12, 14, 15, 17, 19, 20],
    "O_21_2": [2, 3, 5, 8, 10, 12, 14, 16, 18, 20],
    "O_21_3": [2, 3, 4, 8, 11, 13, 14, 16, 18, 20],
    "O_21_4": [2, 3, 4, 7, 10, 12, 14, 16, 18, 20],
    "O_21_5": [2, 3, 6, 8, 11, 13, 14, 16, 18, 20],
    "O_21_6": [3, 5, 6, 8, 11, 13, 15, 16, 18, 20],

    "O_22_1": [2, 3, 5, 8, 11, 13, 15, 17, 19, 21],
    "O_22_2": [3, 5, 7, 10, 12, 13, 15, 17, 19, 21],
    "O_22_3": [2, 3, 5, 9, 11, 13, 15, 17, 19, 21],
}


def safe_get_price(cell):
    """Safely get price from cell"""
    try:
        if cell and not pd.isna(cell):
            # Convert cell to string and remove extra spaces
            price_str = str(cell).strip()

            # Return empty if n.a.
            if price_str == "n.a.":
                return "", ""

            # Handle multiple prices separated by newlines
            if "\n" in price_str:
                prices = price_str.split("\n")
                if len(prices) >= 2:
                    # Return both prices
                    price1 = prices[0].replace(" ", "")
                    price2 = prices[1].replace(" ", "")
                    return price1, price2

            # Remove spaces but keep commas for now
            price_str = price_str.replace(" ", "")

            # Return the price as is, with commas preserved
            return price_str, price_str

    except Exception as e:
        print(f"Error processing price: {str(e)}")
    return "", ""


def find_section_boundaries(table, header_row_idx):
    """Find the start and end indices for a section in the table"""
    start_idx = None
    end_idx = None

    # Search for the VEGETABLES section header
    for i in range(header_row_idx, len(table)):
        row = table[i]
        if not row:
            continue

        # Join the row elements to check for section header
        row_text = " ".join(str(cell) for cell in row if cell)
        if "V E G" in row_text:
            start_idx = i + 2  # Skip the empty row after header
            break

    if start_idx is None:
        print("Could not find VEGETABLES section start")
        return None, None

    # Search for the end of section (next section header or empty rows)
    section_markers = ["O T H E R", "F R U I T S", "R I C E", "F I S H"]
    empty_row_count = 0

    for i in range(start_idx, len(table)):
        row = table[i]
        if not row or all(not cell for cell in row):
            empty_row_count += 1
            if empty_row_count >= 3:  # Three consecutive empty rows
                end_idx = i - 2  # Go back to before empty rows
                break
        else:
            empty_row_count = 0

        # Check for next section header
        row_text = " ".join(str(cell) for cell in row if cell)
        for marker in section_markers:
            if marker in row_text:
                end_idx = i
                break

        if end_idx:
            break

    # If no clear end found, use a default value
    if not end_idx:
        print("Could not find section end")
        # Set end index to start + 20 rows or table length, whichever is smaller
        end_idx = min(start_idx + 20, len(table))
        print(f"Using default end index: {end_idx}")

    return start_idx, end_idx


def find_other_section_boundaries(table, header_row_idx):
    """Find the start and end indices for the Other section in the table"""
    start_idx = None
    end_idx = None

    # Search for the OTHER section header
    for i in range(header_row_idx, len(table)):
        row = table[i]
        if not row:
            continue

        # Join the row elements to check for section header
        row_text = " ".join(str(cell) for cell in row if cell)
        if "O T H E R" in row_text:
            start_idx = i + 2  # Skip the empty row after header
            break

    if start_idx is None:
        print("Could not find OTHER section start")
        return None, None

    # Search for the end of section (next section header or empty rows)
    section_markers = ["F R U I T S", "R I C E", "F I S H"]
    empty_row_count = 0

    for i in range(start_idx, len(table)):
        row = table[i]
        if not row or all(not cell for cell in row):
            empty_row_count += 1
            if empty_row_count >= 3:  # Three consecutive empty rows
                end_idx = i - 2  # Go back to before empty rows
                break
        else:
            empty_row_count = 0

        # Check for next section header
        row_text = " ".join(str(cell) for cell in row if cell)
        for marker in section_markers:
            if marker in row_text:
                end_idx = i
                break

        if end_idx:
            break

    # If no clear end found, use a default value
    if not end_idx:
        print("Could not find section end")
        # Set end index to start + 20 rows or table length, whichever is smaller
        end_idx = min(start_idx + 20, len(table))
        print(f"Using default end index: {end_idx}")

    return start_idx, end_idx


def find_fruits_section_boundaries(table, header_row_idx):
    """Find the start and end indices for the Fruits section in the table"""
    start_idx = None
    end_idx = None

    # Search for the FRUITS section header
    for i in range(header_row_idx, len(table)):
        row = table[i]
        if not row:
            continue

        # Join the row elements to check for section header
        row_text = " ".join(str(cell) for cell in row if cell)
        if "F R U I T S" in row_text:
            start_idx = i + 2  # Skip the empty row after header
            break

    if start_idx is None:
        print("Could not find FRUITS section start")
        return None, None

    # Search for the end of section (next section header or empty rows)
    section_markers = ["R I C E", "F I S H"]
    empty_row_count = 0

    for i in range(start_idx, len(table)):
        row = table[i]
        if not row or all(not cell for cell in row):
            empty_row_count += 1
            if empty_row_count >= 3:  # Three consecutive empty rows
                end_idx = i - 2  # Go back to before empty rows
                break
        else:
            empty_row_count = 0

        # Check for next section header
        row_text = " ".join(str(cell) for cell in row if cell)
        for marker in section_markers:
            if marker in row_text:
                end_idx = i
                break

        if end_idx:
            break

    # If no clear end found, use a default value
    if not end_idx:
        print("Could not find section end")
        # Set end index to start + 20 rows or table length, whichever is smaller
        end_idx = min(start_idx + 20, len(table))
        print(f"Using default end index: {end_idx}")

    return start_idx, end_idx


def find_rice_section_boundaries(table, header_row_idx):
    """Find the start and end indices for the Rice section in the table"""
    start_idx = None
    end_idx = None

    # Search for the RICE section header
    for i in range(header_row_idx, len(table)):
        row = table[i]
        if not row:
            continue

        # Join the row elements to check for section header
        row_text = " ".join(str(cell) for cell in row if cell)
        if "R I C E" in row_text:
            start_idx = i + 4  # Skip the empty row after header
            break

    if start_idx is None:
        print("Could not find RICE section start")
        return None, None

    # Search for the end of section (next section header or empty rows)
    section_markers = ["F I S H"]
    empty_row_count = 0

    for i in range(start_idx, len(table)):
        row = table[i]
        if not row or all(not cell for cell in row):
            empty_row_count += 1
            if empty_row_count >= 3:  # Three consecutive empty rows
                end_idx = i - 2  # Go back to before empty rows
                break
        else:
            empty_row_count = 0

        # Check for next section header
        row_text = " ".join(str(cell) for cell in row if cell)
        for marker in section_markers:
            if marker in row_text:
                end_idx = i
                break

        if end_idx:
            break

    # If no clear end found, use a default value
    if not end_idx:
        print("Could not find section end")
        # Set end index to start + 20 rows or table length, whichever is smaller
        end_idx = min(start_idx + 20, len(table))
        print(f"Using default end index: {end_idx}")

    return start_idx, end_idx


def find_fish_section_boundaries(table, header_row_idx):
    """Find the start and end indices for the Fish section in the table"""
    start_idx = None
    end_idx = None

    # Search for the FISH section header
    for i in range(header_row_idx, len(table)):
        row = table[i]
        if not row:
            continue

        # Join the row elements to check for section header
        row_text = " ".join(str(cell) for cell in row if cell)
        if "F I S H" in row_text:
            start_idx = i + 4  # Skip the empty row after header
            break

    if start_idx is None:
        print("Could not find FISH section start")
        return None, None

    # Search for the end of section (next section header or empty rows)
    empty_row_count = 0

    for i in range(start_idx, len(table)):
        row = table[i]
        if not row or all(not cell for cell in row):
            empty_row_count += 1
            if empty_row_count >= 3:  # Three consecutive empty rows
                end_idx = i - 2  # Go back to before empty rows
                break
        else:
            empty_row_count = 0

        if end_idx:
            break

    # If no clear end found, use the end of the table
    if not end_idx:
        end_idx = len(table)

    # Skip last two rows
    if end_idx and end_idx - start_idx > 2:
        end_idx = end_idx - 3

    return start_idx, end_idx


def clean_price(row, index):
    """
    Clean and convert price string to float.
    Returns "N/A" for null or invalid values.
    """
    price_str = row[index]
    try:
        if (
            not price_str
            or pd.isna(price_str)
            or price_str == "n.a."
            or "n.a" in price_str
            or "n.a." in price_str
        ):
            return "N/A"

        # Remove spaces
        if ".00" in price_str:
            price_str = (price_str.split(".00")[0]).replace(" ", "")
        else:
            return "N/A"

        # If the price starts with a comma, add a leading digit
        # if price_str.startswith(',') and index > 1:
        if index == 2 and " " in row[1]:
            price_str = row[1].split(" ")[1] + price_str
        if index > 2:
            if ".00" in row[index - 1]:
                price_str = row[index - 1].split(".00")[1] + price_str
            else:
                price_str = row[index - 1] + price_str
        # Now convert to float, after removing the comma
        float_value = float(
            price_str.replace(",", "").replace(" ", "").replace("n.a.", "")
        )
        returnValue = str(float_value) if float_value else "N/A"
        return returnValue
    except Exception as e:
        print(f"Error cleaning price {price_str}: {str(e)}")
        return "N/A"


def extract_price_pair(row, yesterday_index, today_index):
    """Extract yesterday and today prices from a row."""
    yesterday = (
        clean_price(row, yesterday_index) if len(row) > yesterday_index else "N/A"
    )
    today = clean_price(row, today_index) if len(row) > today_index else "N/A"
    return yesterday, today


def extract_row_data(row, order_id):
    """Extract row data from a row for Monday reports."""
    if not order_id in ORDER_VALUES:
        return None

    values = ORDER_VALUES[order_id]
    # Now you can access values by index
    yesterday_pair1, today_pair1 = extract_price_pair(row, values[0], values[1])
    yesterday_pair2, today_pair2 = extract_price_pair(row, values[2], values[3])
    yesterday_pair3, today_pair3 = extract_price_pair(row, values[4], values[5])
    yesterday_pair4, today_pair4 = extract_price_pair(row, values[6], values[7])
    yesterday_pair5, today_pair5 = extract_price_pair(row, values[8], values[9])

    print(
        f"{order_id}: Pettah wholesale: Yesterday ({values[0]})={yesterday_pair1}, Today ({values[1]})={today_pair1}"
    )
    print(
        f"{order_id}: Dambulla wholesale: Yesterday ({values[2]})={yesterday_pair2}, Today ({values[3]})={today_pair2}"
    )
    print(
        f"{order_id}: Pettah retail: Last Friday ({values[4]})={yesterday_pair3}, Today ({values[5]})={today_pair3}"
    )
    print(
        f"{order_id}: Dambulla retail: Last Friday ({values[6]})={yesterday_pair4}, Today ({values[7]})={today_pair4}"
    )
    print(
        f"{order_id}: Narahenpita retail: Last Friday ({values[8]})={yesterday_pair5}, Today ({values[9]})={today_pair5}"
    )

    return (
        yesterday_pair1,
        today_pair1,
        yesterday_pair2,
        today_pair2,
        yesterday_pair3,
        today_pair3,
        yesterday_pair4,
        today_pair4,
        yesterday_pair5,
        today_pair5,
    )


def has_key_variants(key):
    """
    Check if a key has variants in ORDER_VALUES dictionary.
    Example: If key is 'M_19', it will check for 'M_19_1', 'M_19_2', etc.
    Returns True if variants exist, False otherwise.
    """
    # Look for any keys that start with the base key and have a variant number
    variants = [k for k in ORDER_VALUES.keys() if k.startswith(key + "_")]
    return len(variants) > 0


def get_key_variants(key):
    """
    Get all variants of a key from ORDER_VALUES dictionary.
    Example: If key is 'M_19', it will return ['M_19_1', 'M_19_2', 'M_19_3']
    Returns empty list if no variants exist.
    """
    return [k for k in ORDER_VALUES.keys() if k.startswith(key + "_")]


def extract_prices(row, key):
    """
    Extract and clean price values from a row.
    Returns tuple of (pettah_wholesale_yesterday, pettah_wholesale_today,
                     dambulla_wholesale_yesterday, dambulla_wholesale_today,
                     pettah_retail_yesterday, pettah_retail_today,
                     dambulla_retail_yesterday, dambulla_retail_today,
                     narahenpita_retail_yesterday, narahenpita_retail_today)
    All values will be strings, with "N/A" for null values.
    """
    global should_process_pdf

    try:
        # Print row contents for debugging
        print(f"Raw row data: {row}")
        print(f"Row length: {len(row)}")
        # For Monday reports, the indices are different due to "Last Friday" format
        print(f"Using key: {key}")
        prices = extract_row_data(row, key)
        
        # Check if prices is None before proceeding
        if prices is None:
            print(f"Warning: Could not extract prices for key {key}")
            return ("N/A",) * 10  # Return N/A for all 10 price fields
            
        # should_process_pdf = "y" == "y"
        if row[0] and row[0].strip() == "Beans":
            if should_process_pdf is None:  # Only ask if we haven't decided yet
                user_input = input(
                    "\nFound 'Beans' in the data. Do you want to process this PDF? (y/n): "
                )
                should_process_pdf = user_input.lower() == "y"
                if not should_process_pdf:
                    return None  # Return None to indicate we should skip this PDF

        return (
            prices[0],
            prices[1],
            prices[2],
            prices[3],
            prices[4],
            prices[5],
            prices[6],
            prices[7],
            prices[8],
            prices[9],
        )

    except Exception as e:
        print(f"Error extracting prices: {str(e)}")
        return ("N/A",) * 10  # Return N/A for all 10 price fields


def get_order_key(row, is_monday_format):
    """
    Determine the key for ORDER_VALUES based on the format and row length.
    If the key has variants, it will be handled by the calling function.

    Args:
        row (list): The row of data
        is_monday_format (bool): Whether the format is Monday format

    Returns:
        str: The key in format 'M_{length}' for Monday format or 'O_{length}' for other formats
    """
    print(f"First Row: {row}")
    for i, cell in enumerate(row):
        if not cell:
            continue
        print(f"{str(i).rjust(2)} | {cell}")
    key = f"M_{len(row)}" if is_monday_format else f"O_{len(row)}"

    if has_key_variants(key):
        variants = get_key_variants(key)
        print(f"Variants for {key}: {variants}")
        for i, variant in enumerate(variants):
            print(f"{i + 1}: {ORDER_VALUES[variant]}")
        key = input(f"Choose a variant for {key}: ")
        return variants[int(key) - 1]
    return key


def process_table_data(table):
    """Convert table data to a MongoDB-compatible format with proper column mapping"""
    global should_process_pdf

    if should_process_pdf is False:  # If user chose not to process, return empty list
        return []

    if not table:
        return []

    # Find the header row index
    header_row_idx = None
    is_monday_format = False

    # First find the header row with "WHOLESALE"
    for idx, row in enumerate(table):
        if row and any("WHOLESALE" in str(cell).upper() for cell in row):
            header_row_idx = idx
            break

    if header_row_idx is None:
        print("Could not find header row")
        return []

    # Now look for "Last Friday" in the next few rows after header
    for i in range(header_row_idx, min(header_row_idx + 10, len(table))):
        if i < len(table):
            row = table[i]
            # Join all cells in the row to handle split text
            row_text = " ".join(str(cell) for cell in row if cell)
            if "LAST" in row_text.upper() and "FRIDAY" in row_text.upper():
                is_monday_format = True
                break
            # Also check individual cells for "Last Friday"
            for cell in row:
                if (
                    cell
                    and "LAST" in str(cell).upper()
                    and "FRIDAY" in str(cell).upper()
                ):
                    is_monday_format = True
                    break

    print(
        f"Header format: {'Monday (Last Friday)' if is_monday_format else 'Regular (Yesterday)'}"
    )
    print(f"Header row index: {header_row_idx}")
    if header_row_idx is not None and header_row_idx + 3 < len(table):
        print(f"Header rows:")
        for i in range(header_row_idx, header_row_idx + 4):
            if i < len(table):
                print(f"Row {i}: {table[i]}")

    all_data = []

    # Now I need to desided the what is the key of ORDER_VALUES, If given key don't have variants then the key is equal to the given key
    # If given key have variants then the key need to get as a user input in interactive way.
    # given key is equal to if is_monday_format is true then 'M_{length}' else 'O_{length}'

    key = ""
    key_processed = False
    # Process vegetables section
    veg_start_idx, veg_end_idx = find_section_boundaries(table, header_row_idx)
    if veg_start_idx is not None and veg_end_idx is not None:
        for row in table[veg_start_idx:veg_end_idx]:
            if row and any(row):  # Skip empty rows
                item_name = str(row[0]).strip() if row[0] else ""
                if item_name and item_name.lower() != "item":
                    print(f"\nProcessing row for item: {item_name}")
                    if not key_processed:
                        key = get_order_key(row, is_monday_format)
                        key_processed = True
                    print(f"using key: {key}")
                    prices = extract_prices(row, key)
                    if prices is not None and any(price != 'N/A' for price in prices):  # Only add if we have any non-N/A price data
                        data_item = {
                            "type": "vegetables",
                            "item": item_name,
                            "pettah_wholesale": {
                                "yesterday": prices[0],
                                "today": prices[1],
                            },
                            "dambulla_wholesale": {
                                "yesterday": prices[2],
                                "today": prices[3],
                            },
                            "pettah_retail": {
                                "yesterday": prices[4],
                                "today": prices[5],
                            },
                            "dambulla_retail": {
                                "yesterday": prices[6],
                                "today": prices[7],
                            },
                            "narahenpita_retail": {
                                "yesterday": prices[8],
                                "today": prices[9],
                            },
                            "timestamp": datetime.now(),
                        }
                        all_data.append(data_item)

    # Process other section with the same format changes
    other_start_idx, other_end_idx = find_other_section_boundaries(
        table, header_row_idx
    )
    if other_start_idx is not None and other_end_idx is not None:
        for row in table[other_start_idx:other_end_idx]:
            if row and any(row):  # Skip empty rows
                item_name = str(row[0]).strip() if row[0] else ""
                if item_name and item_name.lower() != "item":
                    print(f"\nProcessing row for item: {item_name}")
                    print(f"using key: {key}")
                    prices = extract_prices(row, key)
                    if prices is not None and any(price != 'N/A' for price in prices):  # Only add if we have any non-N/A price data
                        data_item = {
                            "type": "other",
                            "item": item_name,
                            "pettah_wholesale": {
                                "yesterday": prices[0],
                                "today": prices[1],
                            },
                            "dambulla_wholesale": {
                                "yesterday": prices[2],
                                "today": prices[3],
                            },
                            "pettah_retail": {
                                "yesterday": prices[4],
                                "today": prices[5],
                            },
                            "dambulla_retail": {
                                "yesterday": prices[6],
                                "today": prices[7],
                            },
                            "narahenpita_retail": {
                                "yesterday": prices[8],
                                "today": prices[9],
                            },
                            "timestamp": datetime.now(),
                        }
                        all_data.append(data_item)

    # Process fruits section
    fruits_start_idx, fruits_end_idx = find_fruits_section_boundaries(
        table, header_row_idx
    )
    if fruits_start_idx is not None and fruits_end_idx is not None:
        for row in table[fruits_start_idx:fruits_end_idx]:
            if row and any(row):  # Skip empty rows
                item_name = str(row[0]).strip() if row[0] else ""
                if item_name and item_name.lower() != "item":
                    print(f"\nProcessing row for item: {item_name}")
                    print(f"using key: {key}")
                    prices = extract_prices(row, key)
                    if prices is not None and any(price != 'N/A' for price in prices):  # Only add if we have any non-N/A price data
                        data_item = {
                            "type": "fruits",
                            "item": item_name,
                            "pettah_wholesale": {
                                "yesterday": prices[0],
                                "today": prices[1],
                            },
                            "dambulla_wholesale": {
                                "yesterday": prices[2],
                                "today": prices[3],
                            },
                            "pettah_retail": {
                                "yesterday": prices[4],
                                "today": prices[5],
                            },
                            "dambulla_retail": {
                                "yesterday": prices[6],
                                "today": prices[7],
                            },
                            "narahenpita_retail": {
                                "yesterday": prices[8],
                                "today": prices[9],
                            },
                            "timestamp": datetime.now(),
                        }
                        all_data.append(data_item)

    # Process rice section
    rice_start_idx, rice_end_idx = find_rice_section_boundaries(table, header_row_idx)
    if rice_start_idx is not None and rice_end_idx is not None:
        for row in table[rice_start_idx:rice_end_idx]:
            if row and any(row):  # Skip empty rows
                item_name = str(row[0]).strip() if row[0] else ""
                if item_name and item_name.lower() != "item":
                    print(f"\nProcessing row for item: {item_name}")
                    print(f"using key: {key}")
                    prices = extract_prices(row, key)
                    if prices is not None and any(price != 'N/A' for price in prices):  # Only add if we have any non-N/A price data
                        data_item = {
                            "type": "rice",
                            "item": item_name,
                            "pettah_wholesale": {
                                "yesterday": prices[0],
                                "today": prices[1],
                            },
                            "marandagahamula_wholesale": {
                                "yesterday": prices[2],
                                "today": prices[3],
                            },
                            "pettah_retail": {
                                "yesterday": prices[4],
                                "today": prices[5],
                            },
                            "dambulla_retail": {
                                "yesterday": prices[6],
                                "today": prices[7],
                            },
                            "narahenpita_retail": {
                                "yesterday": prices[8],
                                "today": prices[9],
                            },
                            "timestamp": datetime.now(),
                        }
                        all_data.append(data_item)

    # Process fish section
    fish_start_idx, fish_end_idx = find_fish_section_boundaries(table, header_row_idx)
    if fish_start_idx is not None and fish_end_idx is not None:
        for row in table[fish_start_idx:fish_end_idx]:
            if row and any(row):  # Skip empty rows
                item_name = str(row[0]).strip() if row[0] else ""
                if item_name and item_name.lower() != "item":
                    print(f"\nProcessing row for item: {item_name}")
                    print(f"using key: {key}")
                    prices = extract_prices(row, key)
                    if prices is not None and any(price != 'N/A' for price in prices):  # Only add if we have any non-N/A price data
                        data_item = {
                            "type": "fish",
                            "item": item_name,
                            "peliyagoda_wholesale": {
                                "yesterday": prices[0],
                                "today": prices[1],
                            },
                            "negombo_wholesale": {
                                "yesterday": prices[2],
                                "today": prices[3],
                            },
                            "pettah_retail": {
                                "yesterday": prices[4],
                                "today": prices[5],
                            },
                            "negombo_retail": {
                                "yesterday": prices[6],
                                "today": prices[7],
                            },
                            "narahenpita_retail": {
                                "yesterday": prices[8],
                                "today": prices[9],
                            },
                            "timestamp": datetime.now(),
                        }
                        all_data.append(data_item)

    return all_data


def get_last_friday(date):
    """Get the date of last Friday for a given date"""
    days_since_friday = (date.weekday() - 4) % 7
    last_friday = date - timedelta(days=days_since_friday)
    return last_friday


def is_monday(date):
    """Check if given date is a Monday"""
    return date.weekday() == 0


def extract_pdf_data(pdf_path):
    """
    Extract tables from PDF using pdfplumber and return the data
    """
    global should_process_pdf
    should_process_pdf = None  # Reset the flag for each new PDF
    try:
        print(f"Reading page 2 from {pdf_path}...")
        with pdfplumber.open(pdf_path) as pdf:
            # Get page 2 (0-based index)
            page = pdf.pages[1]

            # Extract table with specific settings
            table = page.extract_table(
                {
                    "vertical_strategy": "text",
                    "horizontal_strategy": "text",
                    "intersection_y_tolerance": 10,
                    "intersection_x_tolerance": 10,
                    "snap_y_tolerance": 3,
                    "snap_x_tolerance": 3,
                    "join_tolerance": 3,
                    "edge_min_length": 3,
                    "min_words_vertical": 3,
                    "min_words_horizontal": 1,
                }
            )

            if not table:
                print(f"No table found on page 2 of {pdf_path}")
                return None

            # Print raw table data for debugging
            # print("\nRaw table data:")
            # for i, row in enumerate(table):
            #     print(f"Row {i}: {row}")

            # Get the date from filename (both formats: price_report_YYYYMMDD.pdf and price_report_YYYYMMDD_e.pdf)
            filename = os.path.basename(pdf_path)

            # Try both patterns
            match = re.match(r"price_report_(\d{8})\.pdf", filename) or re.match(
                r"price_report_(\d{8})_e(?:_\d)?\.pdf", filename
            )

            if not match:
                print(f"Error: Filename {filename} does not match expected format")
                return None

            date_str = match.group(1)  # Extract the date part (YYYYMMDD)
            try:
                date_obj = datetime.strptime(date_str, "%Y%m%d")
            except ValueError:
                print(f"Error: Invalid date format in filename {filename}")
                return None

            # Process the table data
            processed_data = process_table_data(table)

            # Split the data into different sections
            vegetables_data = [
                item for item in processed_data if item["type"] == "vegetables"
            ]
            other_data = [item for item in processed_data if item["type"] == "other"]
            fruits_data = [item for item in processed_data if item["type"] == "fruits"]
            rice_data = [item for item in processed_data if item["type"] == "rice"]
            fish_data = [item for item in processed_data if item["type"] == "fish"]

            # Create separate documents for each section
            documents = []

            if vegetables_data:
                vegetables_document = {
                    "date": date_obj,
                    "type": "vegetables",
                    "page": 2,
                    "table_index": 0,
                    "data": vegetables_data,
                }
                documents.append(vegetables_document)

            if other_data:
                other_document = {
                    "date": date_obj,
                    "type": "other",
                    "page": 2,
                    "table_index": 1,
                    "data": other_data,
                }
                documents.append(other_document)

            if fruits_data:
                fruits_document = {
                    "date": date_obj,
                    "type": "fruits",
                    "page": 2,
                    "table_index": 2,
                    "data": fruits_data,
                }
                documents.append(fruits_document)

            if rice_data:
                rice_document = {
                    "date": date_obj,
                    "type": "rice",
                    "page": 2,
                    "table_index": 3,
                    "data": rice_data,
                }
                documents.append(rice_document)

            if fish_data:
                fish_document = {
                    "date": date_obj,
                    "type": "fish",
                    "page": 2,
                    "table_index": 4,
                    "data": fish_data,
                }
                documents.append(fish_document)

            return documents

    except Exception as e:
        print(f"Error extracting data from {pdf_path}: {str(e)}")
        return None


def main(specific_pdf=None):
    # Create necessary directories if they don't exist
    os.makedirs("reports", exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)

    # Define the regex patterns for both PDF file formats
    pattern = r"^price_report_\d{8}(?:_e(?:_\d)?)?\.pdf$"

    if specific_pdf:
        # Process specific PDF file
        if not os.path.isfile(specific_pdf):
            print(f"Error: File {specific_pdf} not found")
            return

        # Extract data from specific PDF
        extracted_data = extract_pdf_data(specific_pdf)

        if extracted_data:
            # Store in MongoDB
            for document in extracted_data:
                # Use date and table_index as unique identifier
                query = {
                    "date": document["date"],
                    "table_index": document["table_index"],
                }

                # Update or insert the document
                collection.update_one(query, {"$set": document}, upsert=True)

            # Move the processed file to the processed folder
            filename = os.path.basename(specific_pdf)
            # If file is from another location, copy to processed directory and keep original
            processed_path = os.path.join("data", "processed", filename)
            os.rename(specific_pdf, processed_path)
            print(f"Successfully processed and stored data from {filename}")
            print(f"Copied {filename} to processed folder")
        else:
            print(f"Failed to process {os.path.basename(specific_pdf)}")
    else:
        # Process all PDF files in the data directory
        pdf_dir = "data"
        for filename in os.listdir(pdf_dir):
            # Check if the file matches our pattern and is a file (not directory)
            if re.match(pattern, filename) and os.path.isfile(
                os.path.join(pdf_dir, filename)
            ):
                # Extract data from PDF
                pdf_path = os.path.join(pdf_dir, filename)
                extracted_data = extract_pdf_data(pdf_path)

                if extracted_data:
                    # Store in MongoDB
                    for document in extracted_data:
                        # Use date and table_index as unique identifier
                        query = {
                            "date": document["date"],
                            "table_index": document["table_index"],
                        }

                        # Update or insert the document
                        collection.update_one(query, {"$set": document}, upsert=True)

                    # Move the processed file to the processed folder
                    processed_path = os.path.join(pdf_dir, "processed", filename)
                    os.rename(pdf_path, processed_path)
                    print(f"Successfully processed and stored data from {filename}")
                    print(f"Moved {filename} to processed folder")
                else:
                    print(f"Failed to process {filename}")
            else:
                if filename.endswith(".pdf"):
                    print(
                        f"Skipping {filename} as it doesn't match the required format"
                    )


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        # If a PDF file is specified as command line argument
        main(sys.argv[1])
    else:
        # Run with default behavior
        main()
