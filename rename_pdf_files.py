import os
import shutil
from datetime import datetime
import re

def rename_and_move_pdf_files():
    # Source and destination directories
    source_dir = 'test'
    dest_dir = 'data'
    
    # Create destination directory if it doesn't exist
    os.makedirs(dest_dir, exist_ok=True)
    
    # Regular expressions for both filename formats
    pattern1 = r'price_report_(\d{8})_e(?:_\d+)?\.pdf'  # For format: price_report_20240101_e.pdf and price_report_20240101_e_0.pdf
    pattern2 = r'price_report_(\d{8})\.pdf'    # For format: price_report_20240109.pdf
    
    # Process files in the source directory
    for filename in os.listdir(source_dir):
        if not filename.endswith('.pdf'):
            continue
            
        match1 = re.match(pattern1, filename)
        match2 = re.match(pattern2, filename)
        
        if match1:
            date_str = match1.group(1)
        elif match2:
            date_str = match2.group(1)
        else:
            continue
        
        try:
            # Parse the date from the filename
            date_obj = datetime.strptime(date_str, '%Y%m%d')
            # Create new filename in yyyy-MM-dd format
            new_filename = f"{date_obj.strftime('%Y-%m-%d')}.pdf"
            
            # Source and destination paths
            source_path = os.path.join(source_dir, filename)
            dest_path = os.path.join(dest_dir, new_filename)
            
            # Move and rename the file
            shutil.move(source_path, dest_path)
            print(f"Renamed and moved: {filename} -> {new_filename}")
            
        except ValueError as e:
            print(f"Error processing {filename}: {str(e)}")

if __name__ == "__main__":
    rename_and_move_pdf_files()
