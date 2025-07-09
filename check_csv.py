import csv
import os

csv_path = r"c:\Users\Rachid Aitali\OneDrive - 4JM Solutions Limited\Desktop\SniperX-V3-Best-Version-at-now\token_risk_analysis.csv"

print(f"Checking CSV file: {csv_path}")
print(f"File exists: {os.path.exists(csv_path)}")
print(f"File size: {os.path.getsize(csv_path)} bytes")

try:
    with open(csv_path, 'r', encoding='utf-8') as f:
        print("\n--- First 5 lines of the file: ---")
        for i, line in enumerate(f):
            if i >= 5:  # Only show first 5 lines
                break
            print(f"{i+1}: {line.strip()}")
            
    # Now try to read as CSV
    print("\n--- Attempting to parse as CSV: ---")
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        print(f"CSV Headers: {', '.join(reader.fieldnames) if reader.fieldnames else 'None'}")
        
        # Try to read first data row
        try:
            first_row = next(reader, None)
            if first_row:
                print("\nFirst data row:")
                for key, value in first_row.items():
                    print(f"  {key}: {value}")
            else:
                print("No data rows found in CSV")
        except Exception as e:
            print(f"Error reading first data row: {e}")
            
except UnicodeDecodeError:
    print("Failed to read with UTF-8 encoding, trying with latin-1...")
    with open(csv_path, 'r', encoding='latin-1') as f:
        print("\n--- First 5 lines (latin-1): ---")
        for i, line in enumerate(f):
            if i >= 5:
                break
            print(f"{i+1}: {line.strip()}")
            
except Exception as e:
    print(f"Error processing file: {e}")
