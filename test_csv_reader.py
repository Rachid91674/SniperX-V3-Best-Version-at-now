import os
import csv

def test_csv_reading(csv_file_path):
    print(f"\n=== Testing CSV Reading ===")
    print(f"File: {os.path.abspath(csv_file_path)}")
    print(f"File size: {os.path.getsize(csv_file_path)} bytes")
    
    # Try different encodings
    encodings = ['utf-8', 'latin-1', 'windows-1252', 'utf-16']
    
    for encoding in encodings:
        print(f"\n--- Trying encoding: {encoding} ---")
        try:
            # Read as binary first to detect BOM
            with open(csv_file_path, 'rb') as f:
                raw_content = f.read()
                print(f"First 200 bytes as hex: {raw_content[:200].hex(' ')}")
                print(f"First 200 bytes as text: {raw_content[:200]}")
                
            # Try to read as text
            with open(csv_file_path, 'r', encoding=encoding) as f:
                # Read first 5 lines
                for i in range(5):
                    try:
                        line = next(f)
                        print(f"Line {i+1}: {line.strip()}")
                    except StopIteration:
                        break
                        
            # Try with csv reader
            print(f"\nTrying CSV reader with {encoding}...")
            with open(csv_file_path, 'r', encoding=encoding) as f:
                try:
                    # Try to detect dialect
                    sample = f.read(1024)
                    f.seek(0)
                    dialect = csv.Sniffer().sniff(sample)
                    print(f"Detected dialect - delimiter: {repr(dialect.delimiter)}, quotechar: {repr(dialect.quotechar)}")
                    
                    # Try to read with detected dialect
                    reader = csv.reader(f, dialect=dialect)
                    headers = next(reader)
                    print(f"Headers: {headers}")
                    
                    # Show first few rows
                    print("\nFirst 3 rows:")
                    for i, row in enumerate(reader):
                        if i >= 3:
                            break
                        print(f"Row {i+1}: {row}")
                        
                except Exception as e:
                    print(f"Error with CSV reader: {e}")
                    
        except Exception as e:
            print(f"Error with {encoding}: {e}")

if __name__ == "__main__":
    csv_path = os.path.join(os.path.dirname(__file__), "token_risk_analysis.csv")
    test_csv_reading(csv_path)
