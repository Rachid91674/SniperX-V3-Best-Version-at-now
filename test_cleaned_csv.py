import os
import csv

def main():
    # Path to the cleaned CSV file
    csv_file = os.path.join(os.path.dirname(__file__), "token_risk_analysis_cleaned.csv")
    
    if not os.path.exists(csv_file):
        print(f"Error: File not found: {csv_file}")
        return
    
    print(f"Testing CSV file: {csv_file}")
    print(f"File size: {os.path.getsize(csv_file)} bytes")
    
    # Try reading the file directly
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            # Read the first 10 lines
            print("\nFirst 10 lines:")
            print("-" * 80)
            for i, line in enumerate(f):
                if i >= 10:
                    break
                print(f"{i+1}: {line.strip()}")
            print("-" * 80)
            
            # Reset file pointer
            f.seek(0)
            
            # Try reading with csv.DictReader
            print("\nTrying to read with csv.DictReader:")
            reader = csv.DictReader(f)
            
            if not reader.fieldnames:
                print("Error: No headers found in CSV file")
                return
                
            print(f"Headers: {', '.join(reader.fieldnames)}")
            
            # Print first 3 rows
            print("\nFirst 3 rows:")
            for i, row in enumerate(reader):
                if i >= 3:
                    break
                print(f"Row {i+1}:")
                for key, value in row.items():
                    print(f"  {key}: {value}")
                print()
                
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
