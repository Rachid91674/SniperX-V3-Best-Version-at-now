import os
import chardet
import csv

def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        raw_data = f.read()
        result = chardet.detect(raw_data)
        return result['encoding']

def clean_csv_file(input_file, output_file):
    # Detect file encoding
    try:
        encoding = detect_encoding(input_file)
        print(f"Detected encoding: {encoding}")
        
        # Read the file with detected encoding
        with open(input_file, 'r', encoding=encoding, errors='replace') as f:
            content = f.read()
        
        # Clean up content
        # Remove null bytes
        content = content.replace('\x00', '')
        # Normalize line endings
        content = content.replace('\r\n', '\n').replace('\r', '\n')
        # Remove any remaining control characters except newlines and tabs
        content = ''.join(char for char in content if ord(char) >= 32 or char in '\n\t')
        
        # Write cleaned content to output file with UTF-8 encoding
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(content)
            
        print(f"Successfully cleaned and saved to {output_file}")
        return True
        
    except Exception as e:
        print(f"Error processing file: {e}")
        return False

if __name__ == "__main__":
    input_file = os.path.join(os.path.dirname(__file__), "token_risk_analysis.csv")
    output_file = os.path.join(os.path.dirname(__file__), "token_risk_analysis_cleaned.csv")
    
    if os.path.exists(input_file):
        print(f"Processing file: {input_file}")
        if clean_csv_file(input_file, output_file):
            print("CSV file has been successfully cleaned and saved.")
        else:
            print("Failed to clean the CSV file.")
    else:
        print(f"Error: File not found: {input_file}")
