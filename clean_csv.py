import os
import re

def clean_csv(input_path, output_path):
    print(f"Cleaning CSV file: {input_path}")
    print(f"Output will be saved to: {output_path}")
    
    # Read the entire file as binary
    with open(input_path, 'rb') as f:
        content = f.read()
    
    # Try to decode with different encodings
    encodings = ['utf-8', 'latin-1', 'windows-1252']
    decoded_content = None
    
    for encoding in encodings:
        try:
            decoded_content = content.decode(encoding)
            print(f"Successfully decoded with {encoding} encoding")
            break
        except UnicodeDecodeError as e:
            print(f"Failed to decode with {encoding}: {e}")
    
    if decoded_content is None:
        print("Failed to decode file with any encoding")
        return False
    
    # Clean up the content
    print("Cleaning content...")
    
    # Remove any null bytes
    cleaned_content = decoded_content.replace('\x00', '')
    
    # Normalize line endings
    cleaned_content = cleaned_content.replace('\r\n', '\n').replace('\r', '\n')
    
    # Remove any Excel formula artifacts
    cleaned_content = re.sub(r'=HYPERLINK\([^)]+\)', '', cleaned_content)
    
    # Remove any other non-printable characters except tabs and newlines
    cleaned_content = re.sub(r'[^\x20-\x7E\t\n]', '', cleaned_content)
    
    # Write the cleaned content to a new file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(cleaned_content)
    
    print(f"Successfully cleaned and saved to {output_path}")
    return True

if __name__ == "__main__":
    input_file = os.path.join(os.path.dirname(__file__), "token_risk_analysis.csv")
    output_file = os.path.join(os.path.dirname(__file__), "token_risk_analysis_cleaned.csv")
    
    if clean_csv(input_file, output_file):
        print("\nFirst 20 lines of cleaned file:")
        print("-" * 50)
        with open(output_file, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i >= 20:
                    break
                print(f"{i+1}: {line.strip()}")
        print("-" * 50)
        print("\nCleaning complete. Try running the Monitoring script with the cleaned file.")
    else:
        print("Failed to clean the CSV file.")
