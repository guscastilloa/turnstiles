import pandas as pd
import os
from datetime import datetime
import sys
import csv

def detect_delimiter(filepath, bytes_to_check=2048):
    """Detect if file uses comma or semicolon as delimiter"""
    with open(filepath, 'r', encoding='utf-8') as file:
        # Read first few lines
        start = file.read(bytes_to_check)
        
        # Count potential delimiters
        semicolons = start.count(';')
        commas = start.count(',')
        
        print(f"\nDelimiter analysis for {os.path.basename(filepath)}:")
        #print(f"Found {semicolons} semicolons and {commas} commas in header")
        
        if semicolons > commas:
            return ';'
        return ','

def get_date_column_name(filepath, delimiter):
    # Read just the header
    try:
        headers = pd.read_csv(filepath, delimiter=delimiter, nrows=0).columns
        #print(f"Columns found: {headers.tolist()}")
        
        # Look for possible date column names
        date_columns = [col for col in headers if 'fecha' in col.lower() or 'date' in col.lower()]
        
        if date_columns:
            #print(f"Found potential date columns: {date_columns}")
            return date_columns[0]
    except Exception as e:
        print(f"Error reading headers with pandas: {e}")
        
        # Fallback: try reading with csv module
        with open(filepath, 'r', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter=delimiter)
            headers = next(reader)
            print(f"Headers from CSV reader: {headers}")
            
            date_columns = [col for col in headers if 'fecha' in col.lower() or 'date' in col.lower()]
            if date_columns:
                return date_columns[0]
    
    raise ValueError(f"No date column found in {filepath}")

def analyze_file_dates(filepath):
    print(f"\nAnalyzing {os.path.basename(filepath)}...")
    sys.stdout.flush()
    
    # Detect delimiter
    delimiter = detect_delimiter(filepath)
    print(f"Using delimiter: '{delimiter}'")
    
    # Get correct date column name
    date_col = get_date_column_name(filepath, delimiter)
    #print(f"Using date column: '{date_col}'")
    
    # Read data in chunks
    chunks = pd.read_csv(filepath, 
                        delimiter=delimiter,
                        usecols=[date_col], 
                        chunksize=1000000,
                        quoting=csv.QUOTE_MINIMAL)  # Handle optional quotes
    
    min_date = None
    max_date = None
    total_rows = 0
    unique_dates = set()
    
    for chunk in chunks:
        # Clean the date strings (remove extra spaces)
        chunk[date_col] = chunk[date_col].str.strip()
        
        # Convert to datetime
        chunk[date_col] = pd.to_datetime(chunk[date_col], 
                                       format='%Y.%m.%d %H:%M:%S', 
                                       errors='coerce')
        
        # Drop any rows where date conversion failed
        chunk = chunk.dropna()
        
        # Update statistics
        if len(chunk) > 0:
            chunk_min = chunk[date_col].min()
            chunk_max = chunk[date_col].max()
            min_date = chunk_min if min_date is None else min(min_date, chunk_min)
            max_date = chunk_max if max_date is None else max(max_date, chunk_max)
            total_rows += len(chunk)
            unique_dates.update(chunk[date_col].unique())
        
        # Print progress
        print(f"Processed {total_rows:,} rows...", end='\r')
        sys.stdout.flush()
    
    return {
        'filename': os.path.basename(filepath),
        'min_date': min_date,
        'max_date': max_date,
        'row_count': total_rows,
        'unique_dates': len(unique_dates),
        'delimiter_used': delimiter
    }


def main():
    P2000_DIR = "/hpcfs/home/economia/ga.castillo/projects/TOR/data/P2000"
    
    print("Starting analysis...")
    
    # Analyze merged file first
    merged_path = os.path.join(P2000_DIR, 'Accesos_P2000.csv')
    if os.path.exists(merged_path):
        print(f"\nReading {merged_path}")
        merged_results = analyze_file_dates(merged_path)
        print("\nMerged File Analysis:")
        print(f"File: {merged_results['filename']}")
        print(f"Delimiter used: '{merged_results['delimiter_used']}'")
        print(f"Date Range: {merged_results['min_date']} to {merged_results['max_date']}")
        print(f"Total Rows: {merged_results['row_count']:,}")
        print(f"Unique Timestamps: {merged_results['unique_dates']:,}")
    
    print("\nMonthly Files Analysis:")
    # Analyze each monthly file
    for file in sorted(os.listdir(P2000_DIR)):
        if file.endswith('.csv') and file != 'Accesos_P2000.csv':
            filepath = os.path.join(P2000_DIR, file)
            print(f"\nReading {filepath}")
            try:
                result = analyze_file_dates(filepath)
                print(f"\nFile: {result['filename']}")
                print(f"Delimiter used: '{result['delimiter_used']}'")
                print(f"Date Range: {result['min_date']} to {result['max_date']}")
                print(f"Total Rows: {result['row_count']:,}")
                print(f"Unique Timestamps: {result['unique_dates']:,}")
            except Exception as e:
                print(f"Error processing {file}: {str(e)}")
                continue

if __name__ == "__main__":
    main()
