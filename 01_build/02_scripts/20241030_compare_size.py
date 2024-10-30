import csv
import os

def analyze_csv_differences(original_path, sorted_path):
    # Initialize counters
    original_rows = 0
    sorted_rows = 0
    original_bytes = 0
    sorted_bytes = 0
    
    # Analyze original file
    with open(original_path, 'rb') as f:
        # Read first line to examine line endings
        first_line = f.readline()
        original_line_ending = 'CRLF' if first_line.endswith(b'\r\n') else 'LF' if first_line.endswith(b'\n') else 'CR'
        
        # Reset file pointer
        f.seek(0)
        
        reader = csv.reader((line.decode('utf-8') for line in f), delimiter=';')
        headers_original = next(reader)
        
        for row in reader:
            original_rows += 1
            original_bytes += sum(len(field.encode('utf-8')) for field in row)
            
            # Sample some rows for white space analysis
            if original_rows <= 5:
                print(f"Original Row {original_rows} lengths: {[len(field) for field in row]}")

    # Analyze sorted file
    with open(sorted_path, 'rb') as f:
        # Read first line to examine line endings
        first_line = f.readline()
        sorted_line_ending = 'CRLF' if first_line.endswith(b'\r\n') else 'LF' if first_line.endswith(b'\n') else 'CR'
        
        # Reset file pointer
        f.seek(0)
        
        reader = csv.reader((line.decode('utf-8') for line in f), delimiter=';')
        headers_sorted = next(reader)
        
        for row in reader:
            sorted_rows += 1
            sorted_bytes += sum(len(field.encode('utf-8')) for field in row)
            
            # Sample some rows for white space analysis
            if sorted_rows <= 5:
                print(f"Sorted Row {sorted_rows} lengths: {[len(field) for field in row]}")

    return {
        'original_rows': original_rows,
        'sorted_rows': sorted_rows,
        'original_bytes': original_bytes,
        'sorted_bytes': sorted_bytes,
        'original_line_ending': original_line_ending,
        'sorted_line_ending': sorted_line_ending,
        'headers_match': headers_original == headers_sorted,
        'header_length_diff': sum(len(h) for h in headers_sorted) - sum(len(h) for h in headers_original)
    }

# Paths to your files
original_path = "/hpcfs/home/economia/ga.castillo/projects/TOR/data/P2000/Accesos_P2000.csv"
sorted_path = "/hpcfs/home/economia/ga.castillo/projects/TOR/turnstiles/01_build/03_output/Accesos_P2000_sorted.csv"

results = analyze_csv_differences(original_path, sorted_path)

print("\nAnalysis Results:")
print(f"Row counts: Original={results['original_rows']}, Sorted={results['sorted_rows']}")
print(f"Line endings: Original={results['original_line_ending']}, Sorted={results['sorted_line_ending']}")
print(f"Headers match: {results['headers_match']}")
print(f"Header length difference: {results['header_length_diff']} bytes")
print(f"Byte difference: {results['sorted_bytes'] - results['original_bytes']} bytes")
