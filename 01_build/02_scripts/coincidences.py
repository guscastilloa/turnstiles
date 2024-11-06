import pandas as pd
import os
from datetime import timedelta
from collections import defaultdict
import re
import random

def process_daily_file(file_path, time_window_seconds, output_dir):
    # Extract date from filename (assuming filename format is 'P2000_YYYYMMDD.csv')
    date_str = os.path.basename(file_path).split('_')[1].split('.')[0]
    print(f"Processing data for date: {date_str}")
    
    # Load the file into a DataFrame
    df = pd.read_csv(file_path, delimiter=';', parse_dates=['fecha_completa'])
    print(f"Loaded {len(df)} records")
    
    # Clean up tipoacceso by stripping whitespace
    df['tipoacceso'] = df['tipoacceso'].str.strip()
    
    # Create 'torniquete' field by extracting the base identifier
    df['torniquete'] = df['porteria_detalle'].apply(lambda x: re.split('[-\s]', x)[0])
    print("Created torniquete column")
    
    # Get unique individuals
    unique_individuals = df['carnet'].unique()
    print(f"Found {len(unique_individuals)} unique individuals")
    
    # Initialize dictionary to store coincidences
    coincidences = defaultdict(lambda: {'coincidences': 0, 'same_turnstile': 0})
    
    # Loop over unique individuals
    total_individuals = len(unique_individuals)
    for idx, individual in enumerate(unique_individuals):
        if idx % 100 == 0:  # Progress update every 100 individuals
            print(f"Processing individual {idx + 1}/{total_individuals}")
            
        # Get all occurrences of the individual
        occurrences = df[df['carnet'] == individual]
        
        for _, occurrence in occurrences.iterrows():
            timestamp = occurrence['fecha_completa']
            access_type = occurrence['tipoacceso']  # Get the access type (IN/OUT)
            # Define the time window
            window_seconds = timedelta(seconds=time_window_seconds)
            start_time = timestamp - window_seconds/2
            end_time = timestamp + window_seconds/2
            
            # Find all entries within the time window for other individuals
            window_entries = df[
                (df['fecha_completa'] >= start_time) & 
                (df['fecha_completa'] <= end_time) & 
                (df['carnet'] != individual) &  # Exclude the current individual
                (df['tipoacceso'] == access_type)  # Same access type (IN-IN or OUT-OUT)
            ]
            
            # Update coincidences
            for _, other_entry in window_entries.iterrows():
                # Create sorted tuple of carnets to avoid duplicates
                pair = tuple(sorted([str(individual), str(other_entry['carnet'])]))
                
                # Update coincidences counters
                coincidences[pair]['coincidences'] += 1
                
                # Check if same turnstile (using the extracted base identifier)
                if occurrence['torniquete'] == other_entry['torniquete']:
                    coincidences[pair]['same_turnstile'] += 1
    
    # Create the result DataFrame
    result_df = pd.DataFrame([
        {
            'Carnet1': pair[0],
            'Carnet2': pair[1],
            'total_coincidences': data['coincidences'],
            'same_turnstile_coincidences': data['same_turnstile']
        }
        for pair, data in coincidences.items()
    ])
    
    # Save the result with time window in filename
    output_file = os.path.join(
        output_dir, 
        f"coincidences_{date_str}_window{time_window_seconds}s.csv"
    )
    result_df.to_csv(output_file, index=False)
    print(f"Saved results to {output_file}")
    
    return result_df

def process_all_daily_files(input_dir, output_dir, time_windows, sample_size=None, random_seed=42):
    """
    Process daily files with multiple time windows.
    
    Args:
        input_dir: Directory containing daily CSV files
        output_dir: Directory to save results
        time_windows: List of time windows in seconds to process
        sample_size: Number of files to process (None for all files)
        random_seed: Random seed for reproducible sampling
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Get list of all valid files
    all_files = [f for f in sorted(os.listdir(input_dir)) 
                 if f.startswith('P2000_') and f.endswith('.csv')]
    
    total_files = len(all_files)
    print(f"Found {total_files} files to process")
    
    # Sample files if requested
    if sample_size is not None:
        random.seed(random_seed)
        files_to_process = random.sample(all_files, min(sample_size, total_files))
        print(f"Randomly selected {len(files_to_process)} files to process")
    else:
        files_to_process = all_files
        print("Processing all files")
    
    # Process each selected file
    for i, filename in enumerate(files_to_process, 1):
        print(f"\nProcessing file {i}/{len(files_to_process)}: {filename}")
        file_path = os.path.join(input_dir, filename)
        
        # Process each time window
        for window in time_windows:
            print(f"\nProcessing with {window}-second window...")
            process_daily_file(file_path, window, output_dir)

if __name__ == "__main__":
    # Configuration
    input_dir = "/hpcfs/home/economia/ga.castillo/projects/TOR/turnstiles/01_build/03_output/Daily"
    output_dir = "/hpcfs/home/economia/ga.castillo/projects/TOR/turnstiles/01_build/03_output/Coincidences"
    time_windows = [3, 4, 5, 6, 7]  # Time windows in seconds to test
    
    # Choose one of these options:
    
    # Option 1: Process all files
    # process_all_daily_files(input_dir, output_dir, time_windows)
    
    # Option 2: Process a random sample of files
    # Example: Process 5 random files
    process_all_daily_files(input_dir, output_dir, time_windows, sample_size=5)
