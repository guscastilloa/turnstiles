import pandas as pd
import numpy as np
from datetime import timedelta
import os
from collections import defaultdict
import random

def assign_time_bin(timestamp, bin_size=10):
    """Assign timestamps to bins of bin_size seconds"""
    return pd.Timestamp(timestamp).floor(f'{bin_size}S')

def find_coincidences(df, time_windows=[3,4,5,6,7]):
    """
    Find coincidences for all time windows in a single pass
    """
    # Sort by timestamp (should already be sorted, but ensure)
    df = df.sort_values('fecha_completa')
    
    # Create 10-second bins (larger than our max window of 7 seconds)
    df['time_bin'] = df['fecha_completa'].apply(assign_time_bin)
    
    # Initialize coincidences dictionary for all time windows
    coincidences = {
        window: defaultdict(lambda: {'coincidences': 0, 'same_turnstile': 0})
        for window in time_windows
    }
    
    # Process each time bin
    for bin_timestamp, bin_group in df.groupby('time_bin'):
        # Get next bin's data too
        next_bin = bin_timestamp + pd.Timedelta(seconds=10)
        next_bin_data = df[df['time_bin'] == next_bin]
        
        # Combine current and next bin for processing
        processing_data = pd.concat([bin_group, next_bin_data])
        
        # Process each record in current bin
        for idx, record in bin_group.iterrows():
            # Find potential matches in the processing window
            potential_matches = processing_data[
                (processing_data.index != idx) &  # Not the same record
                (processing_data['tipoacceso'] == record['tipoacceso']) &  # Same access type
                (processing_data['carnet'] != record['carnet'])  # Different person
            ]
            
            # Check time windows
            for window in time_windows:
                time_mask = (
                    (potential_matches['fecha_completa'] >= record['fecha_completa'] - timedelta(seconds=window/2)) &
                    (potential_matches['fecha_completa'] <= record['fecha_completa'] + timedelta(seconds=window/2))
                )
                matches = potential_matches[time_mask]
                
                # Update coincidences for this time window
                for _, match in matches.iterrows():
                    pair = tuple(sorted([str(record['carnet']), str(match['carnet'])]))
                    coincidences[window][pair]['coincidences'] += 1
                    
                    if record['torniquete'] == match['torniquete']:
                        coincidences[window][pair]['same_turnstile'] += 1
    
    # Convert results to DataFrames
    results = {}
    for window in time_windows:
        results[window] = pd.DataFrame([
            {
                'Carnet1': pair[0],
                'Carnet2': pair[1],
                'total_coincidences': data['coincidences'],
                'same_turnstile_coincidences': data['same_turnstile']
            }
            for pair, data in coincidences[window].items()
        ])
    
    return results

def process_daily_file(file_path, time_windows, output_dir):
    """Process a single daily file for all time windows"""
    date_str = os.path.basename(file_path).split('_')[1].split('.')[0]
    print(f"Processing data for date: {date_str}")
    
    # Load and prepare data
    df = pd.read_csv(file_path, delimiter=';', parse_dates=['fecha_completa'])
    df['tipoacceso'] = df['tipoacceso'].str.strip()
    df['torniquete'] = df['porteria_detalle'].apply(lambda x: x.split('-')[0])
    
    # Find coincidences for all time windows
    results = find_coincidences(df, time_windows)
    
    # Save results
    for window, result_df in results.items():
        output_file = os.path.join(
            output_dir, 
            f"coincidences_{date_str}_window{window}s.csv"
        )
        result_df.to_csv(output_file, index=False)
        print(f"Saved results for {window}s window to {output_file}")

def process_files(input_dir, output_dir, time_windows, sample_size=None, random_seed=42):
    """Process multiple files with option for sampling"""
    os.makedirs(output_dir, exist_ok=True)
    
    # Get list of files
    all_files = [f for f in sorted(os.listdir(input_dir)) 
                 if f.startswith('P2000_') and f.endswith('.csv')]
    
    print(f"Found {len(all_files)} files")
    
    # Sample files if requested
    if sample_size is not None:
        random.seed(random_seed)
        files_to_process = random.sample(all_files, min(sample_size, len(all_files)))
        print(f"Selected {len(files_to_process)} files for processing")
    else:
        files_to_process = all_files
    
    # Process each file
    for i, filename in enumerate(files_to_process, 1):
        print(f"\nProcessing file {i}/{len(files_to_process)}: {filename}")
        file_path = os.path.join(input_dir, filename)
        process_daily_file(file_path, time_windows, output_dir)

if __name__ == "__main__":
    INPUT_PATH = "/hpcfs/home/economia/ga.castillo/projects/TOR/turnstiles/01_build/03_output/Daily"
    OUTPUT_PATH = "/hpcfs/home/economia/ga.castillo/projects/TOR/turnstiles/01_build/03_output/Coincidences"

    time_windows = [3, 4, 5, 6, 7]
    
    # Process sample
    process_files(INPUT_PATH, OUTPUT_PATH, time_windows, sample_size=5)
    
    # Or process all files
    # process_files(input_dir, output_dir, time_windows)