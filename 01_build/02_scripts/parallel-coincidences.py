import pandas as pd
import numpy as np
from datetime import timedelta
import os
from collections import defaultdict
import random
import multiprocessing as mp
from functools import partial

def assign_time_bin(timestamp, bin_size=10):
    """Assign timestamps to bins of bin_size seconds"""
    return pd.Timestamp(timestamp).floor(f'{bin_size}S')

def find_coincidences(df, time_windows=[3,4,5,6,7]):
    """Find coincidences for all time windows in a single pass"""
    # Sort by timestamp
    df = df.sort_values('fecha_completa')
    
    # Create 10-second bins
    df['time_bin'] = df['fecha_completa'].apply(assign_time_bin)
    
    # Initialize coincidences dictionary
    coincidences = {
        window: defaultdict(lambda: {'coincidences': 0, 'same_turnstile': 0})
        for window in time_windows
    }
    
    # Process each time bin
    for bin_timestamp, bin_group in df.groupby('time_bin'):
        # Get next bin's data
        next_bin = bin_timestamp + pd.Timedelta(seconds=10)
        next_bin_data = df[df['time_bin'] == next_bin]
        
        # Combine current and next bin
        processing_data = pd.concat([bin_group, next_bin_data])
        
        # Process each record in current bin
        for idx, record in bin_group.iterrows():
            potential_matches = processing_data[
                (processing_data.index != idx) &
                (processing_data['tipoacceso'] == record['tipoacceso']) &
                (processing_data['carnet'] != record['carnet'])
            ]
            
            for window in time_windows:
                time_mask = (
                    (potential_matches['fecha_completa'] >= record['fecha_completa'] - timedelta(seconds=window/2)) &
                    (potential_matches['fecha_completa'] <= record['fecha_completa'] + timedelta(seconds=window/2))
                )
                matches = potential_matches[time_mask]
                
                for _, match in matches.iterrows():
                    pair = tuple(sorted([str(record['carnet']), str(match['carnet'])]))
                    coincidences[window][pair]['coincidences'] += 1
                    if record['torniquete'] == match['torniquete']:
                        coincidences[window][pair]['same_turnstile'] += 1
    
    return coincidences

def process_single_file(file_path, output_dir, time_windows):
    """Process a single file - this will be our parallel worker function"""
    try:
        date_str = os.path.basename(file_path).split('_')[1].split('.')[0]
        print(f"Processing date: {date_str}")
        
        # Load and prepare data
        df = pd.read_csv(file_path, delimiter=';', parse_dates=['fecha_completa'])
        print(f"Loaded {len(df)} records of date {date_str}")
        df['tipoacceso'] = df['tipoacceso'].str.strip()
        df['torniquete'] = df['porteria_detalle'].apply(lambda x: x.split('-')[0])
        
        # Find coincidences
        print("Finding coincidences for date {date_str}")
        coincidences = find_coincidences(df, time_windows)
        
        # Save results for each time window
        for window, data in coincidences.items():
            result_df = pd.DataFrame([
                {
                    'Carnet1': pair[0],
                    'Carnet2': pair[1],
                    'total_coincidences': vals['coincidences'],
                    'same_turnstile_coincidences': vals['same_turnstile']
                }
                for pair, vals in data.items()
            ])
            
            output_file = os.path.join(
                output_dir,
                f"coincidences_{date_str}_window{window}s.csv"
            )
            result_df.to_csv(output_file, index=False)
        
        return f"Successfully processed {date_str}"
    
    except Exception as e:
        return f"Error processing {file_path}: {str(e)}"

def process_files_parallel(input_dir, output_dir, time_windows, sample_size=None, random_seed=42, n_processes=None):
    """Process multiple files in parallel"""
    # Ensure output directory exists
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
    
    # Create full file paths
    file_paths = [os.path.join(input_dir, f) for f in files_to_process]
    
    # Set up parallel processing
    if n_processes is None:
        n_processes = mp.cpu_count() - 1  # Leave one CPU free
    
    print(f"Starting parallel processing with {n_processes} processes")
    
    # Create partial function with fixed arguments
    process_func = partial(process_single_file, output_dir=output_dir, time_windows=time_windows)
    
    # Process files in parallel
    with mp.Pool(processes=n_processes) as pool:
        results = pool.map(process_func, file_paths)
    
    # Print results
    for result in results:
        print(result)

if __name__ == "__main__":
    # Configuration
    input_dir = "/hpcfs/home/economia/ga.castillo/projects/TOR/turnstiles/01_build/03_output/Daily"
    output_dir = "/hpcfs/home/economia/ga.castillo/projects/TOR/turnstiles/01_build/03_output/Coincidences"
    time_windows = [3, 4, 5, 6, 7]
    
    # For testing with 2 files and 3 processes
    # process_files_parallel(
    #     input_dir=input_dir,
    #     output_dir=output_dir,
    #     time_windows=time_windows,
    #     sample_size=2,
    #     n_processes=3
    # )
    
    # For processing all files, uncomment this:
    process_files_parallel(
        input_dir=input_dir,
        output_dir=output_dir,
        time_windows=time_windows,
        n_processes=None  # Adjust based on your node's capacity
    )
