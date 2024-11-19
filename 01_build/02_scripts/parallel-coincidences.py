import pandas as pd
import numpy as np
from datetime import timedelta
import os
from collections import defaultdict
import random
import multiprocessing as mp
from functools import partial
import logging
from pathlib import Path
from config import ProjectConfig, Phase
import argparse

class CoincidenceProcessor:
    def __init__(self, config: ProjectConfig):
        """Initialize processor with configuration"""
        self.config = config
        self.setup_logging()
        
    def setup_logging(self):
        """Set up logging configuration"""
        self.logger = logging.getLogger('CoincidenceProcessor')
        self.logger.setLevel(logging.INFO)
        
        # Clear any existing handlers
        self.logger.handlers = []
        
        # Create log directory if it doesn't exist
        log_dir = Path(self.config.get_path('temp')) / 'logs'
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create handlers
        handlers = [
            logging.FileHandler(log_dir / 'coincidence_processing.log'),
            logging.StreamHandler()
        ]
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        # Add formatter to handlers
        for handler in handlers:
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    @staticmethod
    def assign_time_bin(timestamp, bin_size=10):
        """Assign timestamps to bins of bin_size seconds"""
        return pd.Timestamp(timestamp).floor(f'{bin_size}S')
    
    def find_coincidences(self, df, window):
        """Find coincidences for a single time window"""
        # Sort by timestamp
        df = df.sort_values('fecha_completa')
        
        # Create 10-second bins
        df['time_bin'] = df['fecha_completa'].apply(self.assign_time_bin)
        
        # Initialize coincidences dictionary
        coincidences = defaultdict(lambda: {'coincidences': 0, 'same_turnstile': 0})
        
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
                
                time_mask = (
                    (potential_matches['fecha_completa'] >= record['fecha_completa'] - timedelta(seconds=window/2)) &
                    (potential_matches['fecha_completa'] <= record['fecha_completa'] + timedelta(seconds=window/2))
                )
                matches = potential_matches[time_mask]
                
                for _, match in matches.iterrows():
                    pair = tuple(sorted([str(record['carnet']), str(match['carnet'])]))
                    coincidences[pair]['coincidences'] += 1
                    if record['torniquete'] == match['torniquete']:
                        coincidences[pair]['same_turnstile'] += 1
        
        return coincidences
    
    def process_single_file(self, file_path, window):
        """Process a single file for a specific time window"""
        try:
            date_str = Path(file_path).stem.split('_')[1]
            self.logger.info(f"Processing date: {date_str} for {window}s window")
            
            # Load and prepare data
            df = pd.read_csv(file_path, delimiter=';', parse_dates=['fecha_completa'])
            self.logger.info(f"Loaded {len(df)} records for date {date_str}")
            
            df['tipoacceso'] = df['tipoacceso'].str.strip()
            df['torniquete'] = df['porteria_detalle'].apply(lambda x: x.split('-')[0])
            
            # Find coincidences
            self.logger.info(f"Finding coincidences for date {date_str}")
            coincidences = self.find_coincidences(df, window)
            
            # Save results
            result_df = pd.DataFrame([
                {
                    'Carnet1': pair[0],
                    'Carnet2': pair[1],
                    'total_coincidences': vals['coincidences'],
                    'same_turnstile_coincidences': vals['same_turnstile']
                }
                for pair, vals in coincidences.items()
            ])
            
            output_dir = Path(self.config.get_path('coincidences'))
            output_file = output_dir / f"coincidences_{date_str}_window{window}s.csv"
            result_df.to_csv(output_file, index=False)
            
            return f"Successfully processed {date_str} for {window}s window"
        
        except Exception as e:
            self.logger.error(f"Error processing {file_path}: {str(e)}")
            return f"Error processing {file_path}: {str(e)}"
    
    def process_files_parallel(self, window, sample_size=None, random_seed=42, n_processes=None):
        """Process multiple files in parallel for a specific time window"""
        # Get input and output directories from config
        input_dir = Path(self.config.get_path('daily'))
        output_dir = Path(self.config.get_path('coincidences'))
        
        # Ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Get list of files
        all_files = sorted(input_dir.glob('P2000_*.csv'))
        self.logger.info(f"Found {len(all_files)} files to process for {window}s window")
        
        # Sample files if requested
        if sample_size is not None:
            random.seed(random_seed)
            files_to_process = random.sample(all_files, min(sample_size, len(all_files)))
            self.logger.info(f"Selected {len(files_to_process)} files for processing")
        else:
            files_to_process = all_files
        
        # Set up parallel processing
        if n_processes is None:
            n_processes = max(1, mp.cpu_count() - 1)  # Leave one CPU free
        
        self.logger.info(f"Starting parallel processing with {n_processes} processes")
        
        # Create partial function with fixed arguments
        process_func = partial(self.process_single_file, window=window)
        
        # Process files in parallel
        with mp.Pool(processes=n_processes) as pool:
            results = pool.map(process_func, files_to_process)
        
        # Log results
        for result in results:
            self.logger.info(result)
            
        self.logger.info(f"Completed processing for {window}s window")

def main():
    parser = argparse.ArgumentParser(description='Process turnstile coincidences in parallel')
    parser.add_argument('--window', type=int, choices=[2,3,4,5,6,7], required=True,
                      help='Time window (in seconds) to process')
    parser.add_argument('--test', action='store_true', 
                      help='Run in test mode with sample data')
    parser.add_argument('--sample-size', type=int, default=2,
                      help='Number of files to process in test mode')
    parser.add_argument('--processes', type=int,
                      help='Number of processes to use (default: CPU count - 1)')
    args = parser.parse_args()
    
    # Initialize configuration
    config = ProjectConfig(phase=Phase.BUILD)
    processor = CoincidenceProcessor(config)
    
    # Process files for specified window
    processor.process_files_parallel(
        window=args.window,
        sample_size=args.sample_size if args.test else None,
        n_processes=args.processes
    )

if __name__ == "__main__":
    main()