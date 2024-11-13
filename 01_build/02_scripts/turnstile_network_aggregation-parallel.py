import pandas as pd
import numpy as np
from pathlib import Path
import logging
from config import ProjectConfig, Phase
import os
from tqdm import tqdm
import gc
import psutil
import time
import random
import argparse
from multiprocessing import Pool, cpu_count
from functools import partial

class NetworkAggregator:
    def __init__(self, config: ProjectConfig, test_mode=False, test_files=100):
        """
        Initialize NetworkAggregator
        
        Parameters:
        -----------
        config : ProjectConfig
            Project configuration object
        test_mode : bool
            If True, runs on a limited number of files for testing
        test_files : int
            Number of files to process in test mode
        """
        self.config = config
        self.test_mode = test_mode
        self.test_files = test_files
        
        # Set up paths first so logging directory exists
        self.setup_paths()
        self.setup_logging()
        
        if self.test_mode:
            self.logger.info(f"Running in TEST MODE with {test_files} files per window")
    
    def setup_paths(self):
        """Set up necessary paths for input, output, and temporary files"""
        if self.config.phase != Phase.BUILD:
            raise ValueError("NetworkAggregator should be used in BUILD phase")
        
        # Set up main paths
        self.coincidences_path = Path(self.config.get_path('coincidences'))
        self.networks_path = Path(self.config.get_path('output')) / 'Networks'
        self.temp_path = Path(self.config.get_path('temp')) / 'network_aggregation'
        self.log_path = Path(self.config.get_path('temp')) / 'logs'
        
        # Create directories
        for path in [self.networks_path, self.temp_path, self.log_path]:
            path.mkdir(parents=True, exist_ok=True)
    
    def setup_logging(self):
        """Set up logging configuration"""
        self.logger = logging.getLogger('NetworkAggregator')
        self.logger.setLevel(logging.INFO)
        
        # Clear any existing handlers
        self.logger.handlers = []
        
        # Create handlers
        log_file = self.log_path / 'network_aggregation.log'
        handlers = [
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        # Add formatter to handlers
        for handler in handlers:
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            
        self.logger.info("Logging initialized")
    
    def log_memory_usage(self):
        """Log current memory usage"""
        process = psutil.Process()
        mem_usage = process.memory_info().rss / 1024 / 1024  # in MB
        self.logger.info(f"Current memory usage: {mem_usage:.2f} MB")
    
    def process_file(self, file_path):
        """Process a single coincidence file with error handling"""
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                self.logger.warning(f"Empty file: {file_path}")
                return None
            return df
        except pd.errors.EmptyDataError:
            self.logger.warning(f"Empty file: {file_path}")
            return None
        except Exception as e:
            self.logger.error(f"Error processing {file_path}: {str(e)}")
            return None
            
    def process_window(self, window):
        """Process a single time window"""
        start_time = time.time()
        files = self.get_files_for_window(window)
        
        # Create a test-specific output directory if in test mode
        if self.test_mode:
            output_dir = self.networks_path / 'test_results'
            output_dir.mkdir(exist_ok=True)
        else:
            output_dir = self.networks_path
        
        # Process files and aggregate results
        edge_weights = {}
        successful_files = 0
        
        for file in tqdm(files, desc=f"Window {window}s"):
            df = self.process_file(file)
            if df is not None:
                for _, row in df.iterrows():
                    edge = tuple(sorted([str(row['Carnet1']), str(row['Carnet2'])]))
                    if edge not in edge_weights:
                        edge_weights[edge] = {'total': 0, 'same_turnstile': 0}
                    edge_weights[edge]['total'] += row['total_coincidences']
                    edge_weights[edge]['same_turnstile'] += row['same_turnstile_coincidences']
                successful_files += 1
                
            if successful_files % 100 == 0:
                self.logger.info(f"Processed {successful_files} successful files for window {window}s")
                self.log_memory_usage()
        
        # Create final DataFrame
        final_df = pd.DataFrame([
            {
                'Carnet1': edge[0],
                'Carnet2': edge[1],
                'total_coincidences': data['total'],
                'same_turnstile_coincidences': data['same_turnstile']
            }
            for edge, data in edge_weights.items()
        ])
        
        # Save results
        output_file = output_dir / f"aggregated_network_{window}s.csv"
        final_df.to_csv(output_file, index=False)
        
        elapsed_time = time.time() - start_time
        self.logger.info(f"Window {window}s completed in {elapsed_time/3600:.2f} hours")
        
        return {
            'window': window,
            'edges': len(final_df),
            'total_coincidences': final_df['total_coincidences'].sum(),
            'same_turnstile': final_df['same_turnstile_coincidences'].sum(),
            'processing_time': elapsed_time,
            'successful_files': successful_files
        }
    
    def get_files_for_window(self, window):
        """Get files for a specific time window, respecting test mode if enabled"""
        files = list(self.coincidences_path.glob(f"coincidences_*_window{window}s.csv"))
        
        if self.test_mode:
            # Randomly sample files in test mode
            if len(files) > self.test_files:
                files = random.sample(files, self.test_files)
            self.logger.info(f"Test mode: selected {len(files)} files for window {window}s")
            
        return files
    
    def process_all_windows_parallel(self, windows=[3, 4, 5, 6, 7]):
        """Process all time windows in parallel"""
        n_processes = min(len(windows), cpu_count() - 1)  # Leave one CPU free
        self.logger.info(f"Starting parallel processing with {n_processes} processes")
        
        start_time = time.time()
        
        with Pool(processes=n_processes) as pool:
            results = pool.map(self.process_window, windows)
            
        # Convert results to DataFrame
        results_df = pd.DataFrame(results).set_index('window')
        
        # Save summary statistics
        if self.test_mode:
            output_dir = self.networks_path / 'test_results'
            output_dir.mkdir(exist_ok=True)
            results_df.to_csv(output_dir / 'network_summary_statistics.csv')
        else:
            results_df.to_csv(self.networks_path / 'network_summary_statistics.csv')
        
        total_time = time.time() - start_time
        self.logger.info(f"Total processing time: {total_time/3600:.2f} hours")
        
        return results_df

def main():
    # Add argument parsing for test mode
    parser = argparse.ArgumentParser(description='Aggregate network data from coincidence files')
    parser.add_argument('--test', action='store_true', help='Run in test mode')
    parser.add_argument('--test-files', type=int, default=100, help='Number of files to process in test mode')
    args = parser.parse_args()
    
    # Initialize configuration and aggregator
    config = ProjectConfig(phase=Phase.BUILD)
    aggregator = NetworkAggregator(config, test_mode=args.test, test_files=args.test_files)
    
    # Process all windows in parallel
    results = aggregator.process_all_windows_parallel()
    
    # Print final summary
    print("\nProcessing Summary:")
    print(results)
    
if __name__ == "__main__":
    main()