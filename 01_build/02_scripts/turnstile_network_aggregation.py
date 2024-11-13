# network_aggregator.py

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


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('network_aggregation.log'),
        logging.StreamHandler()
    ]
)

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
        self.setup_logging()
        self.setup_paths()

        # Ensure we're in the right phase
        if self.config.phase != Phase.BUILD:
            raise ValueError("NetworkAggregator should be used in BUILD phase")
            
        # Set up paths
        self.coincidences_path = Path(self.config.get_path('coincidences'))
        self.networks_path = Path(self.config.get_path('output')) / 'Networks'
        self.networks_path.mkdir(parents=True, exist_ok=True)
        
        logging.info(f"Initialized NetworkAggregator")
        logging.info(f"Reading from: {self.coincidences_path}")
        logging.info(f"Writing to: {self.networks_path}")
    
    def get_files_for_window(self, window):
        """Get files for a specific time window, respecting test mode if enabled"""
        files = list(self.coincidences_path.glob(f"coincidences_*_window{window}s.csv"))
        
        if self.test_mode:
            # Randomly sample files in test mode
            if len(files) > self.test_files:
                files = random.sample(files, self.test_files)
            self.logger.info(f"Test mode: selected {len(files)} files for window {window}s")
            
        return files
        
    def process_window(self, window, chunk_size=1000):
        """Process a single time window"""
        start_time = time.time()
        files = self.get_files_for_window(window)
        
        # Create a test-specific output directory if in test mode
        if self.test_mode:
            output_dir = self.networks_path / 'test_results'
            output_dir.mkdir(exist_ok=True)
        else:
            output_dir = self.networks_path
            
        # Split files into chunks
        n_chunks = max(1, len(files) // chunk_size)
        file_chunks = np.array_split(files, n_chunks)
        
        # Process each chunk
        for i, chunk_files in enumerate(file_chunks, 1):
            self.process_chunk_of_files(chunk_files, window, i, n_chunks)
            
        # Merge results
        final_df = self.merge_intermediate_files(window)
        
        # Save results
        output_file = output_dir / f"aggregated_network_{window}s.csv"
        final_df.to_csv(output_file, index=False)
        
        elapsed_time = time.time() - start_time
        self.logger.info(f"Window {window}s completed in {elapsed_time/3600:.2f} hours")
        self.logger.info(f"Total edges: {len(final_df)}")
        
        return {
            'edges': len(final_df),
            'total_coincidences': final_df['total_coincidences'].sum(),
            'same_turnstile': final_df['same_turnstile_coincidences'].sum()
        }

def main():
    # Add argument parsing for test mode
    import argparse
    parser = argparse.ArgumentParser(description='Aggregate network data from coincidence files')
    parser.add_argument('--test', action='store_true', help='Run in test mode')
    parser.add_argument('--test-files', type=int, default=100, help='Number of files to process in test mode')
    args = parser.parse_args()
    
    config = ProjectConfig(phase=Phase.BUILD)
    aggregator = NetworkAggregator(config, test_mode=args.test, test_files=args.test_files)
    results = aggregator.process_all_windows()
    
    # Save summary
    summary_df = pd.DataFrame.from_dict(results, orient='index')
    
    # Save to test-specific directory if in test mode
    if args.test:
        output_dir = Path(aggregator.networks_path) / 'test_results'
        output_dir.mkdir(exist_ok=True)
        summary_df.to_csv(output_dir / 'network_summary_statistics.csv')
    else:
        summary_df.to_csv(aggregator.networks_path / 'network_summary_statistics.csv')

if __name__ == "__main__":
    main()
