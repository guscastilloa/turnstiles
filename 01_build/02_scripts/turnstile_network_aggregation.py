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
    
    def get_files_for_window(self, window):
        """Get files for a specific time window, respecting test mode if enabled"""
        files = list(self.coincidences_path.glob(f"coincidences_*_window{window}s.csv"))
        
        if self.test_mode:
            # Randomly sample files in test mode
            if len(files) > self.test_files:
                files = random.sample(files, self.test_files)
            self.logger.info(f"Test mode: selected {len(files)} files for window {window}s")
            
        return files
    
    def process_chunk_of_files(self, files, window, chunk_idx, n_chunks):
        """Process a subset of files and save intermediate results"""
        edge_weights = {}
        files_processed = 0
        
        self.logger.info(f"Processing chunk {chunk_idx}/{n_chunks} with {len(files)} files")
        self.log_memory_usage()
        
        for file in tqdm(files, desc=f"Chunk {chunk_idx}"):
            try:
                df = pd.read_csv(file)
                for _, row in df.iterrows():
                    edge = tuple(sorted([str(row['Carnet1']), str(row['Carnet2'])]))
                    if edge not in edge_weights:
                        edge_weights[edge] = {'total': 0, 'same_turnstile': 0}
                    edge_weights[edge]['total'] += row['total_coincidences']
                    edge_weights[edge]['same_turnstile'] += row['same_turnstile_coincidences']
                
                files_processed += 1
                if files_processed % 100 == 0:
                    self.log_memory_usage()
                    
            except Exception as e:
                self.logger.error(f"Error processing {file}: {str(e)}")
                continue
        
        # Save intermediate results
        temp_file = self.temp_path / f"intermediate_{window}s_chunk_{chunk_idx}.parquet"
        pd.DataFrame([
            {
                'Carnet1': edge[0],
                'Carnet2': edge[1],
                'total_coincidences': data['total'],
                'same_turnstile_coincidences': data['same_turnstile']
            }
            for edge, data in edge_weights.items()
        ]).to_parquet(temp_file)
        
        # Clear memory
        del edge_weights
        gc.collect()
    
    def merge_intermediate_files(self, window):
        """Merge all intermediate files for a window"""
        intermediate_files = list(self.temp_path.glob(f"intermediate_{window}s_chunk_*.parquet"))
        
        # Read and aggregate all intermediate files
        total_df = None
        for file in tqdm(intermediate_files, desc="Merging chunks"):
            chunk_df = pd.read_parquet(file)
            if total_df is None:
                total_df = chunk_df
            else:
                # Merge and aggregate
                total_df = pd.concat([total_df, chunk_df]).groupby(['Carnet1', 'Carnet2']).sum().reset_index()
            
            # Remove intermediate file
            file.unlink()
            
        return total_df
    
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
    
    def process_all_windows(self, windows=[3, 4, 5, 6, 7]):
        """Process all time windows"""
        results = {}
        for window in windows:
            try:
                self.logger.info(f"\nProcessing {window}s window...")
                results[window] = self.process_window(window)
            except Exception as e:
                self.logger.error(f"Error processing {window}s window: {str(e)}")
                continue
                
        return results

def main():
    # Add argument parsing for test mode
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
```