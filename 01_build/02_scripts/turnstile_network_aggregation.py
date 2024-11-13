# network_aggregator.py

from pathlib import Path
import pandas as pd
from collections import defaultdict
import logging
from config import ProjectConfig, Phase
import os

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
    def __init__(self, config: ProjectConfig):
        """
        Initialize the NetworkAggregator with project configuration
        
        Parameters:
        -----------
        config : ProjectConfig
            Project configuration object that manages paths
        """
        self.config = config
        
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
        """Get all coincidence files for a specific time window"""
        pattern = f"coincidences_*_window{window}s.csv"
        files = list(self.coincidences_path.glob(pattern))
        logging.info(f"Found {len(files)} files for {window}s window")
        return files
    
    def aggregate_window(self, window, chunk_size=1000):
        """
        Aggregate all files for a specific time window
        Uses chunking to handle large datasets efficiently
        """
        files = self.get_files_for_window(window)
        
        # Initialize aggregation dictionaries
        edge_weights = defaultdict(lambda: {'total': 0, 'same_turnstile': 0})
        processed_files = 0
        total_rows = 0
        
        for file in files:
            try:
                # Process file in chunks to manage memory
                for chunk in pd.read_csv(file, chunksize=chunk_size):
                    for _, row in chunk.iterrows():
                        # Create sorted tuple as unique edge identifier
                        edge = tuple(sorted([str(row['Carnet1']), str(row['Carnet2'])]))
                        edge_weights[edge]['total'] += row['total_coincidences']
                        edge_weights[edge]['same_turnstile'] += row['same_turnstile_coincidences']
                        total_rows += 1
                
                processed_files += 1
                if processed_files % 100 == 0:
                    logging.info(f"Processed {processed_files}/{len(files)} files for {window}s window")
                    
            except Exception as e:
                logging.error(f"Error processing file {file}: {str(e)}")
                continue
        
        # Convert to DataFrame
        edges_df = pd.DataFrame([
            {
                'Carnet1': edge[0],
                'Carnet2': edge[1],
                'total_coincidences': data['total'],
                'same_turnstile_coincidences': data['same_turnstile']
            }
            for edge, data in edge_weights.items()
        ])
        
        logging.info(f"Window {window}s aggregation complete:")
        logging.info(f"Total edges: {len(edges_df)}")
        logging.info(f"Total coincidences: {edges_df['total_coincidences'].sum()}")
        
        return edges_df
    
    def process_all_windows(self, windows=[3, 4, 5, 6, 7]):
        """Process all time windows and save results"""
        results = {}
        
        for window in windows:
            logging.info(f"\nProcessing {window}s window...")
            try:
                network_df = self.aggregate_window(window)
                
                # Save aggregated network
                output_file = self.networks_path / f"aggregated_network_{window}s.csv"
                network_df.to_csv(output_file, index=False)
                
                results[window] = {
                    'edges': len(network_df),
                    'total_coincidences': network_df['total_coincidences'].sum(),
                    'same_turnstile': network_df['same_turnstile_coincidences'].sum()
                }
                
                logging.info(f"Saved network to {output_file}")
                
            except Exception as e:
                logging.error(f"Error processing {window}s window: {str(e)}")
                continue
        
        # Save summary statistics
        summary_df = pd.DataFrame.from_dict(results, orient='index')
        summary_df.to_csv(self.networks_path / 'network_summary_statistics.csv')
        
        logging.info("\nAggregation complete!")
        return results

def main():
    """Main execution function"""
    # Initialize configuration for build phase
    config = ProjectConfig(phase=Phase.BUILD)
    
    # Create aggregator
    aggregator = NetworkAggregator(config)
    
    # Process all windows
    results = aggregator.process_all_windows()
    
    # Print final summary
    print("\nFinal Summary:")
    for window, stats in results.items():
        print(f"\n{window}s window:")
        print(f"  Edges: {stats['edges']:,}")
        print(f"  Total coincidences: {stats['total_coincidences']:,}")
        print(f"  Same turnstile coincidences: {stats['same_turnstile']:,}")

if __name__ == "__main__":
    main()
