import os
import sys
print(f"epaaa: {sys.path}")
import pandas as pd
from config import ProjectConfig, Phase
import logging


def log_ground_truth_data():
    # Initialize configuration for BUILD phase
    
    config = ProjectConfig(phase=Phase.BUILD)
    
    # Set up paths using config
    input_dir = config.get_path('input')
    output_dir = config.get_path('output')
    
    # Set up logging
    logging.basicConfig(
        filename=os.path.join(output_dir, 'pickle_files_log.txt'),
        level=logging.INFO,
        format='%(message)s'
    )
    
    # Define directory with ground truth data
    ground_truth_dir = os.path.join(input_dir, 'Interacciones Encuestas')
    
    # List of all expected files
    file_list = [
        'carnets_civil20182.pkl',
        'interacciones_economia20172.pkl',
        'carnets_economia20172.pkl',
        'interacciones_civil20182.pkl',
        'carnets_cpol20182.pkl',
        'interacciones_medicina20172.pkl',
        'carnets_medicina20172.pkl',
        'interacciones_medicina20182.pkl',
        'interacciones_cpol20182.pkl',
        'interacciones_trust20172.pkl',
        'carnets_medicina20182.pkl',
        'interacciones_civil20172.pkl',
        'interacciones_economia20182.pkl',
        'carnets_trust20172.pkl',
        'carnets_economia20182.pkl',
        'carnets_civil20172.pkl'
    ]
        
    # Log details for each file
    for file_name in file_list:
        file_path = os.path.join(ground_truth_dir, file_name)
        logging.info("\n" + "="*40)
        logging.info(f"File Name: {file_name}")
        
        try:
            data = pd.read_pickle(file_path, compression='gzip')
            logging.info(f"Shape: {data.shape}")
            logging.info(f"Columns: {data.columns.tolist()}")
            logging.info(f"Dtypes: {data.dtypes.to_dict()}")
        except Exception as e:
            logging.error(f"Error: {str(e)}")
        
        logging.info("="*40 + "\n")

if __name__ == "__main__":
    log_ground_truth_data()