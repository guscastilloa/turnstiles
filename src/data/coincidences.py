import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import os
from collections import defaultdict
import random
import multiprocessing as mp
import logging
from functools import partial
from pathlib import Path

class coincidenceProcessor:
    def __init__(self):
        """initialize processor with logging configuration"""
        # Create logs directory if it doesn't exist
        log_dir = Path('logs/tests/coincidences')
        log_dir.mkdir(parents=True, exist_ok=True)

        # Configure logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        # Create file handler with current timestamp
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = log_dir / f'coincidences_{timestamp}.log'
        file_handler = logging.FileHandler(log_file)
        
        # Create formatter 
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        
        # Add file handler to logger
        self.logger.addHandler(file_handler)
        self.logger.info(f"initialized coincidence processor with log file: {log_file}")
    
    @staticmethod
    def assign_time_bin(timestamp, bin_size=10):
        """assign timestamps to bins of bin_size seconds"""
        return pd.Timestamp(timestamp).floor(f'{bin_size}s')
    
    def find_coincidences(self, df, window):
        """
        Find coincidences for a single time window
        args:
            df: pd.DataFrame - dataframe with records
            window: int - time window in seconds
        """
        # sort by timestamp
        df = df.sort_values('fecha_completa')
        
        # create 10-second bins
        df['time_bin'] = df['fecha_completa'].apply(self.assign_time_bin)
        
        # initialize coincidences dictionary
        coincidences = defaultdict(lambda: {'coincidences': 0, 'same_turnstile': 0})
        
        # process each time bin
        for bin_timestamp, bin_group in df.groupby('time_bin'):
            # get next bin's data
            next_bin = bin_timestamp + pd.Timedelta(seconds=10)
            next_bin_data = df[df['time_bin'] == next_bin]
            
            # combine current and next bin
            processing_data = pd.concat([bin_group, next_bin_data])
            
            # process each record in current bin
            for idx, record in bin_group.iterrows():
                # only look at entries that are further along in the sorted dataframe
                # this ensures we don't double count
                potential_matches = processing_data[
                    (processing_data.index > idx) &  # this is the key change
                    (processing_data['tipoacceso'] == record['tipoacceso']) &
                    (processing_data['carnet'] != record['carnet'])
                ]
                
                # look for entries within the time window
                time_mask = (
                    (potential_matches['fecha_completa'] <= record['fecha_completa'] + timedelta(seconds=window))
                )
                matches = potential_matches[time_mask]
                
                for _, match in matches.iterrows():
                    pair = tuple(sorted([str(record['carnet']), str(match['carnet'])]))
                    coincidences[pair]['coincidences'] += 1
                    if record['torniquete'] == match['torniquete']:
                        coincidences[pair]['same_turnstile'] += 1
        
        return coincidences
    
    def process_single_file(self, file_path, window, output_dir=Path('data/intermediate/coincidences')):
        """
        Process a single file for a specific time window
        
        Args:
        ---
            file_path: str - path to file to process
            window: int - time window in seconds
            output_dir: str - path to save output file 
        """
        try:
            date_str = Path(file_path).stem.split('_')[1]
            self.logger.info(f"processing date: {date_str} for {window}s window")
            
            # load and prepare data
            print(os.getcwd())
            df = pd.read_csv(file_path, delimiter=';', parse_dates=['fecha_completa'])
            self.logger.info(f"loaded {len(df)} records for date {date_str}")
            
            df['tipoacceso'] = df['tipoacceso'].str.strip()
            df['torniquete'] = df['porteria_detalle'].apply(lambda x: x.split('-')[0])
            
            # find coincidences
            self.logger.info(f"finding coincidences for date {date_str}")
            coincidences = self.find_coincidences(df, window)
            
            # save results
            result_df = pd.DataFrame([
                {
                    'carnet1': pair[0],
                    'carnet2': pair[1],
                    'total_coincidences': vals['coincidences'],
                    'same_turnstile_coincidences': vals['same_turnstile']
                }
                for pair, vals in coincidences.items()
            ])
            
            output_dir = Path(output_dir)  
            # ensure output directory exists
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / f"coincidences_{date_str}_window{window}s.csv"
            result_df.to_csv(output_file, index=False)
            
            self.logger.info(f"Success finding coincidences for date {date_str}")
            
            return f"successfully processed {date_str} for {window}s window"
        
        except Exception as e:
            # self.logger.error(f"error processing {file_path}: {str(e)}")
            return f"error processing {file_path}: {str(e)}"
    
    def process_files_parallel(self, window, sample_size=None, random_seed=42, n_processes=None):
        """process multiple files in parallel for a specific time window"""
        # Fix I/O paths
        input_dir = Path('data/intermediate/daily') 
        output_dir = output_dir

        # ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # get list of files
        all_files = sorted(input_dir.glob('p2000_*.csv'))
        self.logger.info(f"found {len(all_files)} files to process for {window}s window")
        
        # sample files if requested
        if sample_size is not None:
            random.seed(random_seed)
            files_to_process = random.sample(all_files, min(sample_size, len(all_files)))
            self.logger.info(f"selected {len(files_to_process)} files for processing")
        else:
            files_to_process = all_files
        
        # set up parallel processing
        if n_processes is None:
            n_processes = max(1, mp.cpu_count() - 1)  # leave one cpu free
        
        self.logger.info(f"starting parallel processing with {n_processes} processes")
        
        # create partial function with fixed arguments
        process_func = partial(self.process_single_file, window=window)
        
        # process files in parallel
        with mp.pool(processes=n_processes) as pool:
            results = pool.map(process_func, files_to_process)
        
        # log results
        for result in results:
            self.logger.info(result)
            
        self.logger.info(f"completed processing for {window}s window")

