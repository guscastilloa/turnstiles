import csv
from datetime import datetime
import os
from io import StringIO
import gzip
from contextlib import contextmanager
import logging
from typing import Dict, TextIO
import gc

INPUT_PATH = '/hpcfs/home/economia/ga.castillo/projects/TOR/turnstiles/01_build/03_output/Accesos_P2000_sorted.csv'
OUTPUT_PATH = "/hpcfs/home/economia/ga.castillo/projects/TOR/turnstiles/01_build/03_output/Daily"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('split_process.log'),
        logging.StreamHandler()
    ]
)

class DailyFileManager:
    def __init__(self, output_dir: str, fieldnames: list, max_open_files: int = 50):
        self.output_dir = output_dir
        self.fieldnames = fieldnames
        self.max_open_files = max_open_files
        self.open_files: Dict[str, TextIO] = {}
        self.writers: Dict[str, csv.DictWriter] = {}
        
    def get_writer(self, date_str: str) -> csv.DictWriter:
        if date_str not in self.writers:
            # If we've hit the max open files, close the oldest one
            if len(self.open_files) >= self.max_open_files:
                oldest_date = next(iter(self.open_files))
                self.close_file(oldest_date)
            
            filename = os.path.join(self.output_dir, f"P2000_{date_str}.csv")
            file_exists = os.path.exists(filename)
            
            self.open_files[date_str] = open(filename, 'a', newline='', encoding='utf-8')
            self.writers[date_str] = csv.DictWriter(self.open_files[date_str], 
                                                   fieldnames=self.fieldnames, 
                                                   delimiter=';')
            
            if not file_exists:
                self.writers[date_str].writeheader()
        
        return self.writers[date_str]
    
    def close_file(self, date_str: str):
        if date_str in self.open_files:
            self.open_files[date_str].close()
            del self.open_files[date_str]
            del self.writers[date_str]
    
    def close_all(self):
        for date_str in list(self.open_files.keys()):
            self.close_file(date_str)

def split_csv_by_day(input_file: str, output_dir: str, buffer_size: int = 8192, 
                    max_lines: int = 0, chunk_size: int = 100000):
    """
    Split a large CSV file into daily files with optimized memory usage.
    
    Args:
        input_file: Path to input CSV file
        output_dir: Directory for output files
        buffer_size: Size of read buffer in bytes
        max_lines: Maximum lines to process (0 for all)
        chunk_size: Number of rows to process before garbage collection
    """
    row_count = 0
    chunk_count = 0
    
    try:
        with open(input_file, 'r', newline='', encoding='utf-8', buffering=buffer_size) as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            file_manager = DailyFileManager(output_dir, reader.fieldnames)
            
            for row in reader:
                row_count += 1
                
                if max_lines > 0 and row_count > max_lines:
                    break
                
                # Extract date and get corresponding writer
                date_str = datetime.strptime(
                    row['fecha_completa'].split()[0], 
                    "%Y.%m.%d"
                ).strftime('%Y%m%d')
                
                writer = file_manager.get_writer(date_str)
                writer.writerow(row)
                
                # Periodic logging and garbage collection
                if row_count % chunk_size == 0:
                    chunk_count += 1
                    logging.info(f"Processed {row_count:,} rows ({chunk_count} chunks)")
                    gc.collect()  # Force garbage collection
                
            file_manager.close_all()
            
    except Exception as e:
        logging.error(f"Error processing file: {str(e)}")
        raise
    finally:
        logging.info(f"Total rows processed: {row_count:,}")
        logging.info("File splitting completed.")

if __name__ == "__main__":    
    # Ensure output directory exists
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    
    # Run the splitting process with optimizations
    split_csv_by_day(
        INPUT_PATH,
        OUTPUT_PATH,
        buffer_size=8192 * 16,  # 128KB buffer
        chunk_size=100000  # Process 100K rows before GC
    )