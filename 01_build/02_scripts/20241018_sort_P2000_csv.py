import csv
from datetime import datetime
import heapq
import os
import tempfile
import logging
import sys

# Update paths for input and output
INPUT_PATH = "/hpcfs/home/economia/ga.castillo/projects/TOR/data/P2000/Accesos_P2000.csv"
OUTPUT_PATH = "/hpcfs/home/economia/ga.castillo/projects/TOR/turnstiles/01_build/03_output/Accesos_P2000_sorted.csv"
LOG_PATH = "/hpcfs/home/economia/ga.castillo/projects/TOR/turnstiles/01_build/04_temp/sort_log.txt"

# Ensure output directory exists
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

logging.basicConfig(filename=LOG_PATH, level=logging.INFO, format='%(asctime)s - %(message)s')

CHUNK_SIZE = 1000000  # 1 million rows per chunk
MAX_CHUNKS = 0  # 0 means process all chunks

def sort_chunk(chunk):
    return sorted(chunk, key=lambda x: datetime.strptime(x['fecha_completa'].strip(), "%Y.%m.%d %H:%M:%S"))

def write_chunk(chunk, file):
    writer = csv.DictWriter(file, fieldnames=chunk[0].keys(), delimiter=';')
    writer.writeheader()
    writer.writerows(chunk)

def merge_sorted_chunks(sorted_chunks, output_file):
    writers = []
    for i, chunk in enumerate(sorted_chunks):
        reader = csv.DictReader(chunk, delimiter=';')
        logging.info(f"Chunk {i+1} columns: {reader.fieldnames}")
        writers.append(reader)
    
    fieldnames = writers[0].fieldnames
    writer = csv.DictWriter(output_file, fieldnames=fieldnames, delimiter=';')
    writer.writeheader()

    def parse_date(row):
        try:
            return datetime.strptime(row['fecha_completa'].strip(), "%Y.%m.%d %H:%M:%S")
        except KeyError:
            logging.error(f"KeyError: 'fecha_completa' not found in row: {row}")
            return datetime.min
        except ValueError:
            logging.error(f"ValueError: Invalid date in row: {row}")
            return datetime.min

    merged = heapq.merge(*writers, key=lambda row: parse_date(row))
    
    row_count = 0
    for row in merged:
        writer.writerow(row)
        row_count += 1
        if row_count % 1000000 == 0:
            logging.info(f"Merged {row_count} rows")
            print(f"Merged {row_count} rows")

    logging.info(f"Total rows merged: {row_count}")
    print(f"Total rows merged: {row_count}")

def sort_large_csv(input_file, output_file):
    temp_files = []
    chunk_count = 0
    total_rows = 0
    
    try:
        with open(input_file, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            logging.info(f"Input file columns: {reader.fieldnames}")
            print(f"Input file columns: {reader.fieldnames}")
            chunk = []
            for i, row in enumerate(reader):
                chunk.append(row)
                if len(chunk) == CHUNK_SIZE:
                    logging.info(f"Sorting chunk {chunk_count + 1}")
                    print(f"Sorting chunk {chunk_count + 1}")
                    sorted_chunk = sort_chunk(chunk)
                    temp_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, newline='', encoding='utf-8')
                    write_chunk(sorted_chunk, temp_file)
                    temp_file.close()
                    temp_files.append(temp_file.name)
                    chunk = []
                    chunk_count += 1
                    total_rows += CHUNK_SIZE
                    print(f"Processed {total_rows} rows")
                    
                    if MAX_CHUNKS > 0 and chunk_count >= MAX_CHUNKS:
                        break

            if chunk:  # Don't forget the last chunk
                logging.info(f"Sorting final chunk {chunk_count + 1}")
                print(f"Sorting final chunk {chunk_count + 1}")
                sorted_chunk = sort_chunk(chunk)
                temp_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, newline='', encoding='utf-8')
                write_chunk(sorted_chunk, temp_file)
                temp_file.close()
                temp_files.append(temp_file.name)
                total_rows += len(chunk)
                print(f"Processed {total_rows} rows")
        
        logging.info(f"Merging {len(temp_files)} sorted chunks...")
        print(f"Merging {len(temp_files)} sorted chunks...")
        with open(output_file, 'w', newline='', encoding='utf-8') as output:
            merge_sorted_chunks([open(f, 'r', newline='', encoding='utf-8') for f in temp_files], output)
        
        logging.info("Cleaning up temporary files...")
        print("Cleaning up temporary files...")
        for file in temp_files:
            os.unlink(file)

        logging.info("Sorting completed!")
        print("Sorting completed!")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        print(f"An error occurred: {str(e)}")
        raise

try:
    sort_large_csv(INPUT_PATH, OUTPUT_PATH)
    print("Process completed successfully. Check sort_log.txt for details.")
except Exception as e:
    print(f"An error occurred: {str(e)}")
    print("Check sort_log.txt for more details.")