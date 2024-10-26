import csv
from datetime import datetime
import os

def split_csv_by_day(input_file, output_dir, max_lines=0):
    current_date = None
    current_file = None
    writer = None
    row_count = 0

    with open(input_file, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        fieldnames = reader.fieldnames

        for row in reader:
            row_count += 1
            if max_lines > 0 and row_count > max_lines:
                break

            date_str = row['fecha_completa'].split()[0]  # Extract date part
            date = datetime.strptime(date_str, "%Y.%m.%d")
            
            if date != current_date:
                if current_file:
                    current_file.close()
                    print(f"Completed file for {current_date.date()}")

                current_date = date
                filename = os.path.join(output_dir, f"P2000_{date.strftime('%Y%m%d')}.csv")
                current_file = open(filename, 'w', newline='', encoding='utf-8')
                writer = csv.DictWriter(current_file, fieldnames=fieldnames, delimiter=';')
                writer.writeheader()
                print(f"Creating new file: {filename}")

            writer.writerow(row)

            if row_count % 100000 == 0:
                print(f"Processed {row_count} rows")

    if current_file:
        current_file.close()
        print(f"Completed file for {current_date.date()}")

    print(f"Total rows processed: {row_count}")
    print("File splitting completed.")

# File paths
input_file = r"C:/Users/t.rodriguezb/Dropbox/Torniquetes_TRT/Data/P2000/Accesos_P2000_sorted.csv"
output_dir = r"C:/Users/t.rodriguezb/Dropbox/Torniquetes_TRT/Data/P2000/Daily"

# Ensure the output directory exists
os.makedirs(output_dir, exist_ok=True)

# Set the number of lines to process (0 for all lines, or a specific number for testing)
test_lines = 0  # Change this to a positive number for testing, or keep it as 0 to process all lines

# Run the splitting process
split_csv_by_day(input_file, output_dir, max_lines=test_lines)