
import csv
from datetime import datetime

#conda info --envs
#cd "C:\Users\t.rodriguezb\Dropbox\Torniquetes Nuevos\Code"
#cd "C:\Users\t.rodriguezb\Dropbox\Torniquetes_TRT\Data\P2000"


def is_sorted_by_date(file_path):
    previous_date = None
    is_sorted = True
    row_count = 0
    current_month_year = None

    with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
        csv_reader = csv.DictReader(csvfile, delimiter=';')
        
        for row in csv_reader:
            row_count += 1
            current_date_str = row['fecha_completa'].strip()
            
            try:
                current_date = datetime.strptime(current_date_str, "%Y.%m.%d %H:%M:%S")
            except ValueError:
                print(f"Error: Invalid date format in row {row_count}: {current_date_str}")
                continue
            
            # Check for new month-year
            new_month_year = current_date.strftime("%Y-%m")
            if new_month_year != current_month_year:
                print(f"Processing: {new_month_year}")
                current_month_year = new_month_year
            
            if previous_date is not None and current_date < previous_date:
                print(f"Sorting error found at row {row_count}")
                print(f"Previous date: {previous_date}")
                print(f"Current date: {current_date}")
                is_sorted = False
                break
            
            previous_date = current_date
            
            if row_count % 1000000 == 0:
                print(f"Processed {row_count} rows...")

    return is_sorted, row_count

file_path = r"C:/Users/t.rodriguezb/Dropbox/Torniquetes_TRT/Data/P2000/Accesos_P2000_sorted_test.csv"
print(f"Starting to process file: {file_path}")
sorted_result, total_rows = is_sorted_by_date(file_path)

if sorted_result:
    print(f"The file is sorted by date. Total rows processed: {total_rows}")
else:
    print(f"The file is not sorted by date. Stopped checking at row {total_rows}")