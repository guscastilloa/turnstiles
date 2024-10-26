import pandas as pd
import os
from datetime import timedelta
from collections import defaultdict
import re

def process_daily_file(file_path, output_dir, torniquetes_dir):
    # Extract date from filename (assuming filename format is 'P2000_YYYYMMDD.csv')
    date_str = os.path.basename(file_path).split('_')[1].split('.')[0]
    
    # Load the file into a DataFrame
    df = pd.read_csv(file_path, delimiter=';', parse_dates=['fecha_completa'])
    
    # Create 'torniquete' field
    df['torniquete'] = df['porteria_detalle'].apply(lambda x: re.split('[-\s]', x)[0])
    
    # Write unique torniquete values to a CSV file
    unique_torniquetes = df['torniquete'].unique()
    torniquete_df = pd.DataFrame({'torniquete': unique_torniquetes})
    torniquete_file = os.path.join(torniquetes_dir, f"torniquetes_unicos_{date_str}.csv")
    torniquete_df.to_csv(torniquete_file, index=False)
    print(f"Saved unique torniquetes to {torniquete_file}")
    
    # Create 'torniquete' field
    df['torniquete'] = df['porteria_detalle'].apply(lambda x: re.split('[-\s]', x)[0])
    
    # Write unique torniquete values to a CSV file
    unique_torniquetes = df['torniquete'].unique()
    torniquete_df = pd.DataFrame({'torniquete': unique_torniquetes})
    torniquete_file = os.path.join(output_dir, f"unique_torniquetes_{os.path.basename(file_path)}")
    torniquete_df.to_csv(torniquete_file, index=False)
    print(f"Saved unique torniquetes to {torniquete_file}")
    
    # Get unique individuals
    unique_individuals = sorted(df['carnet'].unique())
    
    # Initialize dictionaries to store coincidences
    coincidences = defaultdict(lambda: {'v5s': 0, 'v5s_identical': 0})
    
    # Loop over unique individuals
    for individual in unique_individuals:
        # Get all occurrences of the individual
        occurrences = df[df['carnet'] == individual]
        
        for _, occurrence in occurrences.iterrows():
            timestamp = occurrence['fecha_completa']
            
            # Define the time window
            start_time = timestamp - timedelta(seconds=5)
            end_time = timestamp + timedelta(seconds=5)
            
            # Find all entries within the time window, only for individuals with higher carnet numbers
            window_entries = df[(df['fecha_completa'] >= start_time) & 
                                (df['fecha_completa'] <= end_time) & 
                                (df['carnet'] > individual)]
            
            # Update coincidences
            for _, other_entry in window_entries.iterrows():
                if occurrence['torniquete'] == other_entry['torniquete'] and \
                   occurrence['tipoacceso'] == other_entry['tipoacceso']:
                    pair = (individual, other_entry['carnet'])
                    coincidences[pair]['v5s'] += 1
                    
                    if occurrence['porteria_detalle'] == other_entry['porteria_detalle']:
                        coincidences[pair]['v5s_identical'] += 1
    
    # Create the result DataFrame
    result_df = pd.DataFrame([(c1, c2, data['v5s'], data['v5s_identical']) 
                              for (c1, c2), data in coincidences.items()],
                             columns=['Carnet1', 'Carnet2', 'v5s', 'v5s_identical'])
    
    return result_df

def process_all_daily_files(input_dir, output_dir, torniquetes_dir):
    # Ensure output directories exist
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(torniquetes_dir, exist_ok=True)
    
    # Process each file in the input directory
    for filename in os.listdir(input_dir):
        if filename.endswith('.csv'):
            print(f"Processing {filename}...")
            file_path = os.path.join(input_dir, filename)
            result_df = process_daily_file(file_path, output_dir, torniquetes_dir)
            
            # Save the result
            output_file = os.path.join(output_dir, f"coincidences_{filename}")
            result_df.to_csv(output_file, index=False)
            print(f"Saved results to {output_file}")

# Directories
input_dir = r"C:/Users/t.rodriguezb/Dropbox/Torniquetes_TRT/Data/P2000/Daily"
output_dir = r"C:/Users/t.rodriguezb/Dropbox/Torniquetes_TRT/Data/P2000/Coincidences"
torniquetes_dir = r"C:/Users/t.rodriguezb/Dropbox/Torniquetes_TRT/Data/P2000/Daily/torniquetes_unicos"

# Run the process
process_all_daily_files(input_dir, output_dir, torniquetes_dir)