#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os


INPUT_PATH = '/hpcfs/home/economia/ga.castillo/projects/TOR/turnstiles/01_build/03_output/Accesos_P2000_sorted.csv'
OUTPUT_PATH = "/hpcfs/home/economia/ga.castillo/projects/TOR/turnstiles/01_build/03_output/time_analysis_output"

def load_sample_data(file_path, sample_size=1000000):
    """
    Load a sample of the data with specific columns to minimize memory usage
    """
    # Only read necessary columns
    columns_to_use = ['fecha_completa', 'tipoacceso']
    
    print(f"Reading {sample_size:,} rows from {file_path}...")
    
    try:
        df = pd.read_csv(file_path, 
                        nrows=sample_size,
                        delimiter=';',
                        usecols=columns_to_use)
        
        print(f"Successfully loaded {len(df):,} rows")
        return df
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        raise

def prepare_data(df):
    """
    Prepare the data for time analysis
    """
    # Convert fecha_completa to datetime
    df['fecha_completa'] = pd.to_datetime(df['fecha_completa'], format='%Y.%m.%d %H:%M:%S')
    
    # Extract hour
    df['hour'] = df['fecha_completa'].dt.hour
    
    # Create time periods
    df['time_period'] = pd.cut(
        df['hour'],
        bins=[-1, 5, 7, 9, 11, 14, 16, 18, 21, 23],
        labels=[
            'Early Morning (12-5)',
            'Early Peak (6-7)',
            'Morning Peak (8-9)',
            'Late Morning (10-11)',
            'Lunch (12-14)',
            'Early Afternoon (15-16)',
            'Late Afternoon (17-18)',
            'Evening (19-21)',
            'Night (22-23)'
        ]
    )
    
    return df

def analyze_patterns(df):
    """
    Analyze patterns and create visualizations
    """
    # Setup the plotting style
    plt.style.use('seaborn')
    sns.set_palette("husl")
    
    # Create figure with multiple subplots
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(15, 20))
    
    # 1. Hourly distribution
    hourly_counts = df.groupby(['hour', 'tipoacceso']).size().unstack(fill_value=0)
    hourly_counts.plot(kind='bar', ax=ax1)
    ax1.set_title('Hourly Distribution of Campus Activity')
    ax1.set_xlabel('Hour of Day')
    ax1.set_ylabel('Number of Records')
    ax1.legend(title='Access Type')
    
    # 2. Time period distribution
    period_counts = df.groupby(['time_period', 'tipoacceso']).size().unstack(fill_value=0)
    period_counts.plot(kind='bar', ax=ax2)
    ax2.set_title('Campus Activity by Time Period')
    ax2.set_xlabel('Time Period')
    ax2.set_ylabel('Number of Records')
    plt.xticks(rotation=45)
    ax2.legend(title='Access Type')
    
    # 3. IN/OUT ratio by time period
    in_out_ratio = period_counts['IN'] / period_counts['OUT']
    in_out_ratio.plot(kind='line', marker='o', ax=ax3)
    ax3.set_title('IN/OUT Ratio by Time Period')
    ax3.set_xlabel('Time Period')
    ax3.set_ylabel('Ratio (IN/OUT)')
    ax3.axhline(y=1, color='r', linestyle='--', alpha=0.5)
    plt.xticks(rotation=45)
    
    plt.tight_layout()
    
    # Save the plot
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    plt.savefig(os.path.join(OUTPUT_PATH, 'time_patterns.png'))
    print(f"Saved visualization to {OUTPUT_PATH}/time_patterns.png")
    
    # Generate summary statistics
    summary_stats = {
        'hourly_counts': hourly_counts,
        'period_counts': period_counts,
        'in_out_ratio': in_out_ratio
    }
    
    return summary_stats

def main():
    # Load sample data
    df = load_sample_data(INPUT_PATH, sample_size=1000000)
    
    # Prepare data
    df = prepare_data(df)
    
    # Analyze patterns
    stats = analyze_patterns(df)
    
    # Print summary statistics
    print("\nSummary of Activity by Time Period:")
    print(stats['period_counts'])
    
    print("\nIN/OUT Ratio by Time Period:")
    print(stats['in_out_ratio'])

if __name__ == "__main__":
    main()
