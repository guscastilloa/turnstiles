#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os
import calendar


INPUT_PATH = '/hpcfs/home/economia/ga.castillo/projects/TOR/turnstiles/01_build/03_output/Accesos_P2000_sorted.csv'
OUTPUT_PATH = "/hpcfs/home/economia/ga.castillo/projects/TOR/turnstiles/01_build/03_output/time_analysis_output"


def load_and_prepare_data(file_path, sample_size=None):
    """
    Load and prepare data with monthly information
    """
    columns_to_use = ['fecha_completa', 'tipoacceso']
    
    df = pd.read_csv(file_path, 
                     nrows=sample_size,
                     delimiter=';',
                     usecols=columns_to_use)
    
    # Convert and extract time components
    df['fecha_completa'] = pd.to_datetime(df['fecha_completa'], format='%Y.%m.%d %H:%M:%S')
    df['hour'] = df['fecha_completa'].dt.hour
    df['month'] = df['fecha_completa'].dt.month
    df['year'] = df['fecha_completa'].dt.year
    df['month_name'] = df['fecha_completa'].dt.strftime('%B %Y')
    
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

def create_monthly_heatmap(df):
    """
    Create a heatmap showing activity patterns by month and hour
    """
    plt.figure(figsize=(15, 8))
    
    # Create pivot table for heatmap
    pivot_data = df.pivot_table(
        values='tipoacceso',
        index='month_name',
        columns='hour',
        aggfunc='count',
        fill_value=0
    )
    
    # Create heatmap
    sns.heatmap(pivot_data, 
                cmap='YlOrRd',
                fmt='g',
                cbar_kws={'label': 'Number of Records'},
                xticklabels=range(24))
    
    plt.title('Campus Activity Heatmap by Month and Hour')
    plt.xlabel('Hour of Day')
    plt.ylabel('Month')
    
    return plt.gcf()

def create_publication_summary(df):
    """
    Create publication-ready summary plots
    """
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 15))
    
    # 1. Average daily patterns by semester
    df['semester'] = pd.cut(df['month'], 
                          bins=[0, 6, 12], 
                          labels=['Spring', 'Fall'])
    
    semester_patterns = df.groupby(['semester', 'hour', 'tipoacceso']).size().unstack()
    semester_patterns.columns = semester_patterns.columns.str.strip()
    semester_patterns = semester_patterns.groupby('semester').transform(
        lambda x: x / x.sum() * 100
    )
    
    for semester in semester_patterns.index.get_level_values('semester').unique():
        data = semester_patterns.xs(semester, level='semester')
        data.plot(ax=ax1, label=f'{semester}', marker='o', linestyle='-')
    
    ax1.set_title('Average Daily Patterns by Semester')
    ax1.set_xlabel('Hour of Day')
    ax1.set_ylabel('Percentage of Daily Activity')
    ax1.legend(title='Semester / Access Type')
    ax1.grid(True, alpha=0.3)
    
    # 2. IN/OUT ratio evolution across months
    monthly_ratio = df.groupby(['month_name', 'tipoacceso']).size().unstack()
    monthly_ratio.columns = monthly_ratio.columns.str.strip()
    monthly_ratio['IN/OUT'] = monthly_ratio['IN'] / monthly_ratio['OUT']
    
    monthly_ratio['IN/OUT'].plot(ax=ax2, marker='o', linestyle='-')
    ax2.set_title('Monthly Evolution of IN/OUT Ratio')
    ax2.set_xlabel('Month')
    ax2.set_ylabel('IN/OUT Ratio')
    ax2.axhline(y=1, color='r', linestyle='--', alpha=0.5)
    plt.xticks(rotation=45)
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    return fig

def main():
    # File path
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    
    # Load and prepare data
    df = load_and_prepare_data(INPUT_PATH)
    
    # Create and save monthly heatmap
    heatmap_fig = create_monthly_heatmap(df)
    heatmap_fig.savefig(os.path.join(OUTPUT_PATH, 'monthly_heatmap.png'), 
                        bbox_inches='tight', dpi=300)
    
    # Create and save publication summary
    pub_fig = create_publication_summary(df)
    pub_fig.savefig(os.path.join(OUTPUT_PATH, 'publication_summary.png'), 
                    bbox_inches='tight', dpi=300)
    
    print("Analysis complete. Output saved to 'time_analysis_output' directory.")

if __name__ == "__main__":
    main()