#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os
from config import ProjectConfig, Phase

def load_and_prepare_data(file_path, sample_size=None):
    columns_to_use = ['fecha_completa', 'tipoacceso']
    
    df = pd.read_csv(file_path, 
                     nrows=sample_size,
                     delimiter=';',
                     usecols=columns_to_use)
    
    df['fecha_completa'] = pd.to_datetime(df['fecha_completa'], format='%Y.%m.%d %H:%M:%S')
    df['hour'] = df['fecha_completa'].dt.hour
    df['month'] = df['fecha_completa'].dt.month
    df['year'] = df['fecha_completa'].dt.year
    df['yearmonth'] = df['fecha_completa'].dt.strftime('%Y-%m')
    df['month_name'] = df['fecha_completa'].dt.strftime('%B %Y')
    
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
    plt.figure(figsize=(15, 10))
    
    # Create pivot table with yearmonth index for proper sorting
    pivot_data = df.pivot_table(
        values='tipoacceso',
        index='yearmonth',
        columns='hour',
        aggfunc='count',
        fill_value=0
    )
    
    # Sort index in descending order
    pivot_data = pivot_data.sort_index(ascending=False)
    
    # Create heatmap
    sns.heatmap(pivot_data, 
                cmap='YlOrRd',
                fmt='g',
                cbar_kws={'label': 'Number of Records'},
                xticklabels=range(24))
    
    plt.title('Campus Activity Heatmap by Month and Hour')
    plt.xlabel('Hour of Day')
    plt.ylabel('Year-Month')
    
    return plt.gcf()

def create_publication_summary(df):
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 15))
    
    df['semester'] = pd.cut(df['month'], 
                          bins=[0, 6, 12], 
                          labels=['Spring', 'Fall'])
    
    semester_patterns = df.groupby(['semester', 'hour', 'tipoacceso']).size().unstack()
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
    
    monthly_ratio = df.pivot_table(
        index='yearmonth',
        columns='tipoacceso',
        values='hour',
        aggfunc='count',
        fill_value=0
    ).sort_index()
    
    monthly_ratio['IN/OUT'] = monthly_ratio['IN'] / monthly_ratio['OUT']
    monthly_ratio['IN/OUT'].plot(ax=ax2, marker='o', linestyle='-')
    
    ax2.set_title('Monthly Evolution of IN/OUT Ratio')
    ax2.set_xlabel('Year-Month')
    ax2.set_ylabel('IN/OUT Ratio')
    ax2.axhline(y=1, color='r', linestyle='--', alpha=0.5)
    plt.xticks(rotation=45)
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    return fig

def main():
    # Initialize config
    config = ProjectConfig(phase=Phase.BUILD)
    
    # Setup paths
    input_file = os.path.join(config.get_path('output'), 'Accesos_P2000_sorted.csv')
    output_dir = os.path.join(config.get_path('output'), 'Figures', 'time_analysis')
    os.makedirs(output_dir, exist_ok=True)
    
    # Load and prepare data
    df = load_and_prepare_data(input_file)
    
    # Create and save monthly heatmap
    heatmap_fig = create_monthly_heatmap(df)
    heatmap_fig.savefig(os.path.join(output_dir, 'monthly_heatmap.png'), 
                        bbox_inches='tight', dpi=300)
    
    # Create and save publication summary
    pub_fig = create_publication_summary(df)
    pub_fig.savefig(os.path.join(output_dir, 'publication_summary.png'), 
                    bbox_inches='tight', dpi=300)
    
    print(f"Analysis complete. Output saved to {output_dir}")

if __name__ == "__main__":
    main()