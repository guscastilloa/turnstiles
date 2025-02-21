# Turnstiles Project


## Project Structure

```
.
├── config/         # Configuration files
├── data/           # Data directory (not tracked in git)
│   ├── raw/        # Original data files
│   ├── processed/  # Cleaned and processed data
│   └── intermediate/    # Intermediate data files
├── notebooks/     # Jupyter notebooks for exploration
├── scripts/       # Executable scripts
├── src/           # Source code modules
│   ├── data/      # Data processing modules
│   └── viz/       # Visualization modules
└── tests/         # Tests
    └── data/      # Sample anonymized datasets
```

Note: The `01_build` directory will be removed as we transition to a more modular structure using `src` for modules and `scripts` for execution.

## Data Sources

The project uses three main data sources:

1. **Turnstile Data**
   - Daily student entries through university turnstiles
   - Format: CSV files with timestamp and student ID records
   - Anonymized Sample: `tests/data/turnstile_sample_anon.parquet`

2. **Trust Experiment**
   - Network data from a trust game experiment
   - Format: Multiple CSV files (Friends.csv, Lunch.csv, etc.)
   - Sample: `tests/data/trust_sample_anon.parquet`

3. **Survey Data (MjAlvarez)**
   - Classroom friendship networks
   - Format: Adjacency matrices in CSV format
   - Anonymized Sample: `tests/data/survey_sample_anon.parquet`

## Anonymized Samples

The `tests/data` directory contains anonymized samples of each data source that can be used for development and testing:

- `turnstile_sample_anon.parquet`: Sample of turnstile entries
- `trust_sample_anon.parquet`: Sample of trust experiment data
- `survey_sample_anon.parquet`: Sample of classroom network data

These samples have been anonymized using consistent hashing to maintain ID relationships while protecting privacy.
