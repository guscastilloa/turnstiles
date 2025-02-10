#!/bin/bash
#SBATCH --job-name=check_dates_full    # Job name
#SBATCH --output=logs/check_dates_full_%j.log   # Output file (%j = job ID)
#SBATCH --error=logs/check_dates_full_%j.err    # Error file
#SBATCH --nodes=1                   # Run on a single node
#SBATCH --ntasks=1                  # Run a single task
#SBATCH --cpus-per-task=8          # 
#SBATCH --mem=64G                   # Request 64GB RAM (adjust if needed)
#SBATCH --time=02:00:00            # Time limit hrs:min:sec
#SBATCH --mail-type=END,FAIL       # Email notification
#SBATCH --mail-user=ga.castillo@uniandes.edu.co  # Email address

# Echo start time
echo "Job started at: $(date)"

# Define Paths
HOME_DIR="/hpcfs/home/economia/ga.castillo"
PROJECT_DIR="$HOME_DIR/projects/TOR"
CODE_DIR="$PROJECT_DIR/turnstiles/01_build/02_scripts"


# Load required modules
module load anaconda/conda4.12.0

# Activate virtual environment
source activate tor_env

PYTHONPATH=$(which python)


# Check if environment is activated
if [[ "$CONDA_PREFIX" == *"tor_env"* ]]; then
    echo "Conda environment 'tor_env' is successfully activated."
    echo "PYTHONPATH is $PYTHONPATH"
else
    echo "Failed to activate Conda environment 'tor_env'."
    exit 1
fi


# Verify the Python interpreter
python_path="$HOME/.conda/envs/tor_env/bin/python"
echo "Using Python interpreter at: $python_path"

# Navigate to script directory
cd $CODE_DIR || exit 1
echo "Current wokdir: $PWD"

# Run the Python script with MPI
srun $python_path check_time_patterns_all.py

# Echo end time
echo -e "\nJob finished at: $(date)"

