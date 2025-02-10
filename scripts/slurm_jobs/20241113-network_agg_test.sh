#!/bin/bash
#SBATCH --job-name=net_agg_test    # Job name
#SBATCH --output=logs/net_agg_test_%j.log   # Output file (%j = job ID)
#SBATCH --error=logs/net_agg_test_%j.err    # Error file
#SBATCH --nodes=1                   # Run on a single node
#SBATCH --ntasks=1                  # Run a single task
#SBATCH --cpus-per-task=1          # 
#SBATCH --mem=16G                   
#SBATCH --time=2:00:00            # Time limit hrs:min:sec
#SBATCH --mail-type=BEGIN,END,FAIL       # Email notification
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
srun $python_path turnstile_network_aggregation.py --test --test-files 100

# Echo end time
echo -e "\nJob finished at: $(date)"

