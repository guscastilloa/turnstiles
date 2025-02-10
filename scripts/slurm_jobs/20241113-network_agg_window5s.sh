#!/bin/bash
#SBATCH --job-name=net_agg_window5s    # Job name
#SBATCH --output=logs/net_agg_window5s_%j.log   # Output file (%j = job ID)
#SBATCH --error=logs/net_agg_window5s_%j.err    # Error file
#SBATCH --nodes=1                   # Run on a single node
#SBATCH --ntasks=1                  # Run a single task
#SBATCH --cpus-per-task=2          # Increased to handle 5 parallel processes (one per window) + 1 spare
#SBATCH --mem=32G                   # Increased memory (~15GB per process + overhead)
#SBATCH --time=03:00:00             # Time limit hrs:min:sec for test run
#SBATCH --mail-type=BEGIN,END,FAIL       # Email notification
#SBATCH --mail-user=ga.castillo@uniandes.edu.co  # Email address

# Echo start time
echo "Job started at: $(date)"
echo "Processing window size: 4 seconds"

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
    echo "Number of CPU cores allocated: $SLURM_CPUS_PER_TASK"
    echo "Memory allocated: $SLURM_MEM_PER_NODE MB"
else
    echo "Failed to activate Conda environment 'tor_env'."
    exit 1
fi

# Verify the Python interpreter
python_path="$HOME/.conda/envs/tor_env/bin/python"
echo "Using Python interpreter at: $python_path"

# Navigate to script directory
cd $CODE_DIR || exit 1
echo "Current workdir: $PWD"

# Run the Python script

srun $python_path turnstile_network_aggregation-parallel.py --window 5

# Echo end time
echo -e "\nJob finished at: $(date)"
