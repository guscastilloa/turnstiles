#!/bin/bash
#SBATCH --job-name=checkP2000rows    # Job name
#SBATCH --nodes=1                 # Number of nodes
#SBATCH --cpus-per-task=4        # CPUs per MPI task
#SBATCH --mem=64000                # Memory per node
#SBATCH --time=1:00:00          # Time limit hrs:min:sec
#SBATCH --output=logs/checkP2000rows_%j.log   # Standard output and error log
#SBATCH --mail-type=BEGIN,END,FAIL    # Mail events
#SBATCH --mail-user=ga.castillo@uniandes.edu.co     # Where to send mail



# Directory containing the monthly P2000 files
P2000_DIR="/hpcfs/home/economia/ga.castillo/projects/TOR/data/P2000"
MERGED_FILE="${P2000_DIR}/Accesos_P2000.csv"

# Get row count of merged file (subtract 1 for header)
merged_count=$(( $(wc -l < "$MERGED_FILE") - 1 ))
echo "Rows in merged file (excluding header): $merged_count"

# Initialize counter for monthly files
total_monthly_rows=0

# Loop through all monthly CSV files (excluding the merged file)
echo "Counting rows in monthly files..."
for file in "${P2000_DIR}"/*.csv; do
    if [[ "$file" != "$MERGED_FILE" ]]; then
        # Subtract 1 from each file's count to account for header
        file_rows=$(( $(wc -l < "$file") - 1 ))
        total_monthly_rows=$(( total_monthly_rows + file_rows ))
        echo "$(basename "$file"): $file_rows rows"
    fi
done

echo -e "\nTotal rows in monthly files (excluding headers): $total_monthly_rows"
echo "Merged file rows: $merged_count"

if [ "$total_monthly_rows" -eq "$merged_count" ]; then
    echo "✅ Row counts match!"
else
    echo "❌ Row counts don't match!"
    echo "Difference: $(( merged_count - total_monthly_rows )) rows"
fi
