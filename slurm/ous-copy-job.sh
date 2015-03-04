#!/bin/bash

# Slurm job for data copy tasks, to run on the OUS closed network slurm.

# Accounting / management
#SBATCH --account=nsc
#SBATCH --qos=high
#SBATCH --partition=nsc
#SBATCH --job-name="Copy global run data"

# Job resources. Don't need many resources for a basic rsync or smbclient.
#SBATCH --nodes=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=1000

# Set performance options: 
#SBATCH --mem_bind=local
#SBATCH --hint=compute_bound
#SBATCH --hint=multithread

# Execute the requested command
"$@"

