#!/bin/bash

# Slurm job for OUS.

# Accounting / management
#SBATCH --account=nsc
#SBATCH --qos=high
#SBATCH --partition=nsc

# Job resources. 
#SBATCH --nodes=1
# CPU and memory requirement to be set by arguments to sbatch.

# Set performance options: 
#SBATCH --mem_bind=local
#SBATCH --hint=compute_bound
#SBATCH --hint=multithread

# Execute the requested command
/usr/bin/python "${@}"

