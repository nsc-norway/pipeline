#!/bin/bash

# Slurm job for OUS.

# Accounting / management
#SBATCH --account=nsc
#SBATCH --qos=high
#SBATCH --partition=main

# Job resources. 
#SBATCH --nodes=1
#SBATCH --mem=20G
#SBATCH --cpus-per-task=20

# Set performance options: 
#SBATCH --mem_bind=local
#SBATCH --hint=compute_bound
#SBATCH --hint=multithread

# Execute the requested command
/usr/bin/python /data/nsc.loki/automation/pipeline/run-qc-hiseq.py $1

