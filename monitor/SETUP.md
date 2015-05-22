### Setup!

This monitoring system needs a custom UDF to track which processes
are "open". This is done to prevent the need to load all processes
for every request. It also requires the UDFs provided by Genologics
and the Slurm job UDFs (currently only: Job status).

Check out main.py. For each process type in the following arrays,
add a UDF as specified below:

SEQUENCING: process types in the second element of the tuples
DATA_PROCESSING: list of process types in second element of tuples

UDF specficiation:

