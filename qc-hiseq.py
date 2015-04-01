# Wrapper script to call the qc module from a slurm job initiated by the LIMS
# QC can run with almost unlimited parallelism, but it depends on the number of
# samples (fastq files).

import sys


def main(process_id):
    print "Sorry, not yet implemented"





# Usage: qc-hiseq.py QC-PID
# QC-PID is the process ID of the QC process

if __name__ == "__main__":
    main(sys.argv[1])

