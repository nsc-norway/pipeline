# Quality control script for HiSeq

# This is a meta-script that calls various QC and reporting
# modules.
# This script handles the interface with the LIMS; and the qc module is
# not directly interfaced with the LIMS.


import argparse
from genologics import *
import nsc
import qc


def main(process_id):
    
    pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--pid", help="Process ID", required=True)
    args = parser.parse_args()

    main(args-pid)
