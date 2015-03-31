# Quality control script

# This is a meta-script that calls various QC and reporting
# modules.
# This script handles the interface with the LIMS; and the qc module is
# not directly interfaced with the LIMS.


import argparse
from collections import defaultdict
from genologics import *
import nsc, utilities
import qc
import parse


def get_hiseq_projects(sample_sheet, demultiplex_stats, reads=[]):
    proj_samples = defaultdict(list)
    # sort samples into projects
    for sr in sample_sheet:
        proj_samples[sr['SampleProject']].append(sr)

    # create objects
    projects = []
    for projname, rows in proj_samples.items():
        projdir = "Project_" + projname
        samples = []
        for sam in rows:
            sample_name = sam['SampleID']
            lane = int(sam['Lane'])
            files = []
            files = [projdir + "/Sample_{1}/TODO"]
            s = Sample(files)
            samples.append(s)

        projects.append(Project(projname, projdir, samples))

    

def qc_hiseq():
    ss = parse.parse_hiseq_sample_sheet()
    ds = parse.parse_demux_stats.....
    projects = get_hiseq_projects(demulitplex_dir, ss, ["1","2"])

def main(process_id):
    pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--pid", help="Process ID", required=True)
    args = parser.parse_args()

    main(args-pid)


