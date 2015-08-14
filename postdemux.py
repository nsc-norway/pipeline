import sys, os

from genologics.lims import *
from common import nsc


#    Post-demultiplexing process:
#    1) rename files and directories to NSC standard
#    2) generate reports (Delivery folder)
#    3) post stats to LIMS - OK
#    4) run fastqc
#    5) generate PDFs and the HTML page


udf_list = [
        '# Reads', 'Yield PF (Gb)', '% of Raw Clusters Per Lane',
        '% of PF Clusters Per Lane',
        '% Perfect Index Read', 'One Mismatch Reads (Index)',
        '% Bases >=Q30', 'Ave Q Score'
        ]



def main_lims(process_id):
    """LIMS mode main function.
    """
    pass


def main(todo):
    pass









def create_projdir_ne_mi(runid, basecalls_dir, sample_sheet, lane, reads):
    """Creates project directory and moves fastq files into it."""

    project_name = sample_sheet['header']['Experiment Name']
    
    dir_name = parse.get_project_dir(runid, project_name)
    proj_path = basecalls_dir + "/" + dir_name
    try:
        os.mkdir(proj_path)
    except OSError:
        pass
    for i, sam in enumerate(sample_sheet['data']):
        for r in reads:
            sample_name = sam['samplename']
            if not sample_name:
                sample_name = sam['sampleid']
            basename = "{0}_S{1}_L00{2}_R{3}_001.fastq.gz".format(
                    sample_name, str(i + 1), lane, r)
            old_path = basecalls_dir + "/" + basename
            new_path = proj_path + "/" + basename
            os.rename(old_path, new_path)



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--sbatch', default=False, action='store_true', help="Running under sbatch (not working well)")
    parser.add_argument('--threads', type=int, default=1, help='Number of threads (cores)')
    parser.add_argument('--pid', default=None, help="Process-ID if running within LIMS")
    parser.add_argument('--no-sample-sheet', action='store_true', help="Run without sample sheet, look for files")
    parser.add_argument('--no-process-undetermined', action='store_true', help="Do not process undetermined indexes")
    parser.add_argument('DIR', default=None, nargs='?', help="Run directory")
    args = parser.parse_args()
    threads = args.threads
    try:
        threads = int(os.environ['SLURM_CPUS_ON_NODE'])
        print "Threads from slurm: ", threads
    except KeyError:
        pass

    if args.pid and not args.DIR:
        with utilities.error_reporter(args.pid):
            main_lims(threads, args.pid)

    elif args.DIR and not args.pid:
        main(threads, args.DIR, args.no_sample_sheet, not args.no_process_undetermined)
    else:
        print "Must specify either LIMS-ID of QC process or bcl2fastq2 output directory"
        sys.exit(1)





