# Demultiplexing script for NextSeq

# This is the primary demultiplexing job. It is contolled by the setup-
# nextseq-demultiplexing script, through the use of UDFs on the process in the
# LIMS.

# Its primary data processing functionality is handled by the program:
# bcl2fastq


import sys, os, re
import subprocess
from genologics.lims import *
import shutil
import requests
from common import nsc, utilities, demultiplex, parse, copyfiles



def run_demultiplexing(process, num_samples, bases_mask, n_threads, 
        run_dir, input_dir, output_dir, other_options, log_dir):
    """Run bcl2fastq2."""

    log_path = os.path.join(log_dir, "bcl2fastq-" + process.id + ".log")
    
    args = ['--no-lane-splitting', '--runfolder-dir', run_dir]
    args += ['--input-dir', input_dir]
    args += ['--output-dir', output_dir]
    if bases_mask:
        args += ['--use-bases-mask', bases_mask]
    args += get_thread_args(n_threads, num_samples)
    if other_options:
        args += re.split(" +", other_options)

    with open(log_path, "w") as log:
        rcode = subprocess.call([nsc.BCL2FASTQ2] + args, stdout=log, stderr=log)

    utilities.upload_file(process, nsc.BCL2FASTQ_LOG, log_path)

    if rcode == 0:
        return True
    else:
        utilities.fail(process, "bcl2fastq failure (see log)") 
        return False





            sample_sheet = parse.parse_ne_mi_seq_sample_sheet(sample_sheet_content)
            reads = [1]
            try:
                if seq_proc.udf['Read 2 Cycles']:
                    reads.append(2)
            except KeyError:
                pass

            if sample_sheet:
                try:
                    project_name = sample_sheet['data'][0]['project']
                except KeyError:
                    project_name = sample_sheet['header']['Experiment Name']
                proj_dir = parse.get_project_dir(runid, project_name)
                project_path = os.path.join(cfg.output_dir, proj_dir)
                try:
                    os.mkdir(project_path)
                except OSError:
                    pass
                # Move files into directory for project, no subdirectory with LIMS ID
                move_files(runid, cfg.output_dir, project_name, sample_sheet['data'], reads)

                utilities.running(process, "Gathering statistics")
                id_res_map = make_id_resultfile_map(process, sample_sheet['data'], reads)
                path = os.path.join(cfg.output_dir, "Stats")
                stats = parse.get_nextseq_stats(path, aggregate_lanes=True, aggregate_reads=True)
                success = demultiplex.populate_results(process, id_res_map, stats)

            if not success:
                utilities.fail(process, "Failed to set UDFs")
            
        else:
            utilities.fail(process, "Demultiplexing process exited with an error status")

    else: # cfg
        utilities.fail(process, "Missing configuration information, can't demultiplex")
        
    



