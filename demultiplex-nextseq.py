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
from common import nsc, utilities, demultiplex, parse, copyfiles

class Config:
    pass

def get_config(process):
    """Configuration is stored in UDFs on the demultiplexing process. This
    function loads them into a generic object.
    
    The options are set by """

    try:
        cfg = Config()
        cfg.n_threads = process.udf[nsc.THREADS_UDF]
        cfg.run_dir = process.udf[nsc.SOURCE_RUN_DIR_UDF]
        cfg.output_dir = process.udf[nsc.DEST_FASTQ_DIR_UDF]
    except KeyError:
        return None
    # Optional
    try:
        cfg.other_options = process.udf[nsc.NS_OTHER_OPTIONS_UDF]
    except KeyError:
        cfg.other_options = None

    try:
        cfg.bases_mask = process.udf[nsc.BASES_MASK_UDF]
    except KeyError:
        cfg.bases_mask = None

    return cfg


def get_thread_args(n_threads, num_samples):
    # Computing number of threads: use a little more than we have, per illumina recommendation
    # Total is limited by slurm cpu binding anyway
    base = n_threads * 1.5
    loading = int(max(1, base * 0.1))
    # No more write threads than 1 per sample
    writing = int(max(1, min(num_samples, base * 0.1)))
    remaining = base - loading - writing
    processing = int(max(1, remaining * 0.7))
    demultiplexing = int(max(1, remaining - processing))
    return ['-r', str(loading), '-d', str(demultiplexing),
            '-p', str(processing), '-w', str(writing)]


def run_demultiplexing(process, num_samples, bases_mask, n_threads, 
        run_dir, input_dir, output_dir, other_options, log_dir):
    """Run bcl2fastq2."""

    log_path = os.path.join(log_dir, "bcl2fastq-" + process.id + ".log")
    
    args = ['--runfolder-dir', run_dir]
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


def make_id_resultfile_map(process, sample_sheet_data, reads):
    """Produces map from lane, sample-ID and read to output 
    analyte. Lane is always 1 for NS, but keeping it in for consistency."""
    themap = {}
    for entry in sample_sheet_data:
        name = entry['samplename']
        input_limsid = entry['sampleid']
        input_sample = Artifact(nsc.lims, id=input_limsid).samples[0]
        for output in process.all_outputs(unique=True):
            #for read in reads:
            #    for lane in xrange(1, 5):
            if output.name == nsc.NEXTSEQ_FASTQ_OUTPUT.format(
                    input_sample.name
                    ):
                themap[(1,name,1)] = output
    return themap


def move_files(runid, output_dir, project_name, sample_sheet, reads):
    proj_dir = parse.get_project_dir(runid, project_name)
    project_path = output_dir + "/" + proj_dir
    
    params = []
    for i, row in enumerate(sample_sheet):
        for r in reads:
            par = dict(row)
            par['read'] = r
            par['base'] = output_dir
            par['index'] = i+1
            par['project'] = project_name
            par['project_path'] = project_path
            params.append(par)

    with_id_subdir = not all(p['sampleid'] == p['samplename'] for p in params)
    for p in params:
        files = [
                "{samplename}_S{index}_L{lane}_R{read}_001.fastq.gz".format(
                    lane=str(lane).zfill(3), **p
                    )
                for lane in xrange(1, 5)
                ]

        for f in files:
            if with_id_subdir:
                input_path = "{base}/{project}/{sampleid}/{filename}".format(filename=f, **p)
            else:
                input_path = "{base}/{project}/{filename}".format(filename=f, **p)

            output_path = os.path.join(project_path, f)
            print "in", input_path, "out", output_path
            os.rename(input_path, output_path)

    for dpath in set("{base}/{project}/{sampleid}".format(**p) for p in params):
        os.rmdir(dpath)
    for dpath in set("{base}/{project}".format(**p) for p in params):
        os.rmdir(dpath)


def get_sample_sheet(process, output_run_dir):
    """Get sample sheet from LIMS or run directory."""

    ssheet_file, sample_sheet = demultiplex.download_sample_sheet(process, output_run_dir, False)
    if ssheet_file:
        return ssheet_file, sample_sheet
    elif os.path.exists(os.path.join(output_run_dir, "SampleSheet.csv")):
        path = os.path.join(output_run_dir, "SampleSheet.csv")
        data = open(path).read()
        return path, data 
    else:
        return None, None



def main(process_id):
    os.umask(007)
    process = Process(nsc.lims, id=process_id)

    utilities.running(process)
    
    seq_proc = utilities.get_sequencing_process(process)
    runid = seq_proc.udf['Run ID']

    print "Demultiplexing process for LIMS process", process_id, ", NextSeq run", runid
    destination = os.path.join(nsc.SECONDARY_STORAGE, runid)
    log_dir = os.path.join(destination, "DemultiplexLogs")

    if nsc.DO_COPY_METADATA_FILES:
        already_existed = True
        try:
            os.mkdir(destination)
            already_existed = False
            os.mkdir(log_dir)
        except OSError:
            pass

        if not already_existed:
            utilities.running(process, "Copying run directory")
            if not copyfiles.copy_files(runid, 'hiseq'):
                utilities.fail(process, 'Unable to copy files')
                return False

    success = False

    cfg = get_config(process)
    if cfg:
        # Download sample sheet or get it from the root of the run directory
        # For the NS, the setup-nextseq-demultiplexing script will copy the
        # LIMS generated sample sheet to the demultiplexing process.
        ssheet_file, sample_sheet_content = get_sample_sheet(process, destination)

        if sample_sheet_content:
            sample_sheet = parse.parse_ne_mi_seq_sample_sheet(sample_sheet_content)
            num_samples = len(sample_sheet['data'])
        else:
            sample_sheet = None
            num_samples = 1

        utilities.running(process, "Demultiplexing")
        input_dir = os.path.join(cfg.run_dir, "Data", "Intensities", "BaseCalls")
        process_ok = run_demultiplexing(process, num_samples,
                cfg.bases_mask, cfg.n_threads, destination, input_dir, cfg.output_dir,
                cfg.other_options, log_dir)
        process_ok = True
        
        if process_ok:
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
                move_files(runid, cfg.output_dir, project_name, sample_sheet['data'], reads)

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
        
    
    if success:
        utilities.success_finish(process)
        return True
    # If failing, should have already notified of failure, just quit
    else:
        if not process.udf[nsc.JOB_STATUS_UDF].startswith('Fail'):
            utilities.fail(process, "Unknown failure")
        return False


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        try:
            ok = main(sys.argv[1])
        except:
            process = Process(nsc.lims, id = sys.argv[1])
            utilities.fail(process, "Unexpected: " + str(sys.exc_info()[1]))
            raise
        sys.exit(ok)
    else:
        print "use: demultiplex-nextseq.py <process-id>"
        sys.exit(1)


