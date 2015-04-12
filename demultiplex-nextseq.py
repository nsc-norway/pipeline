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
    function loads them into a generic object."""

    try:
        cfg = Config()
        cfg.n_threads = process.udf[nsc.THREADS_UDF]
        cfg.run_dir = process.udf[nsc.SOURCE_RUN_DIR_UDF]
        cfg.dest_dir = process.udf[nsc.DEST_FASTQ_DIR_UDF]
    except KeyError:
        return None
    # Optional
    try:
        cfg.other_options = process.udf[nsc.OTHER_OPTIONS_UDF]
    except KeyError:
        cfg.other_options = None

    return cfg



def run_demultiplexing(process, sample_sheet_path, n_threads, start_dir,
        dest_run_dir, other_options):
    """Run bcl2fastq2."""

    bcl2fastq_log_path = os.path.join(nsc.LOG_DIR, "bcl2fastq-" + process.id + ".log")
    log = open(bcl2fastq_log_path, "w")
    
    args += ['--output-dir', dest_run_dir]
    args += ['--input-dir', start_dir]
    if other_options:
        args += re.split(" +", other_options)

    rcode = subprocess.call([nsc.CONFIGURE_BCL_TO_FASTQ] + args, cwd=start_dir, stdout=log, stderr=log)

    log.close()
    utilities.upload_file(process, nsc.CONFIGURE_LOG, cfg_log_name)
    shutil.copy(ssheet, dest_run_dir + "/SampleSheet-" + process.id + ".csv")

    if rcode == 0:
        os.chdir(dest_run_dir)
        args = ['-j' + str(n_threads)]
        make_log_file = os.path.join(nsc.LOG_DIR, "make_" + process.id + ".log")

        with open(make_log_file, "w") as log:
            rcode = subprocess.call([nsc.MAKE] + args, stdout=log, stderr=log)

        utilities.upload_file(process, nsc.MAKE_LOG, make_log_file)

        if rcode == 0:
            return True
        else:
            utilities.fail(process, "Make failure (see log)")
    else:
        utilities.fail(process, "configureBclToFastq failure (see log)")
    
    return False




def rename_project_directories(runid, unaligned_dir, sample_sheet):
    """Renames project fastq directories: adds the date, machine name and flowcell
    index (A or B) to the name of the project directories.
    
    Returns the mapping of project names (mangled for the sample sheet) to the 
    renamed directories."""


    projects = set(sam['SampleProject'] for sam in sample_sheet)
    projdir = {}
    for pro in projects:
        original = os.path.join(unaligned_dir, "Project_" + pro)
        rename_to = os.path.join(unaligned_dir, parse.get_hiseq_project_dir(runid, pro))
        os.rename(original, rename_to)
        projdir[pro] = rename_to

    return projdir


def check_fastq_and_attach_files(process, sample_sheet, projdirs, reads):
    """Attaches ResultFile outputs of the HiSeq demultiplexing process."""

    for sam in sample_sheet:
        sample_dir = "Sample_" + sam['SampleID']
        fastq_names = ["{0}_{1}_L{2}_{3}_001.fastq.gz".format(sam['SampleID'],
            sam['Index'], sam['Lane'].zfill(3), r) for r in reads]
        fastq_paths = [os.path.join(projdirs[sam['SampleProject']], 
            sample_dir, fq) for fq in fastq_names]

        for fp in fastq_paths:
            # Continues even if file doesn't exist. This will be discovered
            # in other ways, preferring "robust" operation here.
            if os.path.exists(fp):
                # The convention is to have the LIMS ID in the description field. If this fails, 
                # there's not a lot more we can do, so the following line just crashes with an 
                # exception (HTTP 404).
                result_file_artifact = demultiplex.lookup_outfile(process, sam['Description'], sam['Lane'])
                pf = ProtoFile(nsc.lims, result_file_artifact.uri, fp)
                pf = nsc.lims.glsstorage(pf)
                f = pf.post()
                f.upload(fp) # content of the file is the path


def copy_to_secondary():
    """Copy to secondary storage if required."""

    destination = os.path.join(nsc.SECONDARY_STORAGE, runid)
    if nsc.DO_COPY_METADATA_FILES:
        already_existed = True
        try:
            os.mkdir(destination)
            already_existed = False
        except OSError:
            pass

        if not already_existed:
            if not copyfiles.copy_files(process, 'nextseq'):
                utilities.fail(process, 'Unable to copy files')
                return False


def get_sample_sheet(process, run_dir):
    """Get sample sheet from LIMS, fall back to SampleSheet.csv in run
    directory"""

    ssheet_file, sample_sheet = download_sample_sheet(process, run_dir)
    if ssheet_file:
        return ssheet_file, sample_sheet
    else:
        path = os.path.join(run_dir, "SampleSheet.csv")
        data = open(path).read()
        return path, data 



def main(process_id):
    os.umask(770)
    process = Process(nsc.lims, id=process_id)

    utilities.running(process)
    cfg = get_config(process)
    
    seq_proc = utilities.get_sequencing_process(process)
    runid = seq_proc.udf['Run ID']

    print "Demultiplexing process for LIMS process", process_id, ", NextSeq run", runid
    copy_to_secondary()

    success = False
    if cfg:
        start_dir = os.path.join(cfg.run_dir, "Data", "Intensities", "BaseCalls")
    
        ssheet_file, sample_sheet_data = get_sample_sheet.download_sample_sheet(
                process, cfg.run_dir
                )

        sample_sheet = parse.parse_hiseq_sample_sheet(sample_sheet_data)
    
        if ssheet_file:
            process_ok = run_demultiplexing(process, ssheet_file, cfg.bases_mask,
                    cfg.n_threads, cfg.mismatches, start_dir, cfg.dest_dir,
                    cfg.other_options)
            if process_ok:
                projdirs = rename_project_directories(runid, cfg.dest_dir, sample_sheet)
                reads = ["R1"]
                try:
                    if seq_proc.udf['Read 2 Cycles']:
                        reads.append("R2")
                except KeyError:
                    pass
                check_fastq_and_attach_files(process, sample_sheet, projdirs, reads)
                try:
                    success = demultiplex.populate_results(process, cfg.dest_dir)

                except (IOError,KeyError):
                    success = False

                if not success:
                    utilities.fail(process, "Failed to set UDFs")
            else: # Processing (make, etc)
                utilities.fail(process, "Demultiplexing process exited with an error status")
            
        else: # Sample sheet
            utilities.fail(process, "Can't get the sample sheet")

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


