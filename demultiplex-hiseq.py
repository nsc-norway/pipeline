# Demultiplexing script for HiSeq

# This is the primary demultiplexing job. It is contolled by the setup-
# hiseq-demultiplexing script, through the use of UDFs on the process in the
# LIMS.

# Its primary data processing functionality is handled by:
# configureBclToFastq.pl
# make

# Functional overview:
# - Get options for demultiplexing 
# - Set running / error flag in UDF
# - Run demultiplexing
# - Upload result files and set UDFs on samples
# - Set job finished status
# - Finsih demultiplexing step if using automatic processing

import sys, os, re
import subprocess
from genologics.lims import *
import nsc
import utilities
import demultiplex

class Config:
    pass

def get_config(process):
    '''Configuration is stored in UDFs on the demultiplexing process. This
    function loads them into a generic object.'''

    try:
        cfg = Config()
        cfg.bases_mask = process.udf[nsc.BASES_MASK_UDF]
        cfg.n_threads = process.udf[nsc.THREADS_UDF]
        cfg.mismatches = process.udf[nsc.MISMATCHES_UDF]
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


def download_sample_sheet(process, save_dir):
    '''Downloads the demultiplexing process's sample sheet, which contains only
    samples for the requested project (written by setup-demultiplex-hiseq.py).'''

    sample_sheet = None
    for o in process.all_outputs(unique=True):
        if o.output_type == "ResultFile" and o.name == "SampleSheet csv":
            if len(o.files) == 1:
                sample_sheet = o.files[0].download()

    if sample_sheet:
        if process.id == "":
            raise ValueError("Process ID is an empty string")
        name = "SampleSheet-" + process.id + ".csv"
        file(os.path.join(save_dir, name), 'w').write(sample_sheet)
        return name, sample_sheet
    else:
        return False


def run_demultiplexing(process, ssheet, bases_mask, n_threads, mismatches,
        start_dir, dest_run_dir, other_options):
    '''First calls the configureFastqToBcl.py, then calls make in the fastq file directory.'''

    os.chdir(start_dir)
    cfg_log_name = os.path.join(nsc.LOG_DIR, "configureBclToFastq-" + process.id + ".log")
    log = open(cfg_log_name, "w")
    
    args = ['--mismatches', str(mismatches)]
    args += ['--fastq-cluster-count', "0"]
    args += ['--sample-sheet', ssheet]
    args += ['--use-bases-mask', bases_mask]
    args += ['--output-dir', dest_run_dir]
    args += ['--input-dir', start_dir]
    if other_options:
        args += re.split(" *", other_options)

    # configureBclToFastq.pl
    rcode = subprocess.call([nsc.CONFIGURE_BCL_TO_FASTQ] + args, stdout=log, stderr=log)

    log.close()
    utilities.upload_file(process, nsc.CONFIGURE_LOG, cfg_log_name)

    if rcode == 0:
        os.chdir(dest_run_dir)
        args = ['-j' + str(n_threads)]
        make_log_file = os.path.join(nsc.LOG_DIR, "make_" + process.id + ".log")
        log = open(make_log_file, "w")

        rcode = subprocess.call([nsc.MAKE] + args, stdout=log, stderr=log)
        log.close()

        utilities.upload_file(process, nsc.MAKE_LOG, make_log_file)

        if rcode == 0:
            return True
        else:
            utilities.fail(process, "Make failure (see log)")
    else:
        utilities.fail(process, "configureBclToFastq failure (see log)")
    
    return False


def parse_sample_sheet(sample_sheet):
    lines = sample_sheet.splitlines()
    headers = lines[0].split(",")
    samples = []
    for l in lines[1:]:
        sam = {}
        for h, v in zip(headers, l.split(",")):
            sam[h] = v
        samples.append(sam)

    return samples


def rename_project_directories(runid, unaligned_dir, sample_sheet):
    '''Renames project fastq directories: adds the date, machine name and flowcell
    index (A or B) to the name of the project directories.
    
    Returns the mapping of project names (mangled for the sample sheet) to the 
    renamed directories.'''

    date_machine_flowcell = re.match(r"([\d]+_[^_]+)_[\d]+_([AB])", runid)
    project_prefix = date_machine_flowcell.group(1) + "." + date_machine_flowcell.group(2) + "."

    projects = set(sam['SampleProject'] for sam in sample_sheet)
    projdir = {}
    for pro in projects:
        original = os.path.join(unaligned_dir, "Project_" + pro)
        rename_to = os.path.join(unaligned_dir, project_prefix + "Project_" + pro)
        os.rename(original, rename_to)
        projdir[pro] = rename_to

    return projdir


def check_fastq_and_attach_files(process, sample_sheet, projdirs, reads):
    '''Attaches ResultFile outputs of the HiSeq demultiplexing process.'''

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
                print "looking up analyte id ", sam['Description'], "lane", sam['Lane']
                result_file_artifact = demultiplex.lookup_outfile(process, sam['Description'],
                        sam['Lane'])
                pf = ProtoFile(nsc.lims, result_file_artifact.uri, fp)
                pf = nsc.lims.glsstorage(pf)
                f = pf.post()
                f.upload(fp) # content of the file is the path


def main(process_id):
    process = Process(nsc.lims, id=process_id)

    utilities.running(process)
    cfg = get_config(process)
    
    seq_proc = utilities.get_sequencing_process(process)
    runid = seq_proc.udf['Run ID']
    destination = os.path.join(nsc.SECONDARY_STORAGE, runid)
    try:
        os.mkdir(destination)
        # If directory didn't exist, we can call the copy files job
        if not copyfiles.copy_files(process, 'hiseq'):
            utilities.fail(process, 'Unable to copy files')
            return
    except OSError:
        pass # Already exists (or other errors we'll happily ignore)

    success = False
    if cfg:
        start_dir = os.path.join(cfg.run_dir, "Data", "Intensities", "BaseCalls")
    
        ssheet_file,sample_sheet_data = download_sample_sheet(process, start_dir)
        sample_sheet = parse_sample_sheet(sample_sheet_data)
    
        if ssheet_file:
            process_ok = True
            #process_ok = run_demultiplexing(process, ssheet_file, cfg.bases_mask,
            #        cfg.n_threads, cfg.mismatches, start_dir, cfg.dest_dir,
            #        cfg.other_options)
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
    # If failing, should have already notified of failure, just quit
    else:
        if not process.udf[nsc.JOB_STATUS_UDF].startswith('Fail'):
            utilities.fail(process, "Unknown failure")


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        try:
            main(sys.argv[1])
        except:
            process = Process(nsc.lims, id = sys.argv[1])
            utilities.fail(process, "Unexpected: " + str(sys.exc_info()[1]))
            raise
    else:
        print "use: demultiplex-hiseq.py <process-id>"
        sys.exit(1)


