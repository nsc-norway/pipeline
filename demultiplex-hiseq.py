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
import shutil
from common import nsc, utilities, demultiplex, parse, copyfiles

class Config:
    pass

def get_config(process):
    """Configuration is stored in UDFs on the demultiplexing process. This
    function loads them into a generic object."""

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


def run_demultiplexing(process, ssheet, bases_mask, n_threads, mismatches,
        start_dir, dest_run_dir, other_options):
    """First calls the configureFastqToBcl.py, then calls make in the fastq file directory."""

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



def make_id_resultfile_map(process, sample_sheet_data, reads):
    themap = {}
    lanes = set(int(entry['Lane']) for entry in sample_sheet_data)

    for entry in sample_sheet_data:
        lane = entry['Lane']
        lane_location = lane + ":1"
        id = entry['SampleID']
        input_limsid = entry['Description']
        sample = Artifact(nsc.lims, id=input_limsid).samples[0]
        
        for input,output in process.input_output_maps:
            # note: "uri" indexes refer to the entities themselves
            if input['uri'].location[1] == lane_id:
                if input['uri'].samples[0].id == sample.id:
                    for read in reads:
                        if output['uri'].name == nsc.HISEQ_FASTQ_OUTPUT.format(
                                sample.name, lane, read
                                ):
                            themap[(int(lane), id, read)] = output['uri']

    return themap



def check_fastq_and_attach_files(id_resultfile_map, sample_sheet, projdirs, reads):
    """Attaches ResultFile outputs of the HiSeq demultiplexing process."""

    for sam in sample_sheet:
        sample_dir = "Sample_" + sam['SampleID']

        for r in reads:
            fastq_name = ["{0}_{1}_L{2}_R{3}_001.fastq.gz".format(sam['SampleID'],
                sam['Index'], sam['Lane'].zfill(3), r)]
            fastq_path = os.path.join(projdirs[sam['SampleProject']], sample_dir, fq)

            # Continues even if file doesn't exist. This will be discovered
            # in other ways, preferring "robust" operation here.
            if os.path.exists(fastq_path):
                # The convention is to have the LIMS ID in the description field. If this fails, 
                # there's not a lot more we can do, so the following line just crashes with an 
                # exception (due to HTTP 404).
                result_file_artifact = id_resultfile_map[(int(sam['Lane']), sam['Description'], r)]
                pf = ProtoFile(nsc.lims, result_file_artifact, fastq_path)
                pf = nsc.lims.glsstorage(pf)
                f = pf.post()
                f.upload(fastq_path) # content of the file is the path


def populate_results(process, ids_analyte_map, demultiplex_stats):
    """Set UDFs on inputs (analytes representing the lanes) and output
    files (each fastq file).
    """
    inputs = dict((i.location[0], i) for i in process.all_inputs(unique=True))
    if len(set(i.location[1] for i in inputs)) != 1:
        print "error: Wrong number of flowcells detected"
        return

    for coordinates, stats in demultiplex_stats.items():
        lane, sample_name, read = coordinates
        lims_fastqfile = None
        try:
            lims_fastqfile = ids_analyte_map[(lane, sample_name, read)]
        except KeyError:
            undetermined = not not re.match(r"lane\d$", sample_name)

        if lims_fastqfile:
            lims_fastqfile.get()
            for statname in demultiplex.udf_list:
                try:
                    lims_fastqfile.udf[statname] = stats[statname]
                except KeyError:
                    pass
            lims_fastqfile.put()
    
        elif undetermined:
            analyte = inputs["{0}:1".format(sample_lane['Lane'])]
            analyte.udf[nsc.LANE_UNDETERMINED_UDF] = stats['% of PF Clusters Per Lane']
            analyte.put()



def main(process_id):
    os.umask(007)
    process = Process(nsc.lims, id=process_id)

    utilities.running(process)
    cfg = get_config(process)
    
    seq_proc = utilities.get_sequencing_process(process)
    runid = seq_proc.udf['Run ID']
    print "Demultiplexing job for LIMS process", process_id, ", run", runid
    destination = os.path.join(nsc.SECONDARY_STORAGE, runid)

    if nsc.DO_COPY_METADATA_FILES:
        already_existed = True
        try:
            os.mkdir(destination)
            already_existed = False
        except OSError:
            pass

        if not already_existed:
            if not copyfiles.copy_files(process, 'hiseq'):
                utilities.fail(process, 'Unable to copy files')
                return

    success = False
    if cfg:
        start_dir = os.path.join(cfg.run_dir, "Data", "Intensities", "BaseCalls")
    
        ssheet_file,sample_sheet_data = demultiplex.download_sample_sheet(process, start_dir)
        sample_sheet = parse.parse_hiseq_sample_sheet(sample_sheet_data)
    
        if ssheet_file:
            process_ok = run_demultiplexing(process, ssheet_file, cfg.bases_mask,
                    cfg.n_threads, cfg.mismatches, start_dir, cfg.dest_dir,
                    cfg.other_options)
            if process_ok:
                projdirs = rename_project_directories(runid, cfg.dest_dir, sample_sheet)
                reads = [1]
                try:
                    if seq_proc.udf['Read 2 Cycles']:
                        reads.append(2)
                except KeyError:
                    pass
                id_resultfile_map = make_id_resultfile_map(
                        process, sample_sheet, reads
                        )
                check_fastq_and_attach_files(
                        id_resultfile_map, sample_sheet, projdirs, reads
                        )

                # Demultiplexing stats
                demux_stats_path = parse.get_hiseq_stats(os.path.join(
                    cfg.dest_dir, "Basecall_Stats_" + sample_sheet[0]['FCID'], 
                    "Demultiplex_Stats.htm"
                    ))
                utilities.upload_file(process, "Demultiplex_stats.htm", path = demux_stats_path)
                fc_demux_summary_path = parse.get_hiseq_stats(os.path.join(
                    cfg.dest_dir, "Basecall_Stats_" + sample_sheet[0]['FCID'], 
                    "Flowcell_demux_summary.xml"
                    ))
                demultiplex_stats = parse.get_hiseq_stats(fc_demux_summary_path) 
                populate_results(process, cfg.dest_dir)

                success = True

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
        print "use: demultiplex-hiseq.py <process-id>"
        sys.exit(1)


