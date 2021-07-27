# Prepare data for delivery. Performs various actions depending on the 
# delivery type of the project.
# - portable hard drive -> Hard-links to delivery/ dir on secondary storage
# - diagnostics -> Copies run to diagnostics area, sets permissions
# - norstore -> tars the project, computes md5 of tar, saves the tar and md5 
#               in delivery/

import sys
import os
import re
import shutil
import crypt
import time
import subprocess
import datetime
import glob
import time
import requests
import demultiplex_stats
from genologics.lims import *
from common import nsc
from common import utilities, taskmgr, remote, samples

TASK_NAME = "90. Prepare delivery"
TASK_DESCRIPTION = """Prepare for delivery."""
TASK_ARGS = ['work_dir', 'sample_sheet', 'lanes']

if nsc.TAG == "prod":
    from common import secure
else:
    sys.stderr.write("Using dummy security module\n")
    from common import secure_dummy as secure

# Wait time in seconds after triggering the processing of a project
COVID_PROJECT_DELAY = 10


def delivery_16s(task, project, lims_project, delivery_method, basecalls_dir, project_path):
    """Special delivery method for demultiplexing internal 16S barcodes."""

    assert task.process is not None, "delivery_16s can only run with LIMS mode."

    lims_param_dir = os.path.join(project_path, "lims_parameters")
    try:
        os.mkdir(lims_param_dir)
    except OSError as e:
        if e.errno == 17:
            pass
        else:
            raise e
    sample_sheet_file, sample_metadata_file, parameter_file = [
            os.path.join(lims_param_dir, filename)
            for filename in ["16SSampleSheet.tsv", "sample-metadata.tsv", "params.csv"]
            ]

    outputs = task.lims.get_batch(o['uri'] for i,o in task.process.input_output_maps
                                    if o['output-generation-type'] == "PerReagentLabel")
    with open(sample_sheet_file, "w") as f:
        for output in outputs:
            reagent = next(iter(output.reagent_labels))
            m = re.match(r"16S_... \((.*)-(.*)\)", reagent)
            if m:
                bc1, bc2 = m.groups((1,2))
            else:
                bc1 = ""
                bc2 = ""
            sample_name = re.sub(r"^\d+-", "", output.name)
            f.write("\t".join((sample_name, bc1, bc2)) + "\n")

    with open(sample_metadata_file, "w") as f:
        f.write("\t".join(["sample-id", "nsc-sample-number", "nsc-prep-batch", "nsc-row", "nsc-column"]) + "\n")
        prep_batches = dict()
        for output in outputs:
            match = re.match(r"^(\d+)-(.*)$", output.name)
            if match:
                sample_number, sample_name = match.groups()
            else:
                sample_number, sample_name = "X", output.name
            process = task.process
            label = next(iter(output.reagent_labels))
            while process and not (process.type_name.startswith("16S") and 'Sample prep' in process.type_name):
                inputs = [i['uri'] for i, o in process.input_output_maps if o['uri'].id == output.id]
                if len(inputs) == 1:
                    input = inputs[0]
                else:
                    try:
                        input = next(input for input in inputs if next(iter(input.reagent_labels)) == label)
                    except StopIteration:
                        task.warn("Unable to find ancestor artifact for {} at process {} (number of inputs: {})".format(
                            sample_name, process.id, len(inputs)))
                        process = None
                        break
                process = input.parent_process
                output = input
            if process:
                container = output.location[0]
                try:
                    i_batch = prep_batches[container.id]
                except KeyError:
                    i_batch = len(prep_batches) + 1
                    prep_batches[container.id] = i_batch
                batch, row, col = [str(i_batch)] + output.location[1].split(":")
            else:
                batch, row, col = "NA", "", ""
            f.write("\t".join([sample_name, sample_number, batch, row, col]) + "\n")


    with open(parameter_file, "w") as f:
        ps  =    [["RunID",     task.run_id]]
        ps.append(["bcl2fastq", utilities.get_bcl2fastq2_version(task.process, task.work_dir)])
        ps.append(["RTA",       utilities.get_rta_version(task.work_dir)])
        ps.append(["DeliveryMethod",delivery_method])
        ps.append(["ProjectName",project.name])
        f.write("\n".join(",".join(p) for p in ps) + "\n")

    if delivery_method == "Norstore": 
        project_dir = os.path.basename(project_path).rstrip("/")
        create_htaccess_files(task.process, project.name, project_dir, lims_param_dir)
    
    seq_process = utilities.get_sequencing_process(task.process)
    lims_info = utilities.LimsInfo(lims_project, seq_process)
    #if lims_info.total_number_of_lanes == 1 + lims_info.status_map('COMPLETED', 0):
        # All runs have been completed for this project
    subprocess.call(["/data/runScratch.boston/scripts/run-16s-sbatch.sh", project_path])


def delivery_diag_link(task, project, basecalls_dir, project_path):
    """Special delivery method for diagnostics at OUS"""

    dest_dir = os.path.join(
            nsc.DIAGNOSTICS_DELIVERY,
            os.path.basename(project_path)
            )

    cp_args = ['cp', '-rl']

    if os.path.exists(dest_dir):
        raise RuntimeError("Destination directory '" + dest_dir + "' already exists")
    args = cp_args + [project_path.rstrip("/"), nsc.DIAGNOSTICS_DELIVERY]
    subprocess.check_call(args)

    # Now copy quality control data. Make real copy, not links for qc files, as we change
    # the permissions of the transferred data independently of the source files in the NSC
    # area.
    cp_copy_args = ['cp', '-r']
    # Diagnostics wants the QC info in a particular format (file names, etc.). Do not
    # change without consultiing with them. 
    source_qc_dir = os.path.join(basecalls_dir, "QualityControl" + task.suffix)

    qc_dir = os.path.join(dest_dir, "QualityControl")
    os.mkdir(qc_dir)

    for subdir in ["Stats" + task.suffix, "Reports" + task.suffix]:
        source = os.path.join(basecalls_dir, subdir)
        subprocess.call(cp_copy_args + [source, dest_dir])

    # The locations of the fastqc directories are defined by the get_fastqc_dir() 
    # function in the samples module. These directories will then be moved to a 
    # fixed hierarchy by the following code, so the diag delivery structure is
    # decoupled from how we store it in the run folder. 
    
    # This doesn't sanitise the fastqc directory names, which depend on the fastq 
    # file names. The fastq file names and directories should be kept consistent
    # not just for diagnostics but for other users, so this shouldn't be a problem.
    for sample in project.samples:
        sample_dir = os.path.join(qc_dir, "Sample_" + sample.name)
        if not os.path.exists(sample_dir):
            os.mkdir(sample_dir)

        if task.instrument in ['hiseqx', 'hiseq4k', 'novaseq']:
            source = os.path.join(source_qc_dir, samples.get_fastdup_path(project, sample, sample.files[0]))
            if os.path.exists(source):
                fdp_name = re.sub(r".fastq.gz$", "_fastdup.txt", sample.files[0].filename)
                dest = os.path.join(sample_dir, fdp_name)
                if os.path.exists(dest):
                    shutil.rmtree(dest)
                subprocess.check_call(cp_args + [source, dest])

        for f in sample.files:
            source = os.path.join(source_qc_dir, samples.get_fastqc_dir(project, sample, f) + "/")
            if os.path.exists(source):
                fqc_name = re.sub(r".fastq.gz$", "_fastqc/", f.filename)
                dest = os.path.join(sample_dir, fqc_name)
                if os.path.exists(dest):
                    shutil.rmtree(dest)
                subprocess.check_call(cp_args + [source, dest])

    # Get the demultiplex stats for diag. We generate a HTML file in the same 
    # format as that used by the first version of bcl2fastq.
    fcid = utilities.get_fcid_by_runid(task.run_id)
    bcl2fastq_version = utilities.get_bcl2fastq2_version(task.process, task.work_dir)
    undetermined_project = next(p for p in task.projects if p.is_undetermined)
    demultiplex_stats_content = demultiplex_stats.demultiplex_stats(
            project, undetermined_project, task.work_dir, basecalls_dir, task.instrument,
            task.no_lane_splitting, fcid, bcl2fastq_version, task.suffix
            )
    with open(os.path.join(dest_dir, "Demultiplex_Stats.htm"), 'w') as f:
        f.write(demultiplex_stats_content)

    subprocess.check_call(["chmod", "-R", "ug+rwX,o-rwx", os.path.join(nsc.DIAGNOSTICS_DELIVERY, dest_dir)])
    subprocess.check_call(["chgrp", "-R", "ous-diag-delv", os.path.join(nsc.DIAGNOSTICS_DELIVERY, dest_dir)])

    # Check undetermined percentage, then close the sequencing (Mi/NextSeq) or Data QC (NovaSeq) step.
    # ---- Automation workflow for diagnostics projects ----
    # 1. auto-next-script.sh cron job (lims repo) checks:
    #    - sequencing step completed (NovaSeq) or Finish Date set (others)
    # 2. Triggers the LIMS automations one by one (10. Copy Run, 20...., 90 This One)
    # 3. This automation then goes and closes the QC (NovaSeq) / Sequencing (others) step (see code below)
    # 4. The auto-next-script.sh cron job then sees that it is closed, and all jobs are completed, so
    #    it finishes the demultiplexing step too.
    if task.process: # Only in LIMS mode
        qc_proc = utilities.get_sequencing_process(task.process, qc=True)
        if qc_proc:
            qc_step = Step(qc_proc.lims, id=qc_proc.id)
            if qc_step.current_state.upper() != "COMPLETED":
                task.info("Completing the Sequencing QC step...")
                # The step may have been completed already, if there's another project.
                # This will only process if it's not completed.
                if any(i.udf.get(nsc.LANE_UNDETERMINED_UDF, 0) > 50 for i in qc_proc.all_inputs()):
                    task.fail("Undetermined indices > 50 %. To continue anyway, manually complete" +
                        "the Sequencing and Demultiplexing steps.")
                # NovaSeq -- QC flags are on outputs of the QC step
                qcs = [o['uri'] for i, o in qc_proc.input_output_maps if o['output-generation-type'] == 'PerInput']
                if not qcs:
                    # Others -- QC flags are on inputs
                    qcs = qc_proc.all_inputs(unique=True)
                for qc in qcs:
                    qc.qc_flag = "PASSED"
                qc_proc.lims.put_batch(qcs)
                while qc_step.current_state.upper() != "COMPLETED":
                    error_object = None
                    for _ in range(3):
                        try:
                            qc_step.advance()
                            break
                        except requests.exceptions.HTTPError as e:
                            task.info("Completing sequencing / QC step - waiting for LIMS script...")
                            error_object = e
                            time.sleep(30)
                    else:
                        task.fail("Failed to advance the Sequencing / Data QC step due to an HTTP error in the API "
                                  "(after multiple retries).", repr(error_object))
                    time.sleep(5)
                    qc_step.get(force=True)


def copy_sav_files(task, dest_dir, srun_user_args=[]):
    # Copy "SAV" files for advanced users
    if task.instrument == "nextseq":
        sav_include_paths = [
            "RunInfo.xml",
            "RunParameters.xml",
            "InterOp",
            ]
    else:
        sav_include_paths = [
            "RunInfo.xml",
            "runParameters.xml",
            "InterOp",
            ]
    demultiplexing_sample_sheets = glob.glob(os.path.join(task.work_dir, "DemultiplexingSampleSheet*.csv"))
    if demultiplexing_sample_sheets:
        sav_include_paths.append(os.path.relpath(sorted(demultiplexing_sample_sheets)[-1], task.work_dir))
    rsync_cmd = [nsc.RSYNC, '-r']
    rsync_cmd += sav_include_paths
    rsync_cmd += [os.path.join(dest_dir, task.run_id) + "/"]
    rcode = remote.run_command(rsync_cmd, task, "rsync_sav_files", time="1:00:00",
            srun_user_args=srun_user_args, cwd=task.work_dir, comment=task.run_id)
    # Rsync error code is ignored, failure here is not fatal.


def delivery_external_user(task, lims_project, project_path, delivery_path):
    """Link the fastq files, close LIMS project, and copy SAV data if specified"""

    dest_dir = os.path.join(delivery_path, os.path.basename(project_path))
    subprocess.check_call(["/bin/cp", "-rl", project_path, dest_dir])
    lims_project.close_date = datetime.date.today()
    lims_project.put()
    copy_sav_files(task, dest_dir)


def delivery_harddrive(project_name, source_path):
    # Copy to delivery area
    subprocess.check_call(["/bin/cp", "-rl", source_path, nsc.DELIVERY_DIR])
    #log_path = task.logfile("rsync-" + project_name)
    #args = [nsc.RSYNC, '-rlt', '--chmod=ug+rwX,o-rwx'] # chmod 660
    #args += [source_path.rstrip("/"), nsc.DELIVERY_DIR]
    #rcode = remote.run_command(args, task,  "delivery_hdd", "04:00:00", logfile=log_path)
    #if rcode != 0:
    #    raise RuntimeError("Copying files to loki failed, rsync returned an error")


def create_htaccess_files(process, project_name, project_dir, save_path):
    # Generate username / password files
    match = re.match("^([^-]+)-([^-]+)-\d\d\d\d-\d\d-\d\d$", project_name)
    if match:
        name = match.group(1)
        proj_type = match.group(2)
        username = name.lower() + "-" + proj_type.lower()
        password = secure.get_norstore_password(process, project_name)
    else:
        username = "invalid"
        password = "invalid"
    crypt_pw = crypt.crypt(password)
    
    htaccess = """\
AuthUserFile /data/{project_dir}/.htpasswd
AuthGroupFile /dev/null
AuthName ByPassword
AuthType Basic

<Limit GET>
require user {username}
</Limit>
    """.format(project_dir=project_dir, username=username)
    open(save_path + "/.htaccess", "w").write(htaccess)

    htpasswd = "{username}:{crypt_pw}\n".format(username=username, crypt_pw=crypt_pw)
    open(save_path + "/.htpasswd", "w").write(htpasswd)


def delivery_norstore(process, project_name, source_path, task):
    """Create a tar file"""

    project_dir = os.path.basename(source_path).rstrip("/")
    save_path = os.path.join(nsc.DELIVERY_DIR, project_dir)
    try:
        os.mkdir(save_path)
    except OSError:
        pass
    tarname = project_dir + ".tar"
    args = ["/bin/tar", "cf", save_path + "/" + tarname , project_dir]
    rcode = remote.run_command(
            args, task, "tar", "04:00:00",
            cwd=os.path.dirname(source_path),
            comment=task.run_id
            ) # dirname = parent dir
    if rcode != 0:
        raise RuntimeError('Failed to run "tar" to prepare for Norstore delivery')

    md5_path = os.path.join(save_path + "/md5sum.txt")
    # would use normal md5sum, but we have md5deep as a dependency already
    rcode = remote.run_command(
            nsc.MD5 + [tarname], task,
            "md5deep_tar", "08:00:00", cwd=save_path, stdout=md5_path,
            comment=task.run_id
            )
    if rcode != 0:
        raise RuntimeError("Failed to compute checksum for tar file for Norstore, "+
                "md5deep returned an error")

    try:
        create_htaccess_files(process, project_name, project_dir, save_path) 
    except Exception as e:
        task.warn("Password generation failed: " + str(e))


def fhi_mik_seq_delivery(task, project_type, project, lims_project, lims_process, lims_samples, project_path, delivery_base_dir):
    task.info("Preparing data and scripts for {}...".format(project.name))

    if not re.match(r"[A-Za-z_0-9-]+$", project.name):
        task.fail("fhi_mik_seq_delivery: Unacceptable project name {}.".format(project.name))

    #### PROJECT-RELATED PARAMETERS #####
    proj_dir_name = os.path.basename(project_path)
    output_path = os.path.join(delivery_base_dir, project.name)

    os.mkdir(output_path)

    #### SAMPLE LIST ####
    covid_seq_write_sample_list(task, project, lims_project, lims_process, lims_samples,
                         os.path.join(output_path, "sampleList.csv"),
                         os.path.join(output_path, "extendedSampleList.csv"))

    #### RUN viralrecon ####
    subprocess.check_call(["/bin/cp", "-rl", project_path, delivery_base_dir])
    script1 = """#!/bin/bash
set -e

"""
    if project_type == "MIK-Covid19":
        script1 += """
(cd .. && cp -rl {fastq_dir} for_MIK_IronKey_2)
"""
    script1 += """

# Make tar file with fastqs in parallel with everything else
( cd .. && tar cf for_FHI_TSD_2/{fastq_dir}.tar {fastq_dir} && cd for_FHI_TSD_2/ && md5sum {fastq_dir}.tar > {fastq_dir}.tar.md5 ) &

# Run workflow
/data/common/tools/nscbin/nextflow run /boston/runScratch/analysis/pipelines/2021_covid19/nsc_pipeline_v8/main.nf \\
    --outpath "$PWD" \\
    --samplelist sampleList.csv \\
    --align_tool "bowtie2" \\
    -resume > pipeline_log.txt


# Prepare delivery files
cd ..

# Variants
tar cf for_FHI_TSD_1/{analysis_dir}_variants.tar {analysis_dir}/results/*.tsv {analysis_dir}/pipeline_report_log.txt {analysis_dir}/results/4_consensus/ivar/ {analysis_dir}/results/3_variants/ivar/ {analysis_dir}/results/9_QC/
( cd for_FHI_TSD_1/ && md5sum {analysis_dir}_variants.tar > {analysis_dir}_variants.tar.md5 )

# Need to clean up before making analysis tar file / copy
rm -r {analysis_dir}/work {analysis_dir}/.nextflow*

"""
    if project_type == "MIK-Covid19":
        script1 += """
cp -rl {analysis_dir} for_MIK_IronKey_1
"""
    script1 += """
tar cf for_FHI_TSD_2/{analysis_dir}.tar {analysis_dir}
( cd for_FHI_TSD_2/ && md5sum {analysis_dir}.tar > {analysis_dir}.tar.md5 )

wait
"""
    script = script1.format(analysis_dir=project.name, fastq_dir=proj_dir_name)

    script_file = os.path.join(output_path, "script.sh")
    log_file = os.path.join(output_path, "control_job_log.txt")
    open(script_file, "w").write(script)
    task.info("Starting analysis for {}...".format(project.name))
    subprocess.check_call(
            ["sbatch",
                    "-J", "C_" + project.name,
                    "-o", log_file,
                    "--mem", "16G",
                    "--cpus-per-task", "4",
                    "--wrap", "bash " + script_file],
            cwd=output_path)
    task.info("Waiting {} seconds after starting {}...".format(COVID_PROJECT_DELAY, project.name))
    time.sleep(COVID_PROJECT_DELAY)


def covid_seq_write_sample_list(task, project, lims_project, lims_process, lims_samples,
                output_sample_list_path, output_ext_sample_list_path):
    """Create sample table, used to drive the nextflow-based analysis pipelines."""

    lims_demux_io = [(i['uri'], o['uri']) for i, o in lims_process.input_output_maps
                    if o['output-type'] == "ResultFile" and o['output-generation-type'] == "PerReagentLabel"]

    lims_sample_map = dict((s.name, s) for s in lims_samples)

    # Primary details contains tuples of (name,r1path,r2path,well)
    sample_details_rows = []

    for sample in project.samples:
        # Multiple runs is not supported here
        r1files = [file for file in sample.files if file.i_read == 1]
        if len(r1files) != 1: task.fail("Only supports 1 file pair per sample.")

        lims_sample = lims_sample_map.get(sample.name)
        # Find LIMS demultiplexing information
        lims_demux_pairs = [(i,o) for i, o in lims_demux_io if o.samples[0].id == lims_sample.id]
        if len(lims_demux_pairs) != 1:
            task.fail("Found {} input/output pairs in lims demux step for sample {}, expected 1.".format(
                len(lims_demux_pairs)),
                lims_sample.name
                )
        lims_lane, lims_demuxfile = lims_demux_pairs[0]

        r1path = "../" + r1files[0].path
        r2path = re.sub(r"_R1_001.fastq.gz$", "_R2_001.fastq.gz", r1path)

        # Define sample-level details (shared between all lanes)
        sample_details = [
            ('sample',          sample.name),
            ('Well',            lims_sample.artifact.location[1].replace(":", "")),
            ('OrigCtValue',     lims_sample.udf.get('Org. Ct value', 'NA')),
            ('ProjectName',  project.name),
            ('SeqRunId',     task.run_id),
            ('SequencerType', task.instrument),
            ('fastq_1',         r1path),
            ('fastq_2',         r2path),
            ('MIKInputCols',    lims_sample.udf.get('Additional columns (MIK)', '')),
        ]
        sample_details_rows.append(sample_details)

    headers = [header for header, value in sample_details_rows[0]]
    well_col = headers.index("Well")
    def well_col_order(row):
        return (int(row[well_col][1:]), row[well_col][0])
    string_data_rows_cells = sorted([
                    [str(value) for header, value in sam]
                    for sam in sample_details_rows
    ], key=well_col_order)
    with open(output_sample_list_path, "w") as of:
        of.write(",".join(headers) + "\n")
        for row_cells in string_data_rows_cells:
            of.write(",".join(row_cells) + "\n")


def main(task):
    task.running()

    lims_projects = {}
    if not os.path.isdir(nsc.DELIVERY_DIR):
        task.fail("Delivery directory {0} does not exist.".format(nsc.DELIVERY_DIR))

    if task.process:
        inputs = task.process.all_inputs(unique=True, resolve=True)
        l_samples = task.lims.get_batch(set(sample for i in inputs for sample in i.samples))
        lims_projects = dict(
                (utilities.get_sample_sheet_proj_name(sample.project.name), sample.project)
                for sample in l_samples
                if sample.project
                )
    else:
        lims_projects = {}

    runid = task.run_id
    projects = list(project for project in task.projects if not project.is_undetermined)
    samples.add_index_read_files(projects, task.work_dir)

    sensitive_fail = []
    for project in projects:
        project_path = os.path.join(task.bc_dir, project.proj_dir)

        lims_project = lims_projects.get(project.name)
        if lims_project:
            delivery_type = lims_project.udf[nsc.DELIVERY_METHOD_UDF]
            project_type = lims_project.udf[nsc.PROJECT_TYPE_UDF]
            project_16s = lims_project.udf.get(nsc.PROJECT_16S_UDF)
        elif not task.process:
            delivery_type = nsc.DEFAULT_DELIVERY_MODE
            if delivery_type is None:
                continue
            project_type = "Non-Sensitive"
            if project.name.startswith("Diag-"):
                project_type = "Diagnostics"
            project_16s = False
        else:
            task.warn("Project " + project.name + " is missing delivery information!")
            continue
        if project_16s: # Only supported for LIMS mode...
            task.info("Running 16S delivery for " + project.name + "...")
            delivery_16s(task, project, lims_project, delivery_type, task.bc_dir, project_path)
        elif project_type == "Diagnostics" or delivery_type == "Transfer to diagnostics":
            task.info("Linking " + project.name + " to diagnostics...")
            delivery_diag_link(task, project, task.bc_dir, project_path)
        elif project_type == "Immunology":
            delivery_external_user(task, lims_project, project_path, "/data/runScratch.boston/imm_data")
        elif project_type == "Microbiology":
            delivery_external_user(task, lims_project, project_path, "/data/runScratch.boston/mik_data")
        elif project_type == "FHI-Covid19": # Implicitly requires LIMS mode (or we wouldn't have project_type)
            lims_samples = [s for s in l_samples if s.project == lims_project]
            fhi_mik_seq_delivery(task, project_type, project, lims_project, task.process, lims_samples, project_path, "/data/runScratch.boston/analysis/covid")
        elif project_type == "MIK-Covid19":
            lims_samples = [s for s in l_samples if s.project == lims_project]
            fhi_mik_seq_delivery(task, project_type, project, lims_project, task.process, lims_samples, project_path, "/data/runScratch.boston/analysis/covid")
        elif delivery_type in ["User HDD", "New HDD", "TSD project"]:
            task.info("Hard-linking " + project.name + " to delivery area...")
            delivery_harddrive(project.name, project_path)
        elif delivery_type == "NeLS project":
            task.info("Hard-linking " + project.name + " to delivery area (NeLS)...")
            if project_type != "Non-Sensitive":
                sensitive_fail.append(project.name)
                continue
            delivery_harddrive(project.name, project_path)
        elif delivery_type == "Norstore":
            if project_type != "Non-Sensitive":
                sensitive_fail.append(project.name)
                continue
            task.info("Tar'ing and copying " + project.name + " to delivery area, for Norstore...")
            delivery_norstore(task.process, project.name, project_path, task)
        else:
            print "No delivery prep done for project", project.name

    if sensitive_fail:
        task.fail("Selected Internet-based delivery for sensitive data, nothing done for: " + ",".join(sensitive_fail))
    task.success_finish()


if __name__ == "__main__":
    with taskmgr.Task(TASK_NAME, TASK_DESCRIPTION, TASK_ARGS) as task:
        main(task)

