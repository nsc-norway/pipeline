# Script to run off-board redemultiplexing of Novaseq X data
# It is called on the "Generate BCL Convert Samplesheet" step, and receives the process ID
# of this step as an argument.


import os
import sys
import re
import time
import csv
import yaml
import subprocess
import datetime
from pathlib import Path
from collections import defaultdict

from genologics.lims import *
from genologics import config


RUN_FOLDER_LOCATION = "/data/runScratch.boston/NovaSeqX"
CPU_BCL_CONVERT_CONTAINER_IMAGE = "/data/common/tools/bclconvert/bclconvert-4.3.6.sif"

def main():

    dev_env = len(sys.argv) > 2 and sys.argv[2] == "dev"
    if dev_env:
        config.BASEURI = "https://dev-lims.sequencing.uio.no"
        config.PASSWORD = open("/data/runScratch.boston/scripts/etc/seq-user/dev-apiuser-password.txt").read().strip()

    # Identification of demultiplexing parameters. Errors in this section are fatal and will
    # cause an immediate error message in LIMS.
    try:
        process_id = sys.argv[1]
    except IndexError:
        print("Usage: %s <process id>" % sys.argv[0])
        sys.exit(1)
    
    lims = Lims(config.BASEURI, config.USERNAME, config.PASSWORD)
    samplesheet_process = Process(lims, id=process_id)
    samplesheet_process.get() # Fail here if the process does not exist

    # Get the BCL Conversion process
    bcl_convert_process_id = samplesheet_process.udf.get("BCL Convert LIMS-ID")
    if not bcl_convert_process_id:
        print("ERROR: BCL Convert LIMS-ID is not set")
        sys.exit(1)
    bcl_convert_process = Process(lims, id=bcl_convert_process_id)
    bcl_convert_process.get()


    # The Run ID may be available on an earlier instance of the BCL Convert step,
    # on the NovaSeq X Run step, or it can be manually entered on the current
    # instance of the BCL Convert step. All these steps can be located by searching
    # for processes that have the same input artifacts as this samplesheet process.
    for process in lims.get_processes(inputartifactlimsid=[a.id for a in samplesheet_process.all_inputs(unique=True)]):
        run_id = process.udf.get("Run ID")
        if run_id:
            break
    else:
        print(f"Run ID was not found in related processes for the input artifacts. Run ID can be set manually on "
              f"BCL Convert step {bcl_convert_process_id}.")
        sys.exit(1)

    # Actualize and check all the parameters on the BCL Convert step
    compute_type = samplesheet_process.udf.get("BCL Convert Instrument")
    if not compute_type in ['External DRAGEN', 'CPU']:
        print(f"Unsupported compute type '{compute_type}' for off-board redemultiplexing")
        sys.exit(1)
    bcl_convert_process.udf["Compute platform"] = compute_type
    bcl_convert_process.udf["Run ID"] = run_id
    run_folder_udf = bcl_convert_process.udf.get('Sequencing Output Folder')
    if run_folder_udf:
        run_folder_path = Path(run_folder_udf)
    else:
        run_folder_path = Path(RUN_FOLDER_LOCATION) / run_id
        bcl_convert_process.udf['Sequencing Output Folder'] = str(run_folder_path)
    if not run_folder_path.exists():
        print(f"Run folder {run_folder_path} does not exist. You can override the Sequencing Output Folder on the BCL Convert step ({bcl_convert_process_id}).")
        sys.exit(1)
    data_compression_type = samplesheet_process.udf['FASTQ Compression Format']
    if data_compression_type == "dragen" and compute_type != "External DRAGEN":
        print("ERROR: Invalid configuration: 'dragen' compression is only available for External DRAGEN.")
        sys.exit(1)
    assert (run_folder_path / "Analysis").is_dir(), f"Analysis folder should exist at {run_folder_path / 'Analysis'}"

    analysis_id, analysis_path = get_analysis_id_and_path(bcl_convert_process, run_folder_path, compute_type)
    
    # Now we have created a directory (or reused one) so we can save this, and reuse it if
    # have to rerun this script later.
    bcl_convert_process.udf['Analysis ID'] = analysis_id
    bcl_convert_process.put()

    # Check for existing files - need to be deleted manually if clicking the button again
    # (Samplesheet - to avoid confusion about editing locally and overwriting samplesheet)
    samplesheet_name = "SampleSheet.csv"
    samplesheet_path = analysis_path / samplesheet_name
    if samplesheet_path.exists():
        print(f"Sample sheet already exists at {samplesheet_path}. FASTQ folder and sample sheet must be deleted to continue.")
        sys.exit(1)

    # Get output path. BCL Convert will crash if it exists, so we check this.
    output_folder_parent = analysis_path / "Data" / "BCLConvert"
    if data_compression_type == "dragen":
        output_folder = output_folder_parent / "ora_fastq"
    else:
        output_folder = output_folder_parent / "fastq"
    if output_folder.exists():
        print(f"Error: output folder {output_folder} already exists.")
        sys.exit(1)
    output_folder_parent.mkdir(exist_ok=True, parents=True)

    # Download the sample sheet data from LIMS
    for o in samplesheet_process.all_outputs(unique=True):
        if o.output_type == "ResultFile" and o.name == "SampleSheet":
            if len(o.files) == 1:
                sample_sheet = o.files[0].download()
                break
    else:
        print("Sample sheet data not found")
        sys.exit(1)
    
    # Check compression type
    compression_string = f"^FastqCompressionFormat,{data_compression_type}[\\n,]"
    if not re.search(compression_string, sample_sheet.decode(), flags=re.MULTILINE | re.IGNORECASE):
        print(f"Error: Compression type mismatch LIMS/SampleSheet: SampleSheet does not match: '{compression_string}'")
        sys.exit(1)

    # Store the sample sheet in the analysis folder
    with open(samplesheet_path, "wb") as f:
        f.write(sample_sheet)

    extra_options = samplesheet_process.udf.get("BCL Convert / DRAGEN command line options", "")

    # Create demultiplexing script file and run demultiplexing
    job_name = f"{analysis_id}.{run_id}"
    bcl_convert_process.udf['Status'] = "RUNNING"
    bcl_convert_process.put()
    print("RUNNING", job_name)
    if compute_type == "External DRAGEN":
        returncode = run_demultiplexing_dragen(job_name, run_folder_path, samplesheet_path, output_folder_parent, output_folder, extra_options)
    else:
        returncode = run_demultiplexing_cpu(job_name, run_folder_path, samplesheet_path, output_folder_parent, output_folder, extra_options)

    if returncode != 0:
        print("ERROR: BCL Conversion returned non-zero exit code", returncode, ".")
        bcl_convert_process.udf['Status'] = "FAILED"
        bcl_convert_process.put()
        sys.exit(1)
    if not (output_folder / "Logs" / "FastqComplete.txt").exists():
        print("ERROR: There is no FastqComplete file. BCL Convert probably failed.")
        bcl_convert_process.udf['Status'] = "FAILED"
        bcl_convert_process.put()
        sys.exit(1)


    # Import BCL convert / DRAGEN version and run ID
    bcl_convert_version = get_bclconvert_version(output_folder)

    # Import the demultiplexing results
    demultiplexed_lane_sample_info = parse_demultiplexing_stats(output_folder)
    
    add_project_info_from_lims(lims, demultiplexed_lane_sample_info)
    # Add common info
    for lane_sample_info in demultiplexed_lane_sample_info:
        lane_sample_info['ora_compression'] = (data_compression_type == "dragen")
    
    # Save the demultiplexing results and QC to LIMS, and put the artifact IDs in the sample info list.
    exchange_output_artifact_info(lims, bcl_convert_process, demultiplexed_lane_sample_info)
    add_undetermined_percent_to_lims(lims, samplesheet_process.all_inputs(), demultiplexed_lane_sample_info)

    # Create yaml file for downstream automation
    no_qc_no_unde_dlsi = [
        {k: v for k, v in row.items() if k != "qc"}
        for row in demultiplexed_lane_sample_info
        if row['samplesheet_sample_id'] != "Undetermined"
    ]
    with open(analysis_path / "ClarityLIMSImport_NSC.yaml", "w") as yamlout:
        yaml.dump({
            'status': 'ImportCompleted',
            'bcl_convert_version': bcl_convert_version,
            'compute_platform': compute_type,
            'demultiplexing_process_id': bcl_convert_process_id,
            'samples': no_qc_no_unde_dlsi
            }, yamlout)

    bcl_convert_process.udf['BCL Convert Version'] = bcl_convert_version
    bcl_convert_process.udf['LIMS import completed on'] = str(datetime.datetime.now())
    bcl_convert_process.udf['Status'] = "COMPLETED"
    bcl_convert_process.put()


def get_bclconvert_version(output_folder):
    with open(output_folder / "Logs" / "Info.log") as demultiplex_log:
        for line in demultiplex_log:
            m = re.search(r" SoftwareVersion = ([\d.]+)$", line)
            if m:
                return m.group(1)
    return "UNKNOWN"


def get_analysis_id_and_path(bcl_convert_process, run_folder_path, compute_type):
    """Select an analysis folder - will reuse an existing one or create a new one."""

    compute_type_code = {
        "External DRAGEN": "e",
        "CPU": "c",
    }[compute_type]
    analysis_id = bcl_convert_process.udf.get("Analysis ID")
    if not analysis_id or not analysis_id.startswith(compute_type_code): # Create new folder if none exist, or if wrong type
        previous_analyses = (run_folder_path / "Analysis").glob(f"{compute_type_code}*")
        try:
            previous_analysis_numbers = [int(analysis.name[len(compute_type_code):]) for analysis in previous_analyses if analysis.name[len(compute_type_code):].isdigit()]
            previous_analysis_number = max(previous_analysis_numbers) if previous_analysis_numbers else 0
        except ValueError:
            print(f"Error: Previous analysis {previous_analyses[-1]} does not have a numeric suffix")
            sys.exit(1)
        analysis_id = f"{compute_type_code}{previous_analysis_number + 1}"
        bcl_convert_process.udf["Analysis ID"] = analysis_id
    analysis_path = run_folder_path / "Analysis" / analysis_id
    analysis_path.mkdir(exist_ok=True)
    return analysis_id, analysis_path


def run_demultiplexing_dragen(job_name, run_folder_path, samplesheet_path, output_folder_parent, output_folder, extra_options):
    script_content = f"""#!/bin/bash
#SBATCH --job-name={job_name}
#SBATCH --partition=dragen
#SBATCH --time=24:00:00


/opt/edico/bin/dragen \\
        --bcl-input-directory {run_folder_path} \\
        --ora-reference /staging/projects/p22/reference/hg19/oradata_homo_sapiens \\
        --output-directory {output_folder} \\
        --bcl-sampleproject-subdirectories true \\
        --sample-sheet {samplesheet_path} {extra_options}
"""
    script_path = output_folder_parent / "script.sh"
    with open(script_path, "w") as script_file:
        script_file.write(script_content)
    result = subprocess.run(
            ['sbatch', '--wait', script_path],
            cwd=output_folder_parent
    )
    return result.returncode


def run_demultiplexing_cpu(job_name, run_folder_path, samplesheet_path, output_folder_parent, output_folder, extra_options):

    log_folder = output_folder_parent / "logs"
    log_folder.mkdir(exist_ok=True)
    script_content = f"""#!/bin/bash
#SBATCH --job-name={job_name}
#SBATCH --cpus-per-task=64
#SBATCH --mem=128G
#SBATCH --time=24:00:00
#SBATCH --qos=high

apptainer exec \\
    --bind {RUN_FOLDER_LOCATION} \\
    --bind {log_folder}:/var/log/bcl-convert \\
    {CPU_BCL_CONVERT_CONTAINER_IMAGE} \\
        bcl-convert \\
        --bcl-input-directory {run_folder_path} \\
        --output-directory {output_folder} \\
        --bcl-sampleproject-subdirectories true \\
        --sample-sheet {samplesheet_path} {extra_options}
"""
    script_path = output_folder_parent / "script.sh"
    with open(script_path, "w") as script_file:
        script_file.write(script_content)
    result = subprocess.run(
            ['sbatch', '--wait', script_path],
            cwd=output_folder_parent
    )
    return result.returncode
    

def parse_demultiplexing_stats(output_folder):
    with open(output_folder / "Reports" / "Demultiplex_Stats.csv", newline='') as f:
        demultiplex_stats = list(csv.DictReader(f))
    with open(output_folder / "Reports" / "Quality_Metrics.csv", newline='') as f:
        quality_metrics = list(csv.DictReader(f))
    
    demultiplexed_lane_sample_info = []
    sample_id_positions = {}
    # Compute lane-level aggregates
    lane_total_read_count = defaultdict(int)
    for demultiplex_stats_row in demultiplex_stats:
        lane_total_read_count[demultiplex_stats_row['Lane']] += float(demultiplex_stats_row['# Reads'])

    for demultiplex_stats_row in demultiplex_stats:
        quality_metrics_rows = [row for row in quality_metrics if 
                                    row['Lane'] == demultiplex_stats_row['Lane'] and
                                    row['SampleID'] == demultiplex_stats_row['SampleID'] and
                                    row['Sample_Project'] == demultiplex_stats_row['Sample_Project']
                                    ]

        # Sample S-number depends only on the unique sample ID. It is reused for samples from
        # different projects with the same name.
        sample_id_position = sample_id_positions.get(demultiplex_stats_row['SampleID'])
        if not sample_id_position:
            sample_id_position = len(sample_id_positions) + 1
            sample_id_positions[demultiplex_stats_row['SampleID']] = sample_id_position
        num_data_read_passes = len(quality_metrics_rows)
        read_count = int(demultiplex_stats_row['# Reads'])
        sample_yield = sum(float(row['Yield']) for row in quality_metrics_rows)
        # QC for LIMS
        qc = { # Note: If the metrics are changed, they should also be updated in exchange_output_artifact_info
               # to populate artifacts with no demultiplexing stats
            '# Reads': read_count * num_data_read_passes,
            '# Reads PF': read_count * num_data_read_passes,
            'Yield PF (Gb)': sample_yield / 1e9,
            '% of PF Clusters Per Lane': 100 * float(demultiplex_stats_row['% Reads']),
            '% Perfect Index Read': float(demultiplex_stats_row['% Perfect Index Reads']),
            '% One Mismatch Reads (Index)': float(demultiplex_stats_row['% One Mismatch Index Reads']),
            '% Bases >=Q30': sum(float(row['YieldQ30']) for row in quality_metrics_rows) * 100 / max(1, sample_yield),
            'Ave Q Score': sum(float(row['Mean Quality Score (PF)']) for row in quality_metrics_rows) / num_data_read_passes,
        }
        # Details for yaml file
        demultiplexed_lane_sample_info.append({
            'lane': int(demultiplex_stats_row['Lane']),
            'samplesheet_sample_id': demultiplex_stats_row['SampleID'],
            'samplesheet_sample_project': demultiplex_stats_row['Sample_Project'],
            'num_data_read_passes': num_data_read_passes,
            'samplesheet_position': sample_id_position,
            'project_name': demultiplex_stats_row['Sample_Project'],
            'sample_name': demultiplex_stats_row['SampleID'],
            # For internal use by this script
            'qc': qc
        })
    return demultiplexed_lane_sample_info


def add_project_info_from_lims(lims, demultiplexed_lane_sample_info):
    """Lookup project information based on project name and add this to the sample records.
    
    It uses the project name directly from the sample sheet, bypassing the artifact hierarchy,
    so the project information can be corrected when re-demultiplexing if required."""

    project_details_cache = {}
    for lane_sample_info in demultiplexed_lane_sample_info:
        project_name = lane_sample_info['project_name']
        if project_name != "Undetermined":
            if project_name in project_details_cache:
                project_details = project_details_cache[project_name]
            else:
                projects = lims.get_projects(name=project_name)
                if projects:
                    # Just pick the last project if there are multiple projects with the same name. This shouldn't
                    # happen for real samples.
                    project = projects[-1]
                    # This information will be added to the demultiplexed (lane, sample) entries
                    project_details = {
                        'delivery_method': project.udf.get('Delivery method'),
                        'project_id': project.id,
                        'project_name': project_name,
                        # Note that the file mover will crash if the project type is unknown
                        # (can then be fixed in the yaml file)
                        'project_type': project.udf.get('Project type')
                    }
                else:
                    project_details = {}
                project_details_cache[project_name] = project_details
            lane_sample_info.update(project_details)


def exchange_output_artifact_info(lims, bcl_convert_process, demultiplexed_lane_sample_info):
    update_artifacts = set()
    # Lookup data for each output artifact
    for iparam, oparam in bcl_convert_process.input_output_maps:
        if oparam['output-generation-type'] != "PerReagentLabel": continue
        o = oparam['uri']
        i = iparam['uri']
        sample_id = o.udf.get("SampleSheet Sample_ID")
        if sample_id:
            artifact_lane = int(i.location[1].split(":")[0])
            # Lookup result for this sample by lane and sample ID
            matching_records = [row for row in demultiplexed_lane_sample_info
                    if row['lane'] == artifact_lane and row['samplesheet_sample_id'] == sample_id]

            if len(matching_records) > 1:
                # We need to find the correct record based on project name. We only do this if there is an
                # ambiguity, otherwise we want to let the user change the project.
                try:
                    project_name = o.samples[0].project.name
                except AttributeError:
                    continue # There is nothing we can do if this sample doesn't have a project
                matching_records = [ # Restrict records to the correct project
                    row
                    for row in matching_records
                    if row['Sample_Project'] == project_name
                ]
                if len(matching_records) > 1: # It's unexpected that there are multiple rows with the same project.
                    raise RuntimeError(f"Unable to find unique demultiplexed record for {o.id}.")
                
            if len(matching_records) == 0:
                # Set this output artifact's data to zero
                for metric in ['# Reads', '# Reads PF', 'Yield PF (Gb)', '% of PF Clusters Per Lane',
                                '% Perfect Index Read', '% One Mismatch Reads (Index)', '% Bases >=Q30',
                                'Ave Q Score']:
                    o.udf[metric] = 0
                update_artifacts.add(o)
                # Optional improvement:
                # We could do a lookup based on only the UUID-part of the sample ID here, to allow changing the name,
                # instead of failing to populate artifacts if the name is changed.
                # In most situations it's sufficient to edit the submitted sample name though.

            else:
                assert len(matching_records) == 1
                row = matching_records[0]
                for k, v in row['qc'].items():
                    o.udf[k] = v
                o.udf['Sample sheet position'] = row['samplesheet_position']
                o.udf['Number of data read passes'] = row['num_data_read_passes']
                o.udf['ORA compression'] = row['ora_compression']
                update_artifacts.add(o)

                row['artifact_id'] = o.id
                row['artifact_name'] = o.name
                row['lane_artifact'] = i.id
                row['sample_id'] = o.samples[0].id
    lims.put_batch(list(update_artifacts))


def add_undetermined_percent_to_lims(lims, input_artifacts, demultiplexed_lane_sample_info):
    for lane_artifact in input_artifacts:
        lane_number = int(lane_artifact.location[1].split(":")[0])
        undetermined_rows = [row for row in demultiplexed_lane_sample_info
                             if row['lane'] == lane_number and row['samplesheet_sample_id'] == "Undetermined"]
        if len(undetermined_rows) > 1:
            raise RuntimeError(f"There is more than one undetermined row in lane {lane_number}!")
        elif len(undetermined_rows) == 1:
            lane_artifact.udf['NSC % Undetermined Indices (PF)'] = undetermined_rows[0]['qc']['% of PF Clusters Per Lane']
        # If there is no undetermined, we don't do anything
    lims.put_batch(input_artifacts)


if __name__ == "__main__":
    main()

