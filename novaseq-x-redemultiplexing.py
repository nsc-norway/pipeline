# Script to run off-board redemultiplexing of Novaseq X data
# It is called on the "Generate BCL Convert Samplesheet" step, and receives the process ID
# of this step as an argument.


import os
import sys
import time
import subprocess
from pathlib import Path

from genologics.lims import *
from genologics import config


RUN_FOLDER_LOCATION = "/data/runScratch.boston/NovaSeqX"


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

    compute_type = samplesheet_process.udf.get("BCL Convert Instrument")
    if not compute_type in ['External DRAGEN', 'CPU']:
        print(f"Unsupported compute type '{compute_type}' for off-board redemultiplexing")
        sys.exit(1)
    bcl_convert_process.udf["Compute platform"] = compute_type
    bcl_convert_process.udf["Run ID"] = run_id
    data_compression_type = samplesheet_process.udf['FASTQ Compression Format']

    run_folder_udf = bcl_convert_process.udf.get('Sequencing Output Folder')
    if run_folder_udf:
        run_folder_path = Path(run_folder_udf)
    else:
        run_folder_path = Path(RUN_FOLDER_LOCATION) / run_id
    if not run_folder_path.exists():
        print(f"Run folder {run_folder_path} does not exist. You can override the Sequencing Output Folder on the BCL Convert step ({bcl_convert_process_id}).")
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

    # Store the sample sheet in the analysis folder
    with open(samplesheet_path, "wb") as f:
        f.write(sample_sheet)

    # Create demultiplexing script file and run demultiplexing
    job_name = f"{analysis_id}.{run_id}"
    bcl_convert_process.udf['Status'] = "RUNNING"
    bcl_convert_process.put()
    print("RUNNING", job_name)
    if compute_type == "External DRAGEN":
        returncode, stderr = run_demultiplexing_dragen(job_name, run_folder_path, samplesheet_path, output_folder, fastq_compression)
    else:
        returncode, stderr = run_demultiplexing_cpu(job_name, run_folder_path, samplesheet_path, output_folder_parent, output_folder)

    if returncode != 0:
        print("ERROR: BCL Conversion returned non-zero exit code. Message: '", stderr, "'.")
        bcl_convert_process.udf['Status'] = "FAILED"
        bcl_convert_process.put()
        sys.exit(1)


    # Import the demultiplexing results

    # Import BCL convert / DRAGEN version and run ID

    # Create yaml file for downstream automation
    # Import the demultiplexing results into LIMS 


def get_analysis_id_and_path(bcl_convert_process, run_folder_path, compute_type):
    """Select an analysis folder - will reuse an existing one or create a new one."""

    analysis_id = bcl_convert_process.udf.get("Analysis ID")
    if not analysis_id:
        compute_type_code = {
            "External DRAGEN": "e",
            "CPU": "c",
        }[compute_type]
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


def run_demultiplexing_dragen(job_name, run_folder_path, samplesheet_path, output_folder_parent, output_folder, data_compression_type):
    script_content = f"""#!/bin/bash
#SBATCH --job-name={job_name}
#SBATCH --partition=dragen
#SBATCH --time=24:00:00


dragen \
        --bcl-input-directory {run_folder_path} \
        --ora-reference TODO_PATH \
        --output-directory {output_folder} \
        --bcl-sampleproject-subdirectories true \
        --sample-sheet {samplesheet_path}
"""
    script_path = output_folder_parent / "script.sh"
    with open(script_path, "w") as script_file:
        script_file.write(script_content)
    result = subprocess.run(['sbatch', '--wait', script_path], cwd=output_folder_parent, stderr=subprocess.PIPE)
    return result.returncode, result.stderr


def run_demultiplexing_cpu(job_name, run_folder_path, samplesheet_path, output_folder_parent, output_folder):

    log_folder = output_folder_parent / "logs"
    log_folder.mkdir(exist_ok=True)
    script_content = f"""#!/bin/bash
#SBATCH --job-name={job_name}
#SBATCH --cpus-per-task=64
#SBATCH --mem=128G
#SBATCH --time=24:00:00
#SBATCH --qos=high

apptainer exec \
    --bind {RUN_FOLDER_LOCATION} \
    --bind {log_folder}:/var/log/bcl-convert \
    /data/common/tools/bclconvert/bclconvert-4.2.4.sif \
        bcl-convert \
        --bcl-input-directory {run_folder_path} \
        --output-directory {output_folder} \
        --bcl-sampleproject-subdirectories true \
        --sample-sheet {samplesheet_path}
"""
    script_path = output_folder_parent / "script.sh"
    with open(script_path, "w") as script_file:
        script_file.write(script_content)
    result = subprocess.run(['sbatch', '--wait', script_path], cwd=output_folder_parent, stderr=subprocess.PIPE)
    return result.returncode, result.stderr
    

def parse_qc_stats(bcl_convert_process, analysis_path):
    # TODO
    pass




def run_demultiplexing_job(script_path, analysis_path):
    # Submit the demultiplexing job
    subprocess.run(["sbatch", script_path], check=True) 


if __name__ == "__main__":
    main()

