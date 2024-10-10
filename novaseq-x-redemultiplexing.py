# Script to run off-board redemultiplexing of Novaseq X data
# It is called on the "Generate BCL Convert Samplesheet" step, and receives the process ID
# of this step as an argument.


import os
import sys
import time
from pathlib import Path

from genologics.lims import *
from genologics import config


RUN_FOLDER_LOCATION = "/data/runScratch.boston/NovaSeqX"


def main():

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
        print("BCL Convert LIMS-ID is not set")
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

    compute_type = bcl_convert_process.udf.get("BCL Convert Instrument")
    if not compute_type in ['Dedicated DRAGEN', 'CPU']:
        print(f"Unsupported compute type {compute_type} for off-board redemultiplexing")
        sys.exit(1)

    run_folder_path = Path(RUN_FOLDER_LOCATION) / run_id
    if not run_folder_path.exists():
        print(f"Run folder {run_folder_path} does not exist")
        sys.exit(1)
    assert (run_folder_path / "Analysis").is_dir(), f"Analysis folder should exist at {run_folder_path / 'Analysis'}"

    analysis_id, analysis_path = get_analysis_id_and_path(bcl_convert_process, run_folder_path, compute_type)
    

    # Download the sample sheet data
    for o in samplesheet_process.all_outputs(unique=True):
        if o.output_type == "ResultFile" and o.name == "TODO Sample Sheet Artifact Name":
            if len(o.files) == 1:
                sample_sheet = o.files[0].download()
                break
    else:
        print("Sample sheet data not found")
        sys.exit(1)

    # Store the sample sheet in the analysis folder
    samplesheet_name = "SampleSheet.csv"
    samplesheet_path = analysis_path / samplesheet_name
    if samplesheet_path.exists():
        print(f"Sample sheet already exists at {samplesheet_path}")
        sys.exit(1)
    with open(samplesheet_path, "w") as f:
        f.write(sample_sheet)
    
    # Create demultiplexing script file
    script_path = analysis_path / "demultiplex.sh"
    if compute_type == "Dedicated DRAGEN":
        script_content = get_dragen_script_content(samplesheet_process, samplesheet_name)
    else:
        script_content = get_cpu_script_content(samplesheet_process, samplesheet_name)
    with open(script_path, "w") as f:
        f.write(script_content)
    
    # Submit demultiplexing job
    run_demultiplexing_job(script_path, analysis_path)





# Select an analysis folder
def get_analysis_id_and_path(bcl_convert_process, run_folder_path, compute_type):
    analysis_id = bcl_convert_process.udf.get("Analysis ID")
    if not analysis_id:
        compute_type_code = {
            "Dedicated DRAGEN": "d",
            "CPU": "c",
        }[compute_type]
        previous_analyses = (run_folder_path / "Analysis").glob(f"{compute_type_code}*")
        if not previous_analyses:
            analysis_id = f"{compute_type_code}1"
        else:
            try:
                previous_analysis_numbers = [int(analysis.name[len(compute_type_code):]) for analysis in previous_analyses if analysis.name[len(compute_type_code):].isdigit()]
                previous_analysis_number = max(previous_analysis_numbers) if previous_analysis_numbers else 0
            except ValueError:
                print(f"Previous analysis {previous_analyses[-1]} does not have a numeric suffix")
                sys.exit(1)
            analysis_id = f"{compute_type_code}{previous_analysis_number + 1}"
        bcl_convert_process.udf["Analysis ID"] = analysis_id
    analysis_path = run_folder_path / "Analysis" / analysis_id
    analysis_path.mkdir(parents=True)
    # TODO put the output path
    bcl_convert_process.put()
    return analysis_id, analysis_path



def get_cpu_script_content(run_folder_path, analysis_path, samplesheet_name):
    script_content = f"""#!/bin/bash
#SBATCH --job-name=process_{samplesheet_name}
#SBATCH --cpus-per-task=64
#SBATCH --mem=128G
#SBATCH --time=24:00:00
#SBATCH --qos=high

apptainer exec \
    --bind {RUN_FOLDER_LOCATION} \
    --bind /var/log/bcl-convert:{analysis_path}/Logs \
    /data/common/tools/bcl-convert/4.0.0/bcl-convert.sif \
        bcl-convert \
        --bcl-input-directory {run_folder_path} \
        --output-directory {analysis_path}/BCLConvert/fastq \
        --bcl-sampleproject-subdirectories true \
        --sample-sheet {samplesheet_name}
"""
    return script_content
    

def get_dragen_script_content(samplesheet_process, samplesheet_name):
    script_content = f"""#!/bin/bash
#SBATCH --job-name=process_{samplesheet_name}
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=24:00:00

echo "Processing samplesheet: {samplesheet_name}"
{samplesheet_process} {samplesheet_name}

echo "Samplesheet processing complete"
"""
    return script_content


def parse_qc_stats(bcl_convert_process, analysis_path):
    # TODO
    pass



def run_demultiplexing_job(script_path, analysis_path):
    # Submit the demultiplexing job
    subprocess.run(["sbatch", script_path], check=True) 


if __name__ == "__main__":
    main()

