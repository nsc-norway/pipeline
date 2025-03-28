#!/bin/env python

# Script to be called when entering the Demultiplexing and QC step. 

# Sets the source and destination directories based on information in the LIMS. 
# Copies the sample sheet from the associated cluster generation process to the 
# current LIMS process. The options are set based on the input samples. Other 
# options which do not depend on the inputs should be set as defaults in the LIMS
# or in the NSC configuration file.

# Use:
# python lims-setup-step.py <PROCESS_ID> <SAMPLE_SHEET_LIMSID>

# Should be configured with the LIMSID of the "Input sample sheet" file as the second
# argument, so when it writes this file, it will be uploaded to LIMS automatically.

# In general it will not exit with an error condition, as it should be possible 
# to supply this info manually if it can't be found automatically.


import sys
import os
from genologics.lims import *
import logging
from common import nsc, utilities

# Set these options
if nsc.SITE == "cees":
    CHECKED = {
            "hiseq": [
                "Auto 20. Prepare SampleSheet", "Auto 30. Demultiplexing",
                "Auto 40. Move fastq files", "Auto 50-80. QC", "Auto 90. Delivery and triggers", "Close when finished"
                ],
            "hiseq4k": [
                "Auto 10. Delay", "Auto 20. Prepare SampleSheet", "Auto 30. Demultiplexing",
                "Auto 40. Move fastq files", "Auto 50-80. QC", "Auto 90. Delivery and triggers", "Close when finished"
                ]
            }
    # Used to set different number of threads for different machines. Probably
    # not needed, as both HiSeq 2500 and HiSeq 4000 can use 20 threads here.
    THREADS_OVERRIDE = {}
else:
    CHECKED = {
            "hiseq": [
                "Auto 10. Copy run", "Auto 20. Prepare SampleSheet", "Auto 30. Demultiplexing",
                "Auto 40. Move fastq files", "Auto 50-80. QC", "Auto 90. Delivery and triggers",
                "Close when finished"
                ],
            "miseq": [
                "Auto 10. Copy run", "Auto 20. Prepare SampleSheet", "Auto 30. Demultiplexing",
                "Auto 40. Move fastq files", "Auto 50-80. QC", "Auto 90. Delivery and triggers",
                "Close when finished"
                ],
            "nextseq": [
                "No lane splitting",
                "Auto 10. Copy run", "Auto 20. Prepare SampleSheet", "Auto 30. Demultiplexing",
                "Auto 40. Move fastq files", "Auto 50-80. QC",
                "Auto 90. Delivery and triggers", "Close when finished"
                ],
            "hiseqx": [
                "Auto 10. Copy run", "Auto 20. Prepare SampleSheet", "Auto 30. Demultiplexing",
                "Auto 40. Move fastq files", "Auto 50-80. QC", "Auto 90. Delivery and triggers",
                "Close when finished"
                ],
            "hiseq4k": [
                "Auto 10. Copy run", "Auto 20. Prepare SampleSheet", "Auto 30. Demultiplexing",
                "Auto 40. Move fastq files", "Auto 50-80. QC", "Auto 90. Delivery and triggers",
                "Close when finished"
                ],
            "novaseq": [
                "Auto 10. Copy run", "Auto 20. Prepare SampleSheet", "Auto 30. Demultiplexing",
                "Auto 40. Move fastq files", "Auto 50-80. QC", "Auto 90. Delivery and triggers",
                "Close when finished"
                ],
            }
    THREADS_OVERRIDE = {
            "novaseq": 128,
            "miseq": 32,
            "nextseq": 64
            }


def get_sample_sheet_data(cluster_proc, fcid):
    """Gets the sample sheet from the clustering- or denature/dilute/load process"""

    outputs = cluster_proc.all_outputs(unique=True)
    for io in cluster_proc.input_output_maps:
        i = io[0]
        o = io[1]
        if o['output-type'] == 'ResultFile' and o['output-generation-type'] == 'PerAllInputs':
            name = o['uri'].name
            if name == 'SampleSheet csv' or name == "bcl2fastq Sample Sheet" or name == "MiSeq SampleSheet" or name == "Sample Sheet" or name == "bcl2fastq SampleSheet":
                files = []
                if len(o['uri'].files) == 1:
                    f = o['uri'].files[0]
                    files.append(f)
                    if f.original_location == "{0}.csv".format(fcid):
                        return f.download()
                if len(files) == 1: # Return sample sheet file even if wrong name, if it's the only one
                    return f.download()
    return None


def main(process_id, sample_sheet_file):
    if ':' in process_id:
        server_id, process_id = process_id.split(":")[0:2]
    else:
        server_id, process_id = None, process_id
    process = Process(nsc.get_lims(server_id), id=process_id)

    fcids = set()
    wells = set()
    inputs = process.all_inputs(resolve=True) # Resolve, to cache and use later
    for i in inputs:
        fcids.add(i.location[0].name)
        wells.add(i.location[1])

    if len(fcids) != 1:
        logging.error("Multiple flowcells in inputs, this is going to end in frustration")
        return 1

    fcid = next(iter(fcids))

    if len(wells) != i.location[0].occupied_wells:
        # Use a subset of the lanes
        lanes = "".join(str(l.split(":")[0]) for l in sorted(wells))
        process.udf[nsc.LANES_UDF] = lanes

    seq_proc = utilities.get_sequencing_process(process)
    parent_processes = process.parent_processes()
    parent_uris = set(p.uri for p in parent_processes if p is not None)

    # This script can only handle the case when there is a single clustering process
    # (or for Mi/NextSeq, ...Load samples process)
    if len(parent_uris) == 1:
        # look for clustering (HiSeq) or "load samples" proceess (Ne/MiSeq)
        # This is where the sample sheet is generated
        parent_proc = parent_processes[0]
        sample_sheet_data = get_sample_sheet_data(parent_proc, fcid)
        if sample_sheet_data:
            open(sample_sheet_file, "wb").write(sample_sheet_data)
    else:
        logging.info("Cannot auto-detect sample sheet when there are more than one clustering processes")

    if seq_proc:
        try:
            run_id = seq_proc.udf['Run ID']
        except KeyError:
            run_id = seq_proc.udf.get('RunID')

        if run_id:
            process.udf[nsc.RUN_ID_UDF] = run_id
            logging.debug('Found Run ID: {}.'.format(run_id))
        else:
            logging.debug('Unable to determine Run ID.')

        instrument = utilities.get_instrument(seq_proc)
        for udf in CHECKED.get(instrument, []):
            process.udf[udf] = True
        threads = THREADS_OVERRIDE.get(instrument)
        if threads is not None:
            process.udf[nsc.THREADS_UDF] = threads
        if instrument == "novaseq":
            if next(iter(process.all_inputs())).location[0].type.name == "Library Tube":
                # NovaSeq Standard workflow: Don't split the lanes
                process.udf['No lane splitting'] = True
    else:
        logging.warning("Couldn't find the sequencing process")

    all_samples = process.lims.get_batch(set(s for i in inputs for s in i.samples))
    projects = set(sam.project for sam in all_samples if sam.project)
    project_demux_options = set(project.udf.get('Demultiplexing options') for project in projects)
    if len(project_demux_options) > 1:
        logging.error("""Projects have different values for 'Demultiplexing options': {}. Fix by changing options,
                    or splitting in multiple steps.""".format(
            ", ".join(repr(var) for var in project_demux_options)
            ))
        return 1 ### ABORT - This will leave the step open with an error message

    for option in project_demux_options: # There is just zero or one options
        if option:
            process.udf['Other options for bcl2fastq'] = option

    process.put()
    logging.debug('Saved settings in the process')

    logging.info("Program completed")
    return 0



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.debug('Starting "setup demultiplexing" script')
    sys.exit(main(sys.argv[1], sys.argv[2]))

