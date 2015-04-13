# Demultiplexing common routines

import os
import re
import glob
from xml.etree import ElementTree
from decimal import *
from genologics.lims import *
import nsc
import utilities
import parse


udf_list = [
        '# Reads', 'Yield PF (Gb)', '% of Raw Clusters Per Lane',
        '% Perfect Index Read', 'One Mismatch Reads (Index)',
        '% Bases >=Q30', 'Ave Q Score'
        ]


def make_id_resultfile_map(process, sample_sheet, reads):
    themap = {}
    lanes = set(int(entry['Lane']) for entry in sample_sheet_data)
    ext_sample_sheet = dict(sample_sheet_data)

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
                        if output['uri'].name == nsc.FASTQ_OUTPUT.format(sample.name, read):
                            themap[(int(lane), id, read)] = output['uri']

    return themap


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
            for statname in udf_list:
                try:
                    lims_fastqfile.udf[statname] = stats[statname]
                except KeyError:
                    pass
            lims_fastqfile.put()
    
        elif undetermined:
            analyte = inputs["{0}:1".format(sample_lane['Lane'])]
            analyte.udf[nsc.LANE_UNDETERMINED_UDF] = stats['% of PF Clusters Per Lane']
            analyte.put()



def download_sample_sheet(process, save_dir):
    """Downloads the demultiplexing process's sample sheet, which contains only
    samples for the requested project (added to the LIMS by setup-*-demultiplexing.py)."""

    sample_sheet = None
    for o in process.all_outputs(unique=True):
        if o.output_type == "ResultFile" and o.name == "SampleSheet csv":
            if len(o.files) == 1:
                sample_sheet = o.files[0].download()

    if sample_sheet:
        if process.id == "":
            raise ValueError("Process ID is an empty string")
        name = "SampleSheet-" + process.id + ".csv"
        path = os.path.join(save_dir, name)
        file(path, 'w').write(sample_sheet)
        return name, sample_sheet
    else:
        return None, None


def rename_projdir_ne_mi(runid, output_dir, sample_sheet):
    """Renames project directory if it exists."""

    project_name = sample_sheet['header']['Experiment Name']
    
    basecalls_dir = os.path.join(output_dir, "Data", "Intensities", "BaseCalls") 
    original_path = basecalls_dir + "/Project_" + project_name
    dir_name = parse.get_project_dir(runid, project_name)
    new_path = basecalls_dir + "/" + dir_name
    return new_path


