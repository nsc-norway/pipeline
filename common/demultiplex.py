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
    try:
        os.path.rename(original_path, new_path)
    except OSError:
        print "WARNING: Failed to rename output dir"

    return new_path


