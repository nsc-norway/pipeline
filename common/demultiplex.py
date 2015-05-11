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


def populate_results(process, ids_resultfile_map, demultiplex_stats):
    """Set UDFs on inputs (analytes representing the lanes) and output
    files (each fastq file).
    """
    inputs = dict((i.location[1], i) for i in process.all_inputs(unique=True))
    if len(set(i.location[0] for i in inputs.values())) != 1:
        print "error: Wrong number of flowcells detected"
        return False

    for coordinates, stats in demultiplex_stats.items():
        lane, sample_name, read = coordinates
        lims_fastqfile = None
        try:
            lims_fastqfile = ids_resultfile_map[(lane, sample_name, read)]
            undetermined = False
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
            analyte = inputs["{0}:1".format(lane)]
            analyte.udf[nsc.LANE_UNDETERMINED_UDF] = stats['% of PF Clusters Per Lane']
            analyte.put()

    return True



def download_sample_sheet(process, save_dir, append_limsid=True):
    """Downloads the demultiplexing process's sample sheet, which contains only
    samples for the requested project (added to the LIMS by setup-*-demultiplexing.py)."""

    sample_sheet = None
    for o in process.all_outputs(unique=True):
        if o.output_type == "ResultFile" and o.name == "SampleSheet csv":
            if len(o.files) == 1:
                sample_sheet = o.files[0].download()

    if sample_sheet:
        if append_limsid:
            name = "SampleSheet-" + process.id + ".csv"
        else:
            name = "SampleSheet.csv"
        path = os.path.join(save_dir, name)
        file(path, 'w').write(sample_sheet)
        return path, sample_sheet
    else:
        return None, None


def create_projdir_ne_mi(runid, basecalls_dir, sample_sheet, lane, reads):
    """Creates project directory and moves fastq files into it."""

    project_name = sample_sheet['header']['Experiment Name']
    
    dir_name = parse.get_project_dir(runid, project_name)
    proj_path = basecalls_dir + "/" + dir_name
    try:
        os.mkdir(proj_path)
    except OSError:
        pass
    for i, sam in enumerate(sample_sheet['data']):
        for r in reads:
            sample_name = sam['samplename']
            if not sample_name:
                sample_name = sam['sampleid']
            basename = "{0}_S{1}_L00{2}_R{3}_001.fastq.gz".format(
                    sample_name, str(i + 1), lane, r)
            old_path = basecalls_dir + "/" + basename
            new_path = proj_path + "/" + basename
            os.rename(old_path, new_path)




