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


def make_id_resultfile_map(process, sample_sheet_data, reads):
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





