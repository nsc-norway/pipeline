# Demultiplexing common routines

import os
import re
import glob
from xml.etree import ElementTree
from decimal import *
from genologics.lims import *
import nsc, utilities
import parse


# Stats on FASTQ files. List of tupes:
# First element: list of names that may be seen in the HTML file
# Second element: UDF name
# Third element: function to translate value or None for no change

# * The actual name of the UDF is Yield PF (Gb) R1 etc.

stats = [
        (('# Reads',), '# Reads', lambda x: int(x.replace(',',''))),
        (('Yield (Mbases)',), 'Yield PF (Gb)', lambda x: int(x) / 1.0),
        (('% PF',), '%PF', float), 
        (('% of raw clusters per lane',), '% of Raw Clusters Per Lane', float),
        (('% Perfect Index Reads',), '% Perfect Index Read', float),
        (('% One Mismatch Reads (Index)',), '% One Mismatch Reads (Index)', float),
        (('% of &gt;= Q30 Bases (PF)',), '% Bases >=Q30', float),
        (( 'Mean Quality Score (PF)',), 'Ave Q Score', float)
        ]




def lookup_outfile(process, analyte_id, lane):
    """Look up the output artifact representing a line in the sample sheet"""

    lane_id = lane + ":1"
    sample = Artifact(nsc.lims, id=analyte_id).samples[0]
    for input,output in process.input_output_maps:
        if input['uri'].location[1] == lane_id:
            if input['uri'].samples[0].id == sample.id:
                if output['uri'].name == nsc.FASTQ_OUTPUT.format(sample.name):
                    return output['uri']
                    
    return None


def set_udfs(process, demultiplex_stats):
    """Set UDFs on inputs (analytes representing the lanes) and output
    files (each fastq file).
    """
    inputs = dict((i.location[0], i) for i in process.all_inputs(unique=True))
    if len(set(i.location[1] for i in inputs)) != 1:
        print "error: Wrong number of flowcells detected"
        return

    for sample_lane in demultiplex_stats:
        if sample_lane['Index'] != "Undetermined":
            # Try to look up the sample by LIMS ID -- if the sample sheet is generated
            # by Clarity, the Description is set to the LIMS ID.
            lims_fastqfile = lookup_outfile(process, sample_lane['Description'], sample_lane['Lane'])

            if lims_fastqfile:
                for statname, udfname, convert in stats:
                    for st in statname:
                        try:
                            if not convert:
                                convert = lambda x: x
                            if sample_lane[st]:
                                lims_fastqfile.udf[udfname] = convert(sample_lane[st])
                        except KeyError:
                            pass
                lims_fastqfile.put()

            else: # Undetermined!
                analyte = inputs["{0}:1".format(sample_lane['Lane'])]
                analyte.udf[nsc.LANE_UNDETERMINED_UDF] = sample_lane['% of raw clusters per lane']
                analyte.put()





def set_lane_udfs(process, demultiplexing_dir):
    """ -- NOT USED -- 
    
    Set UDFs on inputs to the demultiplexing proces, based on demultiplexing 
    statistics. """
    

    for lane, data in ds.items():
        analyte = inputs["{0}:1".format(lane)]
        for read, d in data.items():
            yield_pf = sum(s['Yield'] for s in d['Pf'])




def populate_results(process, demux_result_dir):
    """Reads demultiplexing results and adds UDFs to artifacts.
    
    The demux_result_dir argument should refer to the "Unaligned" directory."""

    demultiplex_stats = glob.glob(demux_result_dir + "/Basecall_Stats_*/Demultiplex_Stats.htm")
    if not demultiplex_stats:
        raise ValueError("Demultiplex_Stats.htm file does not exist")

    statfile = open(demultiplex_stats[0])
    stats_data = statfile.read()
    utilities.upload_file(process, "Demultiplex_stats.htm", data = stats_data)

    ds = parse.parse_demux_stats(stats_data)
    set_udfs(process, ds)

    return True

