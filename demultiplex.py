# Demultiplexing common routines

import os
import re
import glob
from xml.etree import ElementTree
from decimal import *
from genologics.lims import *
import nsc, utilities
import results


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
    '''Look up the output artifact representing a line in the sample sheet'''

    lane_id = lane + ":1"
    sample = Artifact(nsc.lims, id=analyte_id).samples[0]
    for input,output in process.input_output_maps:
        if input['uri'].location[1] == lane_id:
            if input['uri'].samples[0].id == sample.id:
                if output['uri'].name == nsc.FASTQ_OUTPUT.format(sample.name):
                    return output['uri']
                    
    return None


def set_output_file_udfs(process, demultiplex_stats):
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


def set_lane_udfs(process, demultiplexing_dir):
    '''Set UDFs on inputs to the demultiplexing proces, based on demultiplexing 
    statistics. Setting data on inputs is a bit strange, but common for QC 
    processes.
    
    TODO: Determine which stats are filled by the sequencing process; add information
    on undetermined indexes.'''
    
    demultiplex_summary = glob.glob(demux_result_dir + "/Basecall_Stats_*/Flowcell_demux_summary.xtm")
    ds = results.parse_demux_summary(demultiplex_summary)
    inputs = dict((i.location[0], i) for i in process.all_inputs(unique=True))
    if len(set(i.location[1] for i in inputs)) != 1:
        print "error: Wrong number of flowcells detected"
        return

    for lane, data in ds.items():
        analyte = inputs["{0}:1".format(lane)]
        for read, d in data.items():
            yield_pf = sum(s['Yield'] for s in d['Pf'])





def populate_results(process, demux_result_dir):
    '''Reads demultiplexing results and adds UDFs to artifacts.
    
    The demux_result_dir argument should refer to the "Unaligned" directory.'''

    demultiplex_stats = glob.glob(demux_result_dir + "/Basecall_Stats_*/Demultiplex_Stats.htm")
    if not demultiplex_stats:
        raise ValueError("Demultiplex_Stats.htm file does not exist")

    statfile = open(demultiplex_stats[0])
    stats_data = statfile.read()
    utilities.upload_file(process, nsc.DEMULTIPLEX_STATS_FILE, data = stats_data)

    ds = results.parse_demux_stats(stats_data)
    set_output_file_udfs(process, ds)
    set_lane_udfs(process, demux_result_dir)

    return True

