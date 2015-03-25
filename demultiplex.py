# Demultiplexing common routines

import os
import re
import glob
from xml.etree import ElementTree
from decimal import *
from genologics.lims import *
import nsc, utilities


# List of tupes:
# First element: list of names that may be seen in the HTML file
# Second element: UDF name without R1 / R2 suffix *
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


def get_demux_stats(demux_summary_file_path):
    tree = ElementTree.parse(demux_summary_file_path)
    root = tree.getroot()
    if root.tag == "Summary":
        for lane in root.findall("Lane"):
            pass


def parse_demux_stats(stats_data):
    '''Parse the Demultiplex_stats.htm file and return a list of records,
    one for each row.'''

    # re.DOTALL does a "tall" match, including multiple lines
    tables = re.findall("<table[ >].*?</table>", stats_data, re.DOTALL)

    header_table = tables[0]
    field_names = []
    for match in re.finditer("<th>(.*?)</th>", header_table):
        field_names.append(match.group(1))

    barcode_lane_table = tables[1]
    barcode_lanes = []
    for row in re.finditer("<tr>(.*?)</tr>", barcode_lane_table, re.DOTALL): 
        barcode_lane = {}
        for i, cell in enumerate(re.finditer("<td>(.*?)</td>", row.group(1))):
            barcode_lane[field_names[i]] = cell.group(1)
        barcode_lanes.append(barcode_lane)

    return barcode_lanes



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


def populate_results(process, demux_result_dir):
    '''Reads demultiplexing results and adds UDFs to artifacts.
    
    The demux_result_dir argument should refer to the "Unaligned" directory.'''

    demultiplex_stats = glob.glob(demux_result_dir + "/Basecall_Stats_*/Demultiplex_Stats.htm")
    if not demultiplex_stats:
        raise ValueError("Demultiplex_Stats.htm file does not exist")

    statfile = open(demultiplex_stats[0])
    stats_data = statfile.read()
    utilities.upload_file(process, nsc.DEMULTIPLEX_STATS_FILE, data = stats_data)

    ds = parse_demux_stats(stats_data)

    for sample_lane in ds:
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

    return True

