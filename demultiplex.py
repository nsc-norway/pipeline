# Demultiplexing common routines

import os
import re
import glob
from genologics.lims import *
from xml.etree import ElementTree


# List of tupes:
# First element: list of names that may be seen in the HTML file
# Second element: UDF name without R1 / R2 suffix *
# Third element: function to translate value or None for no change

# * The actual name of the UDF is Yield PF (Gb) R1 etc.

stats = [
        (('# Reads'), '# Reads', None),
        (('Yield (Mbases)'), 'Yield PF (Gb)', lambda x: str(int(x) / 1.0)),
        (('% PF'), '%PF', None), 
        (('% of raw clusters per lane'), '% of Raw Clusters Per Lane', None),
        (('% Perfect Index Reads'), '% Perfect Index Read', None),
        (('% One Mismatch Reads (Index)'), '% One Mismatch Reads (Index)', None),
        (('% of &gt;= Q30 Bases (PF)'), '% Bases >=Q30', None),
        (( 'Mean Quality Score (PF)'), 'Ave Q Score', None)
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
    for row in re.finditer("<tr>(.*?)</tr>", barcode_lane_table):
        barcode_lane = {}
        for i, cell in enumerate(re.finditer("<td>(.*?)</td>", row)):
            barcode_lane[field_names[i]] = cell
        barcode_lanes.append(barcode_lane)

    return barcode_lanes



def lookup_sample(process, sample_name):
    for i in process.all_inputs(unique=True):
        if i.name == sample_name:
            return i
    return None



def populate_results(process, demux_result_dir):
    '''Reads demultiplexing results and adds UDFs to artifacts.
    
    The demux_result_dir argument should refer to the "Unaligned" directory.'''

    demultiplex_stats = glob.glob(demux_result_dir + "/Basecall_Stats_*/Demultiplex_Stats.htm")
    if not demultiplex_stats:
        raise ValueError("Demultiplex_Stats.htm file does not exist")

    statfile = open(demultiplex_stats[0])
    shared_out_files = process.shared_result_files()
    stats_data = statfile.read()
    for f in shared_out_files:
        if f.name == nsc.DEMULTIPLEX_STATS_FILE:
            f.upload(stats_data)

    ds = parse_demux_stats(stats_data)

    for sample in ds:
        print "SampleRef: ", sample_lane['SampleRef']
        if sample_lane['Index'] != "Undetermined":
            # Try to look up the sample by LIMS ID -- if the sample sheet is generated
            # by Clarity, the Description is set to the LIMS ID.
            limsid = sample_lane['Description']
            lims_sample = None
            if limsid:
                try:
                    lims_sample = Artifact(process.lims, limsid)
                except:
                    pass
            if not lims_sample:
                # Look up by name
                lims_sample = lookup_sample(process, sample_lane['SampleRef'])

            print "Lims sample looked up: ", lims_sample

            if lims_sample:
                for statname, udfname, convert in stats:
                    for st in statname:
                        try:
                            if not convert:
                                convert = lambda x: x
                            lims_sample.udf[udfname] = convert(sample_lane[st])
                            print "Set " , udfname, " to ", sample_lane[st], "on", lims_sample.id
                        except KeyError:
                            pass
                lims_sample.put()

    return True

