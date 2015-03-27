import re
from xml.etree import ElementTree
from collections import defaultdict

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



def parse_demux_summary(demux_summary_file_path):
    '''Get lane-read-level demultiplexing statistics from
    Flowcell_demux_summary.xml.
    
    Statistics gathered:
        - per lane
        - per read (1/2)
        - pass filter / raw
    
    The lane_stats dict is quad-nested; by lane ID, read index (1/2),
    Pf / Raw and finally stat name.
    '''

    tree = ElementTree.parse(demux_summary_file_path)
    root = tree.getroot()
    tree = lambda: defaultdict(tree)
    total = tree()
    undetermined = tree()
    if root.tag == "Summary":
        for lane in root.iterfind("Lane"):
            # Dicts indexed by read, for per-read-lane stats
            lane_id = lane.attrib['index']

            for sample in lane.iterfind("Sample"):
                for barcode in sample.iterfind("Barcode"):
                    barcode_index = barcode.attrib['index']
                    for tile in barcode.iterfind("Tile"):
                        for read in tile.iterfind("Read"):
                            read_id = read.attrib['index']
                            for filtertype in read:
                                ft = filtertype.tag
                                for stat in filtertype:
                                    total[lane_id][read_id][ft][stat.tag] += int(stat.text)
                                    if barcode_index == "Undetermined":
                                        undetermined[lane_id][read_id][ft][stat.tag] += int(stat.text)


    return total, undetermined



#stats = parse_demux_stats("/home/fa2k/tmp/demo/demux-results/Basecall_Stats_C6A17ANXX/Flowcell_demux_summary.xml")

