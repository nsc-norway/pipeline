import sys, os.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common import nsc, parse
from genologics.lims import *



def parse_csv_sample_sheet(sample_sheet):
    lines = sample_sheet.splitlines()
    headers = lines[0].split(",")
    samples = []
    for l in lines[1:]:
        sam = {}
        for h, v in zip(headers, l.split(",")):
            sam[h] = v
        samples.append(sam)
    return samples



def main(project_id, sample_sheet):
    proj = Project(nsc.lims, id=sys.argv[2])
    ssheet_data = parse.parse_hiseq_sample_sheet(open(sys.argv[1]))
    
    for sam in ssheet_data:
        sample_id = sam['SampleID']
        index = sam['Index']
        if index:
            pass #TODO
        




if len(sys.argv) != 3:
    print "Usage: python add-samples.py sample-sheet index-category project-lims-id"
    sys.exit(1)
else:
    main(sys.argv[1], sys.argv[2], sys.argv[3])




