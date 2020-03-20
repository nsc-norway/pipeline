# Takes a sample sheet file on the command line and replaces the sample names
# NOTE: Sample IDs and project names need to be anonymised separately
import random
import sys
alpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
id_name = {}
lane_offset = 0
with open(sys.argv[1]) as f:
    for line in f:
        fiels = line.split(",")
        if len(fiels) > 1+lane_offset and fiels[1+lane_offset]:
            try:
                fiels[1+lane_offset] = id_name[fiels[lane_offset]]
            except KeyError:
                data = fiels[1+lane_offset].split("-")
                if len(data) > 1: # Identify sample names as entries containing hyphen
                    fiels[1+lane_offset] = "-".join("".join(random.choice(alpha) for a in d) for d in data)
                id_name[fiels[lane_offset]] = fiels[1+lane_offset]
        print ",".join(fiels),

