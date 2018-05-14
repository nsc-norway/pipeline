# Takes a sample sheet file on the command line and replaces the sample names
# NOTE: Sample IDs and project names need to be anonymised separately
import random
import sys
alpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
id_name = {}
with open(sys.argv[1]) as f:
    for line in f:
        fiels = line.split(",")
        if len(fiels) > 2 and fiels[2]:
            try:
                fiels[2] = id_name[fiels[1]]
            except KeyError:
                data = fiels[2].split("-")
                fiels[2] = data[0]
                for d in data[1:]:
                    fiels[2] += "-" + "".join(random.choice(alpha) for a in d)
                id_name[fiels[1]] = fiels[2]
        print ",".join(fiels),

