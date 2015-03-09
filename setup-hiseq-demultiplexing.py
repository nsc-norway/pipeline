# Script to be called directly by EPP when starting the HiSeq demultiplexing step. 
# Sets steering options for the demultiplexing job in UDFs on the demultiplexing 
# process. The options are set based on the input samples. Other options which 
# do not depend on the inputs should be set as defaults in the LIMS or in the 
# NSC configuration file.

import sys
from argparse import ArgumentParser
from genologics.lims import *
import nsc
import utilities


# Key determined parameters:
# - Sample sheet filtered to include the lanes of the demultiplexing process's inputs
# - Use-bases-mask parameter

def get_sample_sheet_data(cluster_proc):
    '''Gets the sample sheet from the clustering process'''

    outputs = cluster_proc.all_outputs(unique=True)
    for o in outputs:
        if o.output_type == 'ResultFile' and o.name == 'SampleSheet csv':
            if len(o.files) == 1:
                return o.files[0].download()
    return None


def extract_sample_sheet(sample_sheet, inputs):
    '''Extracts the lanes in the inputs from the sample sheet'''
    
    lanes = []
    for i in inputs:
        lanes.append(i.location[1].split(':')[0])

    ss = sample_sheet.splitlines()
    res = [ss[0]]
    for row in ss[1:]:
        columns = row.split(',')
        if columns[1] in lanes:
            res.append(row)

    return "\r\n".join(res)



def compute_bases_mask(process):
    '''Compute the --use-bases-mask option for fastq conversion. 
    This option specifies how each imaging cycle is interpreted. It 
    gives the number and order of data reads and index reads.
    
    The argument is a reference to the current process.'''

    seq_proc = utilities.get_sequencing_process(process)

    # These are the properties of the run. The full data sequence is 
    # always returned, but we set the index length and multiplicity.
    # For this we also need to look up the properties of the input samples.
    read1_length = seq_proc.udf['Read 1 Cycles']
    read2_length = seq_proc.udf['Read 2 Cycles']
    index1_length = seq_proc.udf['Index 1 Read Cycles']
    index2_length = seq_proc.udf['Index 2 Read Cycles']
    # TODO: what happens for single read / single index runs?
    # Check with actual run...

    # Always use the full read length for data reads
    use_bases_mask =  "y%d" % read1_length
    use_bases_mask += "I" + str




def main(process_id, sample_sheet_file):
    process = Process(nsc.lims, id=process_id)
    # Get the clustering processes
    parent_processes = process.parent_processes()
    parent_pids = set(p.uri for p in parent_processes)
    # This script can only handle the case when there is a single clustering process
    if len(parent_pids) == 1:
        cluster_proc = parent_processes[0]

        # Sample sheet
        sample_sheet = get_sample_sheet_data(cluster_proc)
        if sample_sheet:
            inputs = process.all_inputs(unique=True)
            partial_sample_sheet = extract_sample_sheet(sample_sheet, inputs)
            of = open(sample_sheet_file, 'w')
            of.write(partial_sample_sheet)
            of.close()
        else: # not actually a failure if there is no sample sheet
            print "Failed to find sample sheet"

        # use-bases-mask
        compute_bases_mask(process)

        

    else: # number of parent processes not one
        print "Invalid number of parent processes:", len(parent_pids)
        return 1



if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('pid', help="Process LIMS-ID")
    parser.add_argument('samplesheetfile', help="Sample sheet file to write")

    args = parser.parse_args()
    sys.exit(main(args.pid,args.samplesheetfile))

