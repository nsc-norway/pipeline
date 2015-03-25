# Script to be called directly by EPP when starting the HiSeq demultiplexing step. 
# Sets steering options for the demultiplexing job in UDFs on the demultiplexing 
# process. The options are set based on the input samples. Other options which 
# do not depend on the inputs should be set as defaults in the LIMS or in the 
# NSC configuration file.

import sys, os
from argparse import ArgumentParser
from genologics.lims import *
import nsc
import utilities
import logging


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


def get_paths(process, seq_process):
    try:
        run_id = seq_process.udf['Run ID']
    except:
        return None

    source_path = os.path.join(nsc.PRIMARY_STORAGE, run_id)
    project = process.all_inputs()[0].samples[0].project
    output_subdir = "Unaligned_" + process.id
    dest_path = os.path.join(nsc.SECONDARY_STORAGE, run_id, output_subdir)

    return (source_path, dest_path)



def compute_bases_mask(process, seq_proc):
    '''Compute the --use-bases-mask option for fastq conversion. 
    This option specifies how each imaging cycle is interpreted. It 
    gives the number and order of data reads and index reads.
    
    The first argument is a reference to the current process, the second
    is a reference to the corresponding sequencing process.'''


    # These are the properties of the run. The full data sequence is 
    # always returned, but we set the index length and multiplicity.
    # For this we also need to look up the properties of the input samples.  
    try:
        read_1_length = seq_proc.udf['Read 1 Cycles']
    except KeyError:
        return None

    try:
        read_2_length = seq_proc.udf['Read 2 Cycles']
    except KeyError:
        read_2_length = None

    index_1_length = index_2_length = None
    try:
        index_1_length = seq_proc.udf['Index 1 Read Cycles']
        index_2_length = seq_proc.udf['Index 2 Read Cycles']
    except KeyError:
        pass

    # Get an example index sequence from the pool. Note that it takes an input
    # from the demultiplexing process, not the sequencing, so we are sure to get
    # the right kind of index.
    index_sequence = utilities.get_index_sequence(process.all_inputs()[0])

    if index_sequence and index_sequence != 'NoIndex':
        indices = index_sequence.split('-') 
        i1length = len(indices[0])
        if len(indices) == 2:
            i2length = len(indices[1])
            pool_index_length = (i1length, i2length)
        else:
            pool_index_length = (i1length, 0)
    else:
        pool_index_length = (0, 0)

    # Always use the full read length for data reads
    use_bases_mask =  "y%d" % read_1_length
    index_reads = [index_1_length, index_2_length]
    for read_il, pool_il in zip(index_reads, pool_index_length):
        if read_il:
            use_bases_mask += ","
            if pool_il > 0:
                use_bases_mask += "I" + str(pool_il)
            use_bases_mask += "n" * (read_il - pool_il)

    if read_2_length:
        use_bases_mask += ",y%d" % read_2_length

    return use_bases_mask


def main(process_id, sample_sheet_file):
    process = Process(nsc.lims, id=process_id)
    # Get the clustering processes
    parent_processes = process.parent_processes()
    parent_pids = set(p.uri for p in parent_processes)
    # This script can only handle the case when there is a single clustering process
    if len(parent_pids) == 1:
        logging.debug('Found the right number of parent processes')
        cluster_proc = parent_processes[0]
        seq_proc = utilities.get_sequencing_process(process)

        paths = get_paths(process, seq_proc)
        if paths:
            logging.debug('Found source and destination paths')
            process.udf[nsc.SOURCE_RUN_DIR_UDF] = paths[0]
            process.udf[nsc.DEST_FASTQ_DIR_UDF] = paths[1]
        else:
            logging.debug('Unable to determine source and destination paths')

        # use-bases-mask
        base_mask = compute_bases_mask(process, seq_proc)
        if base_mask:
            logging.debug('Determined bases mask')
            process.udf[nsc.BASES_MASK_UDF] = base_mask

            # Compute number of threads for slurm job
            reads = 2
            try:
                test = seq_proc.udf['Read 2 Cycles']
            except:
                reads = 1

            n_threads = len(process.all_inputs(unique = True)) * reads
            process.udf[nsc.THREADS_UDF] = n_threads
        else:
            logging.debug('Unable to determine bases mask')

        process.put()
        logging.debug('Saved settings in the process')

        # Sample sheet
        sample_sheet = get_sample_sheet_data(cluster_proc)
        if sample_sheet:
            logging.debug('Found the sample sheet')
            inputs = process.all_inputs(unique=True)
            partial_sample_sheet = extract_sample_sheet(sample_sheet, inputs)
            logging.debug('Found the sample sheet')
            of = open(sample_sheet_file, 'w')
            of.write(partial_sample_sheet)
            of.close()
        else: # not actually a failure if there is no sample sheet
            print "Failed to find sample sheet"
        

    else: # number of parent processes not one
        print "Invalid number of parent processes:", len(parent_pids)
        return 1



if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('pid', help="Process LIMS-ID")
    parser.add_argument('samplesheetfile', help="Sample sheet file to write")

    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG)
    logging.debug('Starting "setup demultiplexing" script')
    sys.exit(main(args.pid,args.samplesheetfile))

