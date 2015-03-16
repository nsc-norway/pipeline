# Demultiplexing common routines

import os
from genologics.lims import *

def parse_demux_stats(stats_file):
    return None


def lookup_sample(process, sample_name):
    for i in process.all_inputs(unique=True):
        if i.name == sample_name:
            return i
    return None


def populate_results(process, demux_result_dir):
    '''Reads demultiplexing results and adds UDFs to samples and projects.
    
    The argument should refer to the "Unaligned" directory.'''
    with file(os.path.join(demux_result_dir, "TODO/Demultiplex_stats.xml"), 'r') as statsfile:
        ds = parse_demux_stats(statsfile)


    for sample in ds:
        if sample['SampleRef'] == "Undetermined indices TODO":

            limsid = sample['Description']
            if limsid:
                try:
                    # TODO: LIMSID of sample or artifact?
                    lims_sample = Artifact(process.lims, limsid)
                except:
                    pass
            if not lims_sample:
                lims_sample = lookup_sample(process, sample['SampleRef'])

            if lims_sample:
                for field in ['# Reads', 'Yield', '% PF', '% of Lane', '% Perfect Index Reads',
                        '% One Mismatch Reads (Index)', '% of >=Q30 Bases', 'Mean Quality Score']:
                    try:
                        lims_sample.udf[field] = sample[field]
                    except KeyError:
                        pass

                lims_sample.put()
            


    return True
