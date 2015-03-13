# Demultiplexing common routines

import os


def populate_udfs(process, demux_result_dir):
    '''Reads demultiplexing results and adds UDFs to samples and projects.
    
    The argument should refer to the "Unaligned" directory.'''
    statsfile = file(os.path.join(demux_result_dir, ""), 'r')
    #TODO: collect demultiplexing statistics and shove them into the lims

    return True
