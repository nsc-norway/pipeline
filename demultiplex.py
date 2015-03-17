# Demultiplexing common routines

import os
from genologics.lims import *


stats = [
        (('# Reads'), '# Reads'),
        (('Yield'), 'Yield PF (Gb)'),
        (('% PF'), '%PF'), 
        (('% of Lane'), '% of Raw Clusters Per Lane'),
        (('% Perfect Index Reads'), '% Perfect Index Read'),
        (('% One Mismatch Reads (Index)'), '% One Mismatch Reads (Index)'),
        (('% of >=Q30 Bases'), '% Bases >=Q30'),
        (( 'Mean Quality Score'), 'Ave Q Score')
        ]


def parse_demux_stats(stats_file):
    return None


def lookup_sample(process, sample_name):
    for i in process.all_inputs(unique=True):
        if i.name == sample_name:
            return i
    return None



def populate_results(process, demux_result_dir):
    '''Reads demultiplexing results and adds UDFs to artifacts.
    
    The demux_result_dir argument should refer to the "Unaligned" directory.'''

    statsfile_name = os.path.join(demux_result_dir, "TODO", "Demultiplex_stats.xml")
    statfile = open(statfile_name)
    shared_out_files = process.shared_result_files()
    for f in shared_out_files:
        if f.name == nsc.DEMULTIPLEX_STATS_FILE:
            f.upload(statfile.read())
            statfile.seek(0)

    ds = parse_demux_stats(statsfile)

    for sample in ds:
        if sample['SampleRef'] == "Undetermined indices TODO":

            limsid = sample['Description']
            lims_sample = None
            if limsid:
                try:
                    # TODO: LIMSID of sample or artifact?
                    lims_sample = Artifact(process.lims, limsid)
                except:
                    pass
            if not lims_sample:
                lims_sample = lookup_sample(process, sample['SampleRef'])

            if lims_sample:
                for statname, udfname in stats:
                    for st in statname:
                        try:
                            lims_sample.udf[udfname] = sample[st]
                        except KeyError:
                            pass

                lims_sample.put()

    return True

