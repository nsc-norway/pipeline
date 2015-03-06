import argparse
import subprocess


from genologics.lims import *
import nsc
import utilities


hiseq_exclude_paths = ["/Thumbnail_Images",
        "/Data/Intensities/L00*",
        "/Data/Intensities/BaseCalls/L00*"
        ]

nextseq_exclude_paths = ["TODO" ]


def rsync(source_path, destination_path, exclude):
    '''Runs the rsync command. Note that trailing slashes
    on paths are significant.'''

    args = [nsc.RSYNC]
    args += ["--exclude=" + path for path in exclude]
    args += [source_path, destination_path]
    code = subprocess.check_call(args)
    return code


def fix_source(source):
    return source # todo: check what lims says | remove trailing slash


def main(id = process, instrument, source_path):

    process = Process(nsc.lims, id = process_id)
    process.udf['Status'] = 'Running...'
    process.put()

    if instrument == "hiseq" or instrument == "nextseq":
        destination = nsc.SECONDARY_STORAGE
        source = fix_source(source_path)
        if instrument == "hiseq":
            exclude = hiseq_exclude_paths
        elif instrument == "nextseq":
            exclude = nextseq_exclude_paths
        command_ok = rsync(source, destination, exclude)
    elif instrument == "miseq":
        command_ok = False

    if command_ok:
        process.udf['Status'] = 'Finished'
    else:
        process.udf['Status'] = 'Error'
    process.put()


    


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--pid", help="Process ID")
    parser.add_argument('--instrument', help="Instrument type")
    parser.add_argument('source_path', help="Source path, as given in the LIMS")
    # Destination is configured by nsc config file (I think this is
    # more flexible)

    args = parser.parse_args()

    main(args.pid, args.source_path)

