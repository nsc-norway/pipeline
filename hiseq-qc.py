# Quality control script for HiSeq

# This is a meta-script that calls various QC and reporting
# modules. (TODO)


import argparse




def main(process_id):
    pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--pid", help="Process ID", required=True)
    args = parser.parse_args()

    main(args-pid)
