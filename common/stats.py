# Parsing of bcl2fastq2 output

import re
import os
import json
import utilities
import samples

def get_stats(
        _,  # Instrument
        run_dir,
        aggregate_lanes=True,
        aggregate_reads=False,
        suffix=""
        ):
    stats_file_path = os.path.join(run_dir, "Data", "Intensities", "BaseCalls", "Stats" + suffix, "Stats.json")
    with open(stats_file_path) as  statsfile:
        stats = json.load(statsfile)

    sums = {}
    lane_stats = {}
    for conversion_result in stats['ConversionResults']:

        lane_number = conversion_result['LaneNumber']
        if aggregate_lanes: lane_number = "X"
        lane_metrics = lane_stats.get(lane_number, {
            'TotalClustersRaw': 0,
            'TotalClustersPF': 0
        })
        lane_metrics['TotalClustersRaw'] += conversion_result['TotalClustersRaw']
        lane_metrics['TotalClustersPF'] += conversion_result['TotalClustersPF']
        lane_stats[lane_number] = lane_metrics

        demux_result_list = [(False, dr) for dr in conversion_result['DemuxResults']]
        if 'Undetermined' in conversion_result:
            demux_result_list.append((True, conversion_result['Undetermined']))

        for is_undetermined, demux_result in demux_result_list:
            if is_undetermined:
                sample_id = None
            else:
                sample_id = demux_result['SampleId']

            for read_metrics in demux_result['ReadMetrics']:
                read_number = read_metrics['ReadNumber']
                if aggregate_reads: read_number = 1
                coordinates = (lane_number, sample_id, read_number)

                data = sums.get(coordinates, {
                    'NumberClusters': 0, 'NumberReads': 0, 'Yield': 0,
                    'YieldQ30': 0, 'QualityScoreSum': 0,
                    'Mismatch0': None, 'Mismatch1': 0
                })
                if read_metrics['ReadNumber'] == 1 or (not aggregate_reads):
                    data['NumberClusters'] += demux_result['NumberReads']
                data['NumberReads'] += demux_result['NumberReads']
                data['Yield'] += read_metrics['Yield']
                data['YieldQ30'] += read_metrics['YieldQ30']
                data['QualityScoreSum'] += read_metrics['QualityScoreSum']
                if 'IndexMetrics' in demux_result:
                    mm0 = sum(im['MismatchCounts']['0'] for im in demux_result['IndexMetrics'])
                    if data['Mismatch0'] is not None:
                        data['Mismatch0'] += mm0
                    else:
                        data['Mismatch0'] = mm0
                    data['Mismatch1'] += sum(im['MismatchCounts']['1'] for im in demux_result['IndexMetrics'])
                sums[coordinates] = data

    results = {}
    for coordinates, csums in sums.items():
        results[coordinates] = {
            '# Reads PF': csums['NumberReads'],
            'Yield PF (Gb)': csums['Yield'] / 1e9,
            '% PF': 100.0,
            '% of Raw Clusters Per Lane': csums['NumberClusters'] * 100.0 / lane_stats[coordinates[0]]['TotalClustersRaw'],
            '% of PF Clusters Per Lane': csums['NumberClusters'] * 100.0 / lane_stats[coordinates[0]]['TotalClustersPF'],
            '% One Mismatch Reads (Index)': csums['Mismatch1'] * 100.0 / csums['NumberReads'],
            '% Bases >=Q30': csums['YieldQ30'] * 100.0 / csums['Yield'],
            'Ave Q Score': csums['QualityScoreSum'] * 1.0 / csums['Yield']
        }
        if csums['Mismatch0'] is None: # Reproduce previous behaviour -- 100% if there are no IndexMetrics
            results[coordinates]['% Perfect Index Read'] = 100
        else:
            results[coordinates]['% Perfect Index Read'] = csums['Mismatch0'] * 100.0 / csums['NumberReads']

    return results


###################### Other metrics #######################

def add_duplication_results(qc_dir, projects):
    for project in projects:
        for sample in project.samples:
            for f in sample.files:
                if f.i_read == 1 and not f.empty: # Don't need the empty flag, but skip it 
                                                  # quickly when we know it's empty
                    try:
                        path = os.path.join(qc_dir, samples.get_fastdup_path(project, sample, f))
                        with open(path) as metrics_file:
                            lines = [line.strip().split("\t") for line in metrics_file.readlines()]
                            assert lines[0][0:2] == ["NUM_READS", "READS_WITH_DUP"]
                            num_reads, reads_with_dup = [int(v) for v in lines[1][0:2]]
                            stats = f.stats or dict()
                            stats['% Sequencing Duplicates'] = reads_with_dup * 100.0 / num_reads
                            stats['fastdup reads with duplicate'] = reads_with_dup
                            stats['fastdup reads analysed'] = num_reads
                            f.stats = stats
                    except IOError:
                        pass

### Command-line mode: dump run stats
if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
            print "Use: dump_stats.py RUN_DIR"
            sys.exit(1)

    run_folder = sys.argv[1]
    run_id = os.path.basename(run_folder)
    instrument = utilities.get_instrument_by_runid(run_id)
    print get_stats(instrument, run_folder)
