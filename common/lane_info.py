# Utilities to get lane-based stats

# Currently only used by 50_emails.py

import re
import os
from collections import defaultdict
from xml.etree import ElementTree
from genologics.lims import *

class LaneStats(object):
    def __init__(self, cluster_den_raw, cluster_den_pf, pf_ratio, phix=None, occupancy=None):
        self.cluster_den_raw = cluster_den_raw
        self.cluster_den_pf = cluster_den_pf
        self.pf_ratio = pf_ratio
        self.phix = phix
        self.occupancy = occupancy

class NotSupportedException(Exception):
    pass

def get_from_interop(run_dir, merge_lanes=False):
    """Get cluster density and PF ratio from the "InterOp" files. (binary
    run statistics format)

    Uses the interop library from Illumina to parse the files.
    """

    try:
        from interop import py_interop_run_metrics, py_interop_run, py_interop_summary
    except ImportError as e:
        raise NotSupportedException("Could not import the interop library (Illumina)", e)

    # Setup code: This specifies necessary files to load for summary metrics
    valid_to_load = py_interop_run.uchar_vector(py_interop_run.MetricCount, 0)
    py_interop_run_metrics.list_summary_metrics_to_load(valid_to_load)
    valid_to_load[py_interop_run.ExtendedTile] = 1
    run_metrics = py_interop_run_metrics.run_metrics()
    run_metrics.read(run_dir, valid_to_load)
    summary = py_interop_summary.run_summary()
    py_interop_summary.summarize_run_metrics(run_metrics, summary)
    extended_tile_metrics = run_metrics.extended_tile_metric_set()

    read_count = summary.size()
    lane_count = summary.lane_count()
    if lane_count == 0:
        raise RuntimeError("InterOp data appears to be corrupted: the number of lanes is zero.")
    result = {}
    raw_density, pf_density, occu_pct_sum = 0, 0, 0
    phix_pct = [] # We report PhiX % per read R1 / R2 (non-index)
    for lane in range(lane_count):
        nonindex_read_count = 0
        if not merge_lanes:
            raw_density, pf_density, occu_pct_sum = 0, 0, 0
            phix_pct = []
        for read in range(read_count):
            read_data = summary.at(read)
            if not read_data.read().is_index():
                data = read_data.at(lane)
                raw_density += data.density().mean()
                pf_density += data.density_pf().mean()
                if nonindex_read_count >= len(phix_pct):
                    phix_pct.append(data.percent_aligned().mean())
                else:
                    phix_pct[nonindex_read_count] += data.percent_aligned().mean() * 1.0
                nonindex_read_count += 1
        occupancy_lane_metrics = extended_tile_metrics.metrics_for_lane(data.lane())
        if not occupancy_lane_metrics.empty():
            occu_pct_sum += sum(occupancy_lane_metrics[i].percent_occupied() for i in
                                    range(occupancy_lane_metrics.size())) \
                                            / occupancy_lane_metrics.size()
        if not merge_lanes:
            result[data.lane()] = LaneStats(
                        raw_density / nonindex_read_count,
                        pf_density / nonindex_read_count,
                        pf_density / max(1, raw_density),
                        phix_pct,
                        occu_pct_sum
                        )
    if merge_lanes:
        result["X"] = LaneStats(
                    raw_density / (lane_count * nonindex_read_count),
                    pf_density / (lane_count * nonindex_read_count),
                    pf_density / max(1, raw_density),
                    [phix_r / lane_count for phix_r in phix_pct],
                    occu_pct_sum / lane_count
                    )
    return result


def get_r1r2_udf(artifact, udf_base_name, transform=lambda x: x):
    denominator = 0.0
    value = 0.0
    if (udf_base_name + " R1") in  artifact.udf:
        denominator += 1.0
        value += artifact.udf.get(udf_base_name + " R1")
    if (udf_base_name + " R2") in  artifact.udf:
        denominator += 1.0
        value += artifact.udf.get(udf_base_name + " R2")
    if denominator == 0.0:
        return None
    else:
        return transform(value / denominator)


def get_r1r2_udf_list(artifact, udf_base_name):
    result = []
    if (udf_base_name + " R1") in  artifact.udf:
        result.append(artifact.udf.get(udf_base_name + " R1"))
    if (udf_base_name + " R2") in  artifact.udf:
        var = artifact.udf.get(udf_base_name + " R2")
        if var is not None:
            result.append(var)
    return result or None


def get_from_lims(process, instrument, expand_lanes=None):
    """Get the cluster density and PF ratio from the input analytes of the provided
    process.
    
    NextSeq lanes are combined into one, as the NextSeq flow cell container only has one
    well. We support expanding this to 4 lanes again using the expand_lanes argument."""
    lanes = {}
    for lane in process.all_inputs(unique=True):

        # Support multiple lane IDs for each analyte, to take care of the NextSeq
        if lane.location[1] == "A:1" and instrument == "miseq":
            lane_ids = [1]
        elif lane.location[1] == "A:1" and instrument == "nextseq":
            if expand_lanes:
                lane_ids = [1,2,3,4]
            else:
                lane_ids = "X"
        else:
            lane_ids = [int(re.match("(\d+):1", lane.location[1]).group(1))]

        # Get info for this lane (or lanes, for expand_lanes)
        for lane_id in lane_ids:
            density_raw_1000 = get_r1r2_udf(lane, 'Cluster Density (K/mm^2)')
            n_raw = get_r1r2_udf(lane, 'Clusters Raw')
            n_pf = get_r1r2_udf(lane, 'Clusters PF')
            if not None in [n_raw, n_pf, density_raw_1000] and n_raw != 0.0:
                density_pf_1000 = int(density_raw_1000 * n_pf * 1.0 / n_raw)
            else:
                density_pf_1000 = None
            # The following UDFs are divided by 100 only if a value is found (otherwise: None)
            pf_ratio = get_r1r2_udf(lane, '%PF')
            phix = get_r1r2_udf_list(lane, '% Aligned')
            
            lanes[lane_id] = LaneStats(density_raw_1000 * 1000.0, density_pf_1000 * 1000.0, pf_ratio, phix)

    return lanes

