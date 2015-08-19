# Utilities to get lane-based stats

# Currently only used by 50_emails.py

import re
import os
from genologics.lims import *

def get_from_files(run_dir, instrument):
    """Returns a dict indexed by lane number with values of cluster density tuples. Each
    tuple contains the raw cluster density, the PF cluster density and the PF ratio (this
    is redundant information, but various methods provide various values, so keeping all
    for maximum accuracy).
    
    Returns:
     { lane_id => (raw_density, pf_density, pf_ratio) }

    Returns merged (average) lane stats for NextSeq.
    """
    lanes = {}
    if instrument == "miseq" or instrument == "hiseq":
        # MiSeq has the Data/reports info, like HiSeq, getting clu. density from files
        pf_path = os.path.join(run_dir, "Data", "reports", "NumClusters By Lane PF.txt")
        lane_pf = parse.get_lane_cluster_density(pf_path)
        raw_path = os.path.join(run_dir, "Data", "reports", "NumClusters By Lane.txt")
        lane_raw = parse.get_lane_cluster_density(raw_path)
        for l in sorted(lane_raw.keys()):
            lanes[l] = (lane_raw[l], lane_pf[l], lane_pf[l] / lane_raw[l])

    elif instrument == "nextseq":
        run_completion = ElementTree.parse(
                os.path.join(run_dir, "RunCompletionStatus.xml")).getroot()
        clus_den = float(run_completion.find("ClusterDensity").text)
        pf_ratio = float(run_completion.find("ClustersPassingFilter").text) / 100.0
        # For merged files:
        lanes = {"X": (clus_den, clus_den * pf_ratio, pf_ratio)}

    return lanes


def get_from_lims(process, instrument):
    """Get the cluster density and PF ratio from the input analytes of the provided
    process.
    
    NextSeq lanes are combined into one, as the NextSeq flow cell container only has one
    well."""
    lanes = {}
    for lane in process.all_inputs(unique=True):
        if lane.location == "A:1" and instrument == "miseq":
            lane_id = 1
        elif lane.location == "A:1" and instrument == "nextseq":
            lane_id = "X"
        else:
            lane_id = int(re.match("(\d+):1", lane.location[1]).group(1))
        density_raw_1000 = lane.udf['Cluster Density (K/mm^2) R1']
        n_raw = lane.udf['Clusters Raw R1']
        n_pf = lane.udf['Clusters PF R1']
        density_pf_1000 = density_raw * n_pf * 1.0 / n_raw
        pf_ratio = lane.udf['%PF R1'] / 100.0
        lanes[lane_id] = (density_raw_1000 * 1000.0, density_pf_1000 * 1000.0, pf_ratio)

    return lanes

