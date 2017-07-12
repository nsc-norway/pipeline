# Utilities to get lane-based stats

# Currently only used by 50_emails.py

import re
import os
from collections import defaultdict
from xml.etree import ElementTree
from genologics.lims import *

class LaneStats(object):
    def __init__(self, cluster_den_raw, cluster_den_pf, pf_ratio, phix):
        self.cluster_den_raw = cluster_den_raw
        self.cluster_den_pf = cluster_den_pf
        self.pf_ratio = pf_ratio
        self.phix = phix

class NotSupportedException(Exception):
    pass

def get_lane_cluster_density(path):
    """Get cluster density for lanes from report files in Data/reports.

    Returns a dict indexed by the 1-based lane number."""

    with open(path) as f:
        # Discard up to and including Lane
        while not next(f).startswith("Lane\t"):
            pass

        lane_sum = defaultdict(int)
        lane_ntile = defaultdict(int)
        for l in f:
            cell = l.split("\t")
            lane_sum[int(cell[0])] += float(cell[2])
            lane_ntile[int(cell[0])] += 1

        return dict((i+1, lane_sum[i] / lane_ntile[i]) for i in lane_sum.keys())


def get_from_files(run_dir, instrument, expand_lanes=False):
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
        lane_pf = get_lane_cluster_density(pf_path)
        raw_path = os.path.join(run_dir, "Data", "reports", "NumClusters By Lane.txt")
        lane_raw = get_lane_cluster_density(raw_path)
        for l in sorted(lane_raw.keys()):
            lanes[l] = LaneStats(lane_raw[l], lane_pf[l], lane_pf[l] / lane_raw[l], None)

    elif instrument == "nextseq":
        run_completion = ElementTree.parse(
                os.path.join(run_dir, "RunCompletionStatus.xml")).getroot()
        clus_den = float(run_completion.find("ClusterDensity").text)
        pf_ratio = float(run_completion.find("ClustersPassingFilter").text) / 100.0
        # For merged files:
        if expand_lanes:
            lane_ids = [1,2,3,4]
        else:
            lane_ids = ["X"]

        lanes = dict(
                (lane_id, LaneStats(clus_den, clus_den * pf_ratio, pf_ratio, None))
                for lane_id in lane_ids
                )
    else:
        # HiSeq 3000/4000/X Lane statistics is not implemented here. Could use
        # illuminate library to parse interop files, but maybe it is sufficient to
        # use LIMS.
        lanes = dict(
                (lane_id, LaneStats(None, None, None, None))
                for lane_id in [1,2,3,4,5,6,7,8]
                )

    return lanes


def get_from_interop(run_dir, merge_lanes=False):
    """Get cluster density and PF ratio from the "InterOp" files. (binary
    run statistics format)

    Uses the Illuminate library to parse the files. This creates a Pandas dataframe,
    from which the relevant metrics are easily obtained.
    """

    try:
        import illuminate
    except ImportError:
        raise NotSupportedException

    dataset = illuminate.InteropDataset(run_dir)
    df = dataset.TileMetrics().df
    if merge_lanes:
        means = df[df.code.isin((100,101,300))].groupby(by=df.code).mean()
    else:
        means = df[df.code.isin((100,101,300))].groupby(by=(df.lane,df.code)).mean()

    raw = means[means.code==100].value.values
    pf = means[means.code==101].value.values
    phix = means[means.code==300].value.values / 100.0

    if merge_lanes:
        lanes = dict([("X", LaneStats(raw[0], pf[0], pf[0] / raw[0], phix[0]))])
    else:
        lane_id = means[means.code==100]['lane'].values.astype('uint64')
        lanes = dict(zip(lane_id, (LaneStats(*args) for args in zip(raw, pf, pf/raw, phix))))

    return lanes


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
            try:
                density_raw_1000 = lane.udf['Cluster Density (K/mm^2) R1']
                n_raw = lane.udf['Clusters Raw R1']
                n_pf = lane.udf['Clusters PF R1']
                density_pf_1000 = int(density_raw_1000 * n_pf * 1.0 / n_raw)
                pf_ratio = lane.udf['%PF R1'] / 100.0
                phix = lane.udf['% Aligned R1'] / 100.0
                lanes[lane_id] = LaneStats(density_raw_1000 * 1000.0, density_pf_1000 * 1000.0, pf_ratio, phix)
            except KeyError: # Missing data in LIMS, proceed anyway
                lanes[lane_id] = LaneStats(None, None, None, None)

    return lanes

