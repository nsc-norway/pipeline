import re
import os
import operator
from common import taskmgr, utilities, samples, stats


TOP = """<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html xmlns:casava="http://www.illumina.com/casava/alignment" xmlns:str="http://exslt.org/strings">
<link rel="stylesheet" href="css/Reports.css" type="text/css">
<body>
<h1>Flowcell: {fcid}</h1>
<h2>Barcode lane statistics</h2>
<div ID="ScrollableTableHeaderDiv"><table width="100%">
<col width="4%">
<col width="5%">
<col width="19%">
<col width="8%">
<col width="7%">
<col width="5%">
<col width="12%">
<col width="7%">
<col width="4%">
<col width="5%">
<col width="4%">
<col width="5%">
<col width="6%">
<col width="5%">
<col>
<tr>
<th>Lane</th>
<th>Sample ID</th>
<th>Sample Ref</th>
<th>Index</th>
<th>Description</th>
<th>Control</th>
<th>Project</th>
<th>Yield (Mbases)</th>
<th>% PF</th>
<th># Reads</th>
<th>% of raw clusters per lane</th>
<th>% Perfect Index Reads</th>
<th>% One Mismatch Reads (Index)</th>
<th>% of &gt;= Q30 Bases (PF)</th>
<th>Mean Quality Score (PF)</th>
</tr>
</table></div>
<div ID="ScrollableTableBodyDiv"><table width="100%">
<col width="4%">
<col width="5%">
<col width="19%">
<col width="8%">
<col width="7%">
<col width="5%">
<col width="12%">
<col width="7%">
<col width="4%">
<col width="5%">
<col width="4%">
<col width="5%">
<col width="6%">
<col width="5%">
<col>
"""

MID = """</table></div>
<p></p>
<h2>Sample information</h2>
<div ID="ScrollableTableHeaderDiv"><table width="100%">
<col width="10%">
<col width="10%">
<col width="7%">
<col>
<tr>
<th>Sample<p></p>ID</th>
<th>Recipe</th>
<th>Operator</th>
<th>Directory</th>
</tr>
</table></div>
<div ID="ScrollableTableBodyDiv"><table width="100%">
<col width="10%">
<col width="10%">
<col width="7%">
<col>
"""

BOTTOM=""" </table></div>
<p>bcl2fastq2 {version} (via NSC compatibility module)</p>
</body>
</html>
"""


def demultiplex_stats(project, undetermined_project, basecalls_dir, instrument, fcid, bcl2fastq_version):
    """Generate Demultiplexing_stats.htm file.
    
    Note: this function MODIFIES the project object tree, adding stats to the file objects,
    with aggregate_lanes. Be extremely careful if stats are used in other code calling this."""
    run_stats = stats.get_stats(
            instrument,
            task.work_dir,
            aggregate_lanes = task.no_lane_splitting,
            aggregate_reads = True,
            miseq_uniproject=project
            )

    samples.add_stats([undetermined_project, project], run_stats)
    out = TOP.format(fcid=fcid)
    project_lanes = set(f.lane for sample in project.samples for f in sample.files)
    file_sample_sorted = sorted(
            ((f, sample, p) 
                for p in (project, undetermined_project) 
                for sample in p.samples for f in sample.files
                if f.i_read==1 and f.lane in project_lanes),
            key=lambda item: (item[0].lane, item[1].sample_index == 0, item[1].sample_index)
            )
    for f, sample, p in file_sample_sorted:
        if p.is_undetermined:
            sample_name = "lane{0}".format(f.lane)
            sample_index_sequence = "Undetermined"
            sample_project = "Undetermined_indices"
            description = ""
        else:
            sample_name = sample.name
            sample_index_sequence = f.index_sequence
            sample_project = p.name
            description = sample.sample_id
        out += "<tr>\n"
        out += "<td>{0}</td>\n".format(f.lane)
        out += "<td>{0}</td>\n".format(sample_name)
        out += "<td></td>\n"
        out += "<td>{0}</td>\n".format(sample_index_sequence)
        out += "<td>{0}</td>\n".format(description)
        out += "<td>N</td>\n"
        out += "<td>{0}</td>\n".format(sample_project)
        out += "<td>{0}</td>\n".format(utilities.display_int(f.stats['Yield PF (Gb)'] * 1000.0))
        # For compatibility we pretend that there is only PF data -- 100 % PF ratio and use "% of PF" below
        out += "<td>{0:3.2f}</td>\n".format(100.0)
        out += "<td>{0}</td>\n".format(utilities.display_int(f.stats['# Reads PF']))
        out += "<td>{0:3.2f}</td>\n".format(f.stats['% of PF Clusters Per Lane'])
        if p.is_undetermined:
            out += "<td>{0:3.2f}</td>\n".format(0.0)
            out += "<td>{0:3.2f}</td>\n".format(0.0)
        else:
            out += "<td>{0:3.2f}</td>\n".format(f.stats['% Perfect Index Read'])
            out += "<td>{0:3.2f}</td>\n".format(f.stats['% One Mismatch Reads (Index)'])
        out += "<td>{0:3.2f}</td>\n".format(f.stats['% Bases >=Q30'])
        out += "<td>{0:3.2f}</td>\n".format(f.stats['Ave Q Score'])
        out += "</tr>\n"

    out += MID 
    for sample in sorted(project.samples, key=operator.attrgetter('sample_index')):
        out += "<tr>\n"
        out += "<td>{0}</td>\n".format(sample.name)
        out += "<td></td>\n"
        out += "<td>{0}</td>\n".format("Unknown")
        out += "<td>{0}</td>\n".format(os.path.join(basecalls_dir, project.proj_dir, sample.sample_dir))
        out += "</tr>\n"
    for lane in sorted(project_lanes):
        out += "<tr>\n"
        out += "<td>{0}</td>\n".format("lane{0}".format(lane))
        out += "<td></td>\n"
        out += "<td>{0}</td>\n".format("Unknown")
        out += "<td>{0}</td>\n".format(os.path.join(basecalls_dir, "Undetermined_indices", "Sample_lane{0}".format(lane)))
        out += "</tr>\n"
    out += BOTTOM.format(version=bcl2fastq_version)

    return out


def interactive(task):
    task.add_argument('PROJECT', help="Project for which to generate Demultiplex_Stats.htm")
    task.running()
    projects = task.projects
    instrument = utilities.get_instrument_by_runid(task.run_id)
    if instrument in ['hiseq', 'nextseq']:
        fcid = re.match(r"[\d]{6}_[\dA-Z]+_[\d]+_[AB]([A-Z\d]+)$", task.run_id).group(1)
    else:
        task.fail("Can't do this for the MiSeq at the moment")

    bcl2fastq_version = utilities.get_bcl2fastq2_version(task.work_dir)

    project = next(project for project in projects if not project.is_undetermined and project.name.startswith(task.args.PROJECT))
    undetermined_project = next(
            project 
            for project in projects if project.is_undetermined
            )

    print demultiplex_stats(project, undetermined_project, task.bc_dir, instrument, fcid, bcl2fastq_version)
    task.success_finish()


if __name__ == "__main__":
    with taskmgr.Task(
            "Compat module",
            "Compatibility reports (normally called by other scripts)",
            ['work_dir', 'sample_sheet']
            ) as task:
        interactive(task)


