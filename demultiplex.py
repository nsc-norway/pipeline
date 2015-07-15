

def download_sample_sheet(process, save_dir, append_limsid=True):
    """Downloads the demultiplexing process's sample sheet, which contains only
    samples for the requested project (added to the LIMS by setup-*-demultiplexing.py)."""

    sample_sheet = None
    for o in process.all_outputs(unique=True):
        if o.output_type == "ResultFile" and o.name == "SampleSheet csv":
            if len(o.files) == 1:
                sample_sheet = o.files[0].download()

    if sample_sheet:
        if append_limsid:
            name = "SampleSheet-" + process.id + ".csv"
        else:
            name = "SampleSheet.csv"
        path = os.path.join(save_dir, name)
        file(path, 'w').write(sample_sheet)
        return path, sample_sheet
    else:
        return None, None

