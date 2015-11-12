from xml.etree import ElementTree
import sys
# Run information module

# For parsing runParameters.xml

# Returns a dict with keys 

def read_run_parameters(runParameters_path):
    xmltree = ElementTree.parse(runParameters_path)
    root = xmltree.getroot()
    rp = {}
    hiseq_run_mode = get_first(root, "Setup/RunMode")
    next_mi_chemistry = get_first(root, "Chemistry")

    if hiseq_run_mode:
        rp['instrument'] = 'hiseq'
        rp['runMode'] = hiseq_run_mode
        rp['chemistryVersion'] = get_first(root, "Setup/Sbs")
        rp['controlSoftware'] = get_first(root, "Setup/ApplicationName")
        rp['controlSoftwareVersion'] = get_first(root, "Setup/ApplicationVersion")
        rp['rtaVersion'] = get_first(root, "Setup/RTAVersion")

    elif next_mi_chemistry.startswith("NextSeq"):
        rp['instrument'] = 'nextseq'
        rp['runMode'] = next_mi_chemistry
        rp['chemistryVersion'] = get_first(root, "ReagentKitVersion")
        rp['controlSoftware'] = get_first(root, "Setup/ApplicationName")
        rp['controlSoftwareVersion'] = get_first(root, "Setup/ApplicationVersion")
        rp['rtaVersion'] = get_first(root, "RTAVersion")
    else:
        rp['instrument'] = 'miseq'
        rp['runMode'] = next_mi_chemistry
        rp['runMode'] = next_mi_chemistry
        rp['chemistryVersion'] = get_first(root, "ReagentKitVersion")
        rp['controlSoftware'] = "MiSeq Control Software"
        rp['controlSoftwareVersion'] = get_first(root, "Setup/ApplicationVersion")
        rp['rtaVersion'] = get_first(root, "RTAVersion")

    if rp['instrument'] in ('hiseq', 'miseq'):
        rp['reads'] = [
                (int(el.attrib['NumCycles']), el.attrib['IsIndexedRead'] == "Y")
                for el in 
                    sorted(
                        root.findall("Reads/Read") + root.findall("Reads/RunInfoRead"),
                        key=lambda x: int(x.attrib["Number"])
                        )
                    ]

    else:
        data_read = [int(root.findall("Setup/Read{0}".format(i)).text) for i in [1,2]]
        index_read = [int(root.findall("Setup/IndexRead{0}".format(i)).text) for i in [1,2]]
        rp['reads'] = [(data_read[0], False)]
        for ir in index_read:
            if ir > 0:
                rp['reads'].append((ir, True))
        if data_read[1] > 0:
            rp['reads'].append((data_read[1], False))

    return rp


def get_first(root, path):
    for runmode in root.findall(path):
        return runmode.text
    return None


if __name__ == "__main__":
    print read_run_parameters(sys.argv[1])

