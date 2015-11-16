from xml.etree import ElementTree
import sys
# Run information module

# For parsing runParameters.xml

# Returns a dict with keys 
import re 

def read_run_parameters(runParameters_path):
    xmltree = ElementTree.parse(runParameters_path)
    root = xmltree.getroot()
    rp = {}
    hiseq_run_mode = get_first(root, "Setup/RunMode")
    next_mi_chemistry = get_first(root, "Chemistry")

    if hiseq_run_mode:
        rp['instrument'] = 'hiseq'
        rp['runMode'] = hiseq_run_mode
        rp['sbs'] = get_first(root, "Setup/Sbs")
        rp['controlSoftwareName'] = get_first(root, "Setup/ApplicationName")
        rp['controlSoftwareVersion'] = get_first(root, "Setup/ApplicationVersion")
        rp['rtaVersion'] = get_first(root, "Setup/RTAVersion")

    elif next_mi_chemistry.startswith("NextSeq"):
        rp['instrument'] = 'nextseq'
        rp['chemistry'] = next_mi_chemistry
        rp['chemistryVersion'] = get_first(root, "ReagentKitVersion")
        rp['controlSoftwareName'] = get_first(root, "Setup/ApplicationName")
        rp['controlSoftwareVersion'] = get_first(root, "Setup/ApplicationVersion")
        rp['rtaVersion'] = get_first(root, "RTAVersion")
    else:
        rp['instrument'] = 'miseq'
        rp['chemistry'] = next_mi_chemistry
        rp['chemistryVersion'] = get_first(root, "ReagentKitVersion")
        rp['controlSoftwareName'] = "MiSeq Control Software"
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
        data_read = [int(root.findall("Setup/Read{0}".format(i))[0].text) for i in [1,2]]
        index_read = [int(root.findall("Setup/Index{0}Read".format(i))[0].text) for i in [1,2]]
        rp['reads'] = [(data_read[0], False)]
        for ir in index_read:
            if ir > 0:
                rp['reads'].append((ir, True))
        if data_read[1] > 0:
            rp['reads'].append((data_read[1], False))

    # Derived values
    rp['sequencingChemistry'] = get_sequencing_chemistry_string(rp)

    return rp


def get_sequencing_chemistry_string(rp):
    if rp['instrument'] == 'hiseq':
        return rp['sbs']
    elif rp['instrument'] == 'nextseq':
        if rp['chemistry'] in ("NextSeq High", "NextSeq Mid"):
            return rp['chemistry'] + " Output"
        else:
            raise ValueError("Unknown NextSeq chemistry '" + str(rp['chemistry']) + "', please update this code")
    elif rp['instrument'] == 'miseq':
        version = re.match(r"Version(\d+)", rp['chemistryVersion'])
        if version:
            return "MiSeq v{0}".format(version.group(1))
        else:
            raise ValueError("Don't know how to report MiSeq chemistry version " + repr(rp['chemistryVersion']))
    else:
        raise ValueError("Unknown instrument " + repr(rp['instrument']))


def get_first(root, path):
    for runmode in root.findall(path):
        return runmode.text
    return None


if __name__ == "__main__":
    print read_run_parameters(sys.argv[1])

