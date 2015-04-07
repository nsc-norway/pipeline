import sys, os.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import time
from genologics.lims import *
import nsc


pid = sys.argv[1]
process = Process(nsc.lims, id = pid)
# Template completed run used: https://dev-lims.ous.nsc.local/clarity/work-complete/451
process.udf['Flow Cell Position'] = 'A'
#process.udf['Read 2 Cycles'] = ""
process.udf['Run Type'] = "Single Read Indexing Run"
process.udf['Experiment Name'] = "2015-01-28-GG-A-50bpSR-KEF"
process.udf['Output Folder'] = "Z:\\"
process.udf['Status'] = "Cycle 58 of 58"
process.udf['Read 1 Cycles'] = "51"
process.udf['Run ID'] = '150128_7001448_0281_AC6A17ANXX'
#process.udf['Control Lane'] = ''
process.udf['Index 1 Read Cycles'] = '7'
process.udf['TruSeq SBS Kit Type'] = 'TruSeq SBS Kit-HS (50 cycles)'
process.udf['Flow Cell ID'] = 'C6A17ANXX'
#process.udf['Index 2 Read Cycles'] = ''
process.udf['TruSeq SBS Kit lot #'] = 'RGT4759092, RGT4533416'
process.udf['Flow Cell Version'] = 'HiSeq Flow Cell v4'
process.udf['Finish Date'] = '2015-03-04'
process.udf['Comments'] = 'InstaFake run UDF filler'
process.put()



