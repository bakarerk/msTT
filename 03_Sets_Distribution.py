from huaweiDumpLibrary import parseDBSite,excList,cellParser
from dbname import sqldbdate

###########################################
molist = ["RRCTRLSWITCH"]
filenameEK = "RRCTRLSWITCH"
###########################################

sqldbname = sqldbdate + ".sqlite"

final_file = open("exports\\03_sets_distribution_" + sqldbdate + "_" + filenameEK + ".txt","w")
final_file_freq = open("exports\\03_sets_distribution_" + sqldbdate + "_" + filenameEK + "_freq.txt","w")



for mo in molist:
    print(mo)

    curMO = cellParser(mo,sqldbname)

    for cols in curMO.columns.values:

        if cols not in excList:

            newDF = curMO.groupby([cols],sort=True).count()
            paraDict = newDF.iloc[:,1].to_dict()
            if "FREQ" in curMO.columns.values:
                freqBasedDF = curMO[[cols, "FREQ"]].groupby(["FREQ",cols]).size()#EUARFCN
                freqBasedDict = freqBasedDF.to_dict()
            else:
                freqBasedDict = {}
            if len(paraDict.keys()) > 1:
                result = "NOK"
            else:
                result = "OK"

            if "&" not in str(paraDict):
                for parameter,count in paraDict.items():
                    final_file.write("{}\t{}\t{}\t{}\t{}\t{:.2f}\t{}\n".format(mo,cols,parameter,count,sum(paraDict.values()),100*count/sum(paraDict.values()),result))
                if len(freqBasedDict.keys())>0:
                    for parameter,count in freqBasedDict.items():
                        final_file_freq.write("{}\t{}\t{}\t{}\t{}\t{:.2f}\t{}\n".format(mo,cols,parameter,count,sum(paraDict.values()),100*count/sum(paraDict.values()),result))

final_file.close()
final_file_freq.close()
