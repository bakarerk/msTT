from huaweiDumpLibrary import parseDBSite,excList,distributionCalc,groupidList
from dbname import sqldbdate

###########################################
#molist = ["CELLHSUPA","CELLINTERFREQHOCOV","CELLHSDPCCH","CELLMCLDR","CELLU2LTEHONCOV","CELLINTERRATHOCOV"]
molist =["CELLCOUNTERPARAGROUP"]#,"TimeAlignmentTimer","CAMGTCFG","CELLALGOEXTSWITCH","CELLALGOSWITCH","CELLCHPWRCFG","CELLCQIADJALGO","CELLCSPCPARA","CELLDLSCHALGO","CELLDRXPARA","CELLEICIC","CELLHOPARACFG","CELLPCALGO","CELLPDCCHALGO","CELLRACHALGO","CELLUCIONPUSCHPARA","CELLULPCCOMM","CELLULPCDEDIC","CELLULSCHALGO","ENODEBALGOSWITCH","ENODEBCONNSTATETIMER","GLOBALPROCSWITCH","HOMEASCOMM","INTRARATHOCOMM","IPGUARD","NCELLDLRSRPMEASPARA","PCCHCFG","PUCCHCFG","PUSCHCFG","RRCCONNSTATETIMER","S1","SCPOLICY","TCPMSSCTRL","TIMEALIGNMENTTIMER","UECOOPERATIONPARA","VQMALGO","WTCPPROXYALGO"]#,"ENODEBALGOSWITCH"]#,"NBMPARA","FRCCHLTYPEPARA","FRC","RRCESTCAUSE","DCCC","HSDPCCH","UCSVOICEPPC","STATETIMER","CONNMODETIMER"]#,"ENodeBAlgoSwitch","InterRatPolicyCfgGroup","InterRatHoUtranGroup","CellHoParaCfg","QciPara"]#,"CELLU2LTEHONCOV"]
filenameEK = "CELLCOUNTERPARAGROUP"
########################################### s

sqldbname = sqldbdate + ".sqlite"

final_file = open("exports\\03_sets_distribution_" + sqldbdate + "_" + filenameEK + ".txt","w")
final_file_freq = open("exports\\03_sets_distribution_" + sqldbdate + "_" + filenameEK + "_freq.txt","w")



for mo in molist:
    print(mo)

    curMO = distributionCalc(mo,sqldbname)

    for cols in curMO.columns.values:

        if cols not in excList:

            #group based için
            crossed = set(groupidList)&set(curMO.columns.values)
            crossed_list = list(crossed)
            if len(crossed) > 0:
                crossed_list.insert(0,cols)
                #crossed_list.insert(-1,"FREQ")
                newDF =curMO[crossed_list].groupby(crossed_list).size()#groupid
                paraDict = newDF.to_dict()
            #group based değilse
            else:
                newDF = curMO.groupby([cols], sort=True).count()
                paraDict = newDF.iloc[:, 1].to_dict()
                crossed_list = "Parameter"


            #freq based için
            if "FREQ" in curMO.columns.values:
                freqBasedDF = curMO[[cols, "FREQ"]].groupby(["FREQ",cols]).size()#EUARFCN
                freqBasedDict = freqBasedDF.to_dict()
            else:
                freqBasedDict = {}
            if len(paraDict.keys()) > 1:
                result = "NOK"
            else:
                result = "OK"
            #başlık
            final_file.write("MO\tPARAMETER\t{}\tValue\tCount\tTotalNE\t%Dist\tResult\n".format(crossed_list[1:]))
            if "&" not in str(paraDict):
                for parameter,count in paraDict.items():
                    if crossed_list == "Parameter":
                        final_file.write("{}\t{}\t{}\t{}\t{}\t{}\t{:.2f}\t{}\n".format(mo,cols,"-",parameter,count,sum(paraDict.values()),100*count/sum(paraDict.values()),result))
                        print("{}\t{}\t{}\t{}\t{}\t{}\t{:.2f}\t{}".format(mo,cols,"-",parameter,count,sum(paraDict.values()),100*count/sum(paraDict.values()),result))
                    else:
                        final_file.write("{}\t{}\t{}\t{}\t{}\t{}\n".format(mo, cols, parameter[1:],parameter[0], count,sum(paraDict.values())))
                        print("{}\t{}\t{}\t{}\t{}\t{}".format(mo, cols, parameter[1:],parameter[0], count,sum(paraDict.values())))
                        #print("{}\t{}\t{}{}\t\t{}\t{}\t{}\t{:.2f}\t{}".format(mo, cols, parameter[:-1],parameter[-1], count,sum(paraDict.values()),100 * count / sum(paraDict.values()), result))

                if len(freqBasedDict.keys())>0:
                    for parameter,count in freqBasedDict.items():
                        final_file_freq.write("{}\t{}\t{}\t{}\t{}\t{:.2f}\t{}\n".format(mo,cols,parameter,count,sum(paraDict.values()),100*count/sum(paraDict.values()),result))

final_file.close()
final_file_freq.close()
