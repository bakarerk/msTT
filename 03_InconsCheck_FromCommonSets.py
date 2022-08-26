from huaweiDumpLibrary import parseDBSite,excList,cellParser


###########################################
sqldbdate = "20220823"
molist = ["ENODEBALGOSWITCH","CAMGTCFG","CELLINTERFREQHOCOV"]
###########################################

sqldbname = sqldbdate + ".sqlite"

final_file = open("exports\inconsistency2" + sqldbdate +".txt","w")
final_file_freq = open("exports\inconsistency2" + sqldbdate +"_freq.txt","w")



for mo in molist:
    print(mo)

    curMO = cellParser(mo,sqldbname)

    if str(curMO) != "-1":

        for cols in curMO.columns.values:

            if cols not in excList:

                newDF = curMO.groupby([cols],sort=True).count()
                paraDict = newDF.iloc[:,1].to_dict()
                freqBasedDF = curMO[[cols, "FREQ"]].groupby(["FREQ",cols]).size()#EUARFCN
                freqBasedDict = freqBasedDF.to_dict()
                if len(paraDict.keys()) > 1:
                    result = "NOK"
                else:
                    result = "OK"

                if "&" not in str(paraDict):
                    for parameter,count in paraDict.items():
                        final_file.write("{}/t{}/t{}/t{}/t{}/t{:.2f}/t{}\n".format(mo,cols,parameter,count,sum(paraDict.values()),100*count/sum(paraDict.values()),result))
                    for parameter,count in freqBasedDict.items():
                        final_file_freq.write("{}/t{}/t{}/t{}/t{}/t{:.2f}/t{}\n".format(mo,cols,parameter,count,sum(paraDict.values()),100*count/sum(paraDict.values()),result))

final_file.close()
final_file_freq.close()

'''

molist =["CqiAdaptiveCfg","HoMeasComm","IntraRatHoComm","InterRatHoComm","eNodeBFlowCtrlPara","CspcAlgoPara","VQMAlgo","ServiceDiffSetting","eNodeBMlb","ParaAutoOptCfg","BlindNcellOpt","QoEHoCommonCfg","eNodeBAlmCfg","ENodeBFrameOffset","ANR","MRO","NCellClassMgt","FddResMode","eNodeBResModeAlgo","EuUlCoSchCfg","eNBRsvdPara","EuCoSchULICSCfg","ENodeBAlgoSwitch","ENodeBNbPara","CSFallBackPolicyCfg","CnOperatorHoCfg","CSFallBackBlindHoCfg","EnodebAlgoExtSwitch","RrcConnStateTimer","ENodeBConnStateTimer","PdcpRohcPara","ENodeBCipherCap","ENodeBIntegrityCap","CounterCheckPara","EnodebCounterParaGrp","ANRMeasureParam","eNodeBUSParaCfg","SrbCfg","UserPriority","UsUeSpidConfig","MultiCarrUnifiedSch","EnodebRsvdParamExt","UeTimerConfig","GlobalProcSwitch","ScPolicy","IopsCfg"]

for mo in molist:

    print(mo)

    curMO = parseDBSite(mo)

    for cols in curMO.columns.values:

        if cols not in excList:

            newDF = curMO.groupby([cols],sort=True).count()
            paraDict = newDF.iloc[:,1].to_dict()

            if "&" not in str(paraDict):

                if len(paraDict.keys()) > 1:
                    result = "NOK"
                else:
                    result = "OK"

                final_file.write("{}|{}|{}|{}\n".format(mo,cols,paraDict,result))

final_file.close()
'''