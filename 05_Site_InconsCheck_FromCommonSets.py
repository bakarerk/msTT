from huaweiDumpLibrary import parseDBSite,excList,cellParser
from dbname import sqldbdate

###########################################
molist =["ENODEBALGOSWITCH"]
###########################################

sqldbname = sqldbdate + ".sqlite"

final_file = open("exports\\05_all_sets_distribution_" + sqldbdate +".txt","w")


#,"HoMeasComm","IntraRatHoComm","InterRatHoComm","eNodeBFlowCtrlPara","CspcAlgoPara","VQMAlgo","ServiceDiffSetting","eNodeBMlb","ParaAutoOptCfg","BlindNcellOpt","QoEHoCommonCfg","eNodeBAlmCfg","ENodeBFrameOffset","ANR","MRO","NCellClassMgt","FddResMode","eNodeBResModeAlgo","EuUlCoSchCfg","eNBRsvdPara","EuCoSchULICSCfg","ENodeBAlgoSwitch","ENodeBNbPara","CSFallBackPolicyCfg","CnOperatorHoCfg","CSFallBackBlindHoCfg","EnodebAlgoExtSwitch","RrcConnStateTimer","ENodeBConnStateTimer","PdcpRohcPara","ENodeBCipherCap","ENodeBIntegrityCap","CounterCheckPara","EnodebCounterParaGrp","ANRMeasureParam","eNodeBUSParaCfg","SrbCfg","UserPriority","UsUeSpidConfig","MultiCarrUnifiedSch","EnodebRsvdParamExt","UeTimerConfig","GlobalProcSwitch","ScPolicy","IopsCfg"]

for mo in molist:

    print(mo)

    curMO = parseDBSite(mo,sqldbname)

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
