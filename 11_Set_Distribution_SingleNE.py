from huaweiDumpLibrary import parseDBSite,excList,distributionCalcSingleSite,groupidList
from dbname import sqldbdate

###########################################
#molist = ["CELLHSUPA","CELLINTERFREQHOCOV","CELLHSDPCCH","CELLMCLDR","CELLU2LTEHONCOV","CELLINTERRATHOCOV"]
molist =["NBMPARA","NBMPARA","FRCCHLTYPEPARA","FRC","RRCESTCAUSE","DCCC","HSDPCCH","UCSVOICEPPC","STATETIMER","CONNMODETIMER"]#,"ENodeBAlgoSwitch","InterRatPolicyCfgGroup","InterRatHoUtranGroup","CellHoParaCfg","QciPara"]#,"CELLU2LTEHONCOV"]
molist = ["Cell","CellOp","CellReselGeran","CellReselUtran","CellSel","CellResel","CellAcBar","UeTimerConst","CellAccess","CellUlpcComm","TimeAlignmentTimer","CellSiMap","UlGuardBandCfg","eNBCellOpRsvdPara","CSFallBackHo","CellEmtcAlgo","CellCeSchCfg","CellCeCfg","CeRachCfg","CellDynPowerSharing","PrbRachCeConfig","PrbPdcchCeConfig","PrbUlSchCeAlgo","PrbDlSchCeAlgo","EuPrbSectorEqm","SectorSplitCell","PrbDlGapConfig","PrbSchConfig","EuPrbSectorEqmGroup","PrbSectorEqmGrpItem","CellNprsCfg","NrNRelationship","NrNFreq","EmtcSibConfig","CellAuxDuBind","CellAuxEnbBind","CellMbsfnSfEnhConfig","CellReselToNr","NbCellMultiPrbConfig","CellIntrfCtrlParam","EmtcCeSchCfg","CellMultiCarrUniSch","CellIntelAmcConfig","SymbolPwrSaving","CellUacBar","ContigIntraBandCarr","AbnRrcAccessCfg","CellEdtConfig","CellSmartPwrLock","CellEnhCoverage","CellMbfcs","InterVendorMlbAlgo","CellShortTtiAlgo","CellAlgoExtSwitch","VolteAlgoConfig","CellAutoShutdown","EuCellAlmCfg","NCellSrsMeasPara","CellRbReserve","HighSpdAdaptionPara","UlCsAlgoPara","UeCooperationPara","NCellParaCfg","CellPrbValMlb","CellULUnifiedOLC","CellSimuLoad","SfnEdgeRruRelation","SuperCombCell","DdCellGroup","CellPrsCfg","VoiceAmrControl","CellTtiBundlingAlgo","CLZeroBufferzone","NCellDlRsrpMeasPara","UserVpfmPara","CellEABAlgoPara","CellRachCECfg","RruJointCalParaCfg","CellMlbUeSel","WtcpProxyAlgo","CellFrameOffset","UpPTSInterfCfg","CellRelayAlgoPara","CellBackOff","SimuloadPmiTemplate","CellCounterParaGroup","CellRicAlgo","CellCsiRsParaCfg","CellRfShutdown","CellShutdown","CellLowPower","InterRatCellShutdown","CellDss","CellServiceDiffCfg","CellMlbAutoGroup","CellUlMimoParaCfg","CellPdcchAlgo","UlInterfSuppressCfg","CellDlschAlgo","CellDlpcPdsch","CellDlpcPdcch","CellDlpcPhich","CellUlschAlgo","CellCspcPara","CellAlgoSwitch","CellRachAlgo","CellRacThd","CellMBMSCfg","CellMLB","CellUlIcic","CellDlIcic","CellPcAlgo","CellPucchAlgo","CellMro","CellDacqCfg","CellMMAlgo","eNBCellRsvdPara","CellEicic","CellDynAcBarAlgoPara","ULZeroBufferZone","CellCqiAdjAlgo","CellUSParaCfg","CellULIcicMcPara","CellSrlte","CellVMS","CellBf","GeranInterfCfg","BcchCfg","PUSCHCfg","PUCCHCfg","PCCHCfg","PDSCHCfg","PHICHCfg","SRSCfg","CellChPwrCfg","RACHCfg","Cdma2000HrpdRefCell","Cdma2000HrpdPreReg","Cdma20001XrttPreReg","Cdma2000HrpdNCell","Cdma20001XrttNCell","EutranBlkNCell","UtranBlkNCell","EutranNFreqSCellOp","UtranNFreqSCellOp","GeranNFGroupSCellOp","Cdma2000BcSCellOp","EutranNFreqRanShare","IntraFreqBlkCell","InterFreqBlkCell","EutranInterNFreq","EutranInterFreqNCell","EutranIntraFreqNCell","UtranNFreq","UtranNCell","GeranNfreqGroup","GeranNcell","Cdma2000BandClass","Cdma2000Nfreq","GeranNfreqGroupArfcn","UtranRanShare","GeranRanShare","DMIMOCluster","PIMFreqGroup","SsrdCellGroup","CaGroupCell","LIOptRule","LIOptFunction","CellDrxSpecialPara","CellUlpcDedic","CellDlpcPdschPa","eNBCellQciRsvdPara","CaMgtCfg","CellHoParaCfg","GeranInterfArfcn","CellDlCompAlgo","CellUlCompAlgo","QCIUTRANRELATION","QCIGERANRELATION","QCIEUTRANRELATION","DrxParaGroup","CellDrxPara","CellQciPara","CellMimoParaCfg","CellPreallocGroup","CellBfMimoParaCfg","CellMlbHo","CaGroupSCellCfg","CellUeMeasControlCfg","CellUciOnPuschPara","UeSpecDrxParaGroup","CellCqiAdaptiveCfg","CellSrsAdaptiveCfg","IntraFreqHoGroup","InterFreqHoGroup","InterRatHoCommGroup","InterRatHoGeranGroup","InterRatHoUtranGroup","InterRatHoCdmaHrpdGroup","CellMcPara","LwaMgtCfg","UeDiffConfig","NsaDcMgmtConfig","CellUlIcAlgo","CePucchCfg","CellDmrsCfg","InterRatHoNrParamGrp","CellOpHoCfg","DistBasedHO","RlfTimerConstGroup","CellDataComprConfig","NrNFreqSCellOp","NsaDcQciParamGroup","MBMSPara"]
filenameEK = "3g_rnc_chk"
########################################### s

sqldbname = sqldbdate + ".sqlite"

final_file = open("exports\\03_sets_distribution_single" + sqldbdate + "_" + filenameEK + ".txt","w")
final_file_freq = open("exports\\03_sets_distribution_single" + sqldbdate + "_" + filenameEK + "_freq.txt","w")

NE = "LBRM049"

for mo in molist:
    print(mo)

    curMO = distributionCalcSingleSite(mo,sqldbname,NE)

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
