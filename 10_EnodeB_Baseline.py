from huaweiDumpLibrary import parseDBSite,excList,distributionCalc,groupidList,enodebList,exportFilteredDict,exportFilteredDictFreq
from dbname import sqldbdate

final_file = open("exports\\10_sets_distribution_" + sqldbdate + ".txt","w")

sqldbname = sqldbdate + ".sqlite"

checklist = [
["CAMGTCFG","CARRAGGRA2THDRSRP","-112","ALL"],
["CAMGTCFG","CARRAGGRA4THDRSRP","-108","ALL"],
["CAMGTCFG","ActiveBufferLenThd","5","ALL"],
["CAMGTCFG","DeactiveBufferLenThd","3","ALL"],
["CAMGTCFG","ActiveBufferDelayThd","5","ALL"],
["CAMGTCFG","DeactiveThroughputThd","100","ALL"],
["CAMGTCFG","SccCfgInterval","15","ALL"],
["CAMGTCFG","CARRAGGRA6OFFSET","4","ALL"],
["CAMGTCFG","CELLCAALGOSWITCH","CaDl3CCSwitch-1","ALL"],
["CAMGTCFG","CELLCAALGOSWITCH","CaUl2CCSwitch-1","ALL"],
["CAMGTCFG","CELLCAALGOSWITCH","CaDl4CCSwitch-1","ALL"],
["CAMGTCFG","CELLCAALGOSWITCH","CaDl2CCExtSwitch-1","ALL"],
["CAMGTCFG","CELLCAALGOSWITCH","CaDl3CCExtSwitch-1","ALL"],
["CELLALGOSWITCH","Dl256QamAlgoSwitch","Dl256QamSwitch-1","ALL"],
["CELLALGOSWITCH","ENHANCEDMLBALGOSWITCH","SpectralEffBasedLoadEvalSw-1","ALL"],
["CELLALGOSWITCH","ENHANCEDMLBALGOSWITCH","ActiveUeBasedLoadEvalSw-1","ALL"],
["CELLALGOSWITCH","FreqLayerSwitch","UtranSrvccSteeringSwitch-OFF","ALL"],
["CELLCQIADJALGO ","VOLTENACKDELTACQI","30","ALL"],
["CELLALGOSWITCH","HoAllowedSwitch","CsfbAdaptiveBlindHoSwitch-0","ALL"],
["CELLALGOSWITCH","HoAllowedSwitch","UtranCsfbSwitch-1","ALL"],
["CELLALGOSWITCH","HoAllowedSwitch","UtranFlashCsfbSwitch-1","ALL"],
["CELLALGOSWITCH","ULSCHEXTSWITCH","UlPAMCSwitch-1","ALL"],
["CELLALGOSWITCH","ULSCHEXTSWITCH","EnhancedSchForSparseSwitch-1","ALL"],
["CELLDLSCHALGO","DLHARQMAXTXNUM","6","ALL"],
["CELLHOPARACFG","HoModeSwitch","UtranPsHoSwitch-0","ALL"],
["CELLHOPARACFG","HoModeSwitch","UtranRedirectSwitch-1","ALL"],
["CELLHOPARACFG","HoModeSwitch","UtranSrvccSwitch-1","ALL"],
["CELLHOPARACFG","BLINDHOA1A2THDRSRP","-121","ALL"],
["CellHoParaCfg ","FlashSrvccSwitch","ON","ALL"],
["CellHoParaCfg ","UlPoorCoverPathLossThd","140","ALL"],
["CellHoParaCfg ","UlPoorCoverSinrThd","-100","ALL"],
["CellHoParaCfg ","VolteQualPktLossPeriod","2","ALL"],
["CellHoParaCfg ","VolteQualIfHoQCI1PlrThld","10","ALL"],
["CellHoParaCfg ","VolteQualRecoveryQci1PlrThld","5","ALL"],
["CellHoParaCfg ","VoLTEQualityHoAlgoSwitch","InterFreqVolteQualityHoSwitch-1","ALL"],
["CELLMIMOPARACFG","MimoAdaptiveSwitch","CL_ADAPTIVE","ALL"],
["CELLSEL","QRXLEVMIN","-62","ALL"],
["CellTtiBundlingAlgo ","TtiBundlingAlgoSw","TTIBUNDLING_ALGO_ENHANCE_SW-1","ALL"],
["CellUciOnPuschPara ","DeltaOffsetAckIndexForTtiB","11","ALL"],
["CELLULSCHALGO ","AdaptHarqSwitch","ADAPTIVE_HARQ_SW_ON","ALL"],
["CELLULSCHALGO","ULHARQMAXTXNUM","7","ALL"],
["PUSCHCFG","R12QAM64ENABLED","BOOLEAN_TRUE","ALL"],
["PUSCHCFG","QAM64ENABLED","BOOLEAN_TRUE","ALL"],
["CELL","DLBANDWIDTH","CELL_BW_N50","6200"],
["CELL","DLBANDWIDTH","CELL_BW_N100","1450"],
["CELL","DLBANDWIDTH","CELL_BW_N50","3200"],
["CELL","FREQBAND","20","6200"],
["CELL","FREQBAND","3","1450"],
["CELL","FREQBAND","7","3200"],
["CELL","ULBANDWIDTH","CELL_BW_N50","6200"],
["CELL","ULBANDWIDTH","CELL_BW_N100","1450"],
["CELL","ULBANDWIDTH","CELL_BW_N50","3200"],
["CELLMLB","IdleMlbUeNumDiffThd","5","6200"],
["CELLMLB","IdleMlbUeNumDiffThd","30","1450"],
["CELLMLB","IdleMlbUeNumDiffThd","15","3200"],
["CELLMLB","INTERFREQMLBTHD","40","6200"],
["CELLMLB","INTERFREQMLBTHD","70","1450"],
["CELLMLB","INTERFREQMLBTHD","60","3200"],
["CELLMLB","INTERFREQMLBUENUMTHD","40","6200"],
["CELLMLB","INTERFREQMLBUENUMTHD","40","3200"],
["CELLMLB","INTERFREQMLBUENUMTHD","80","1450"],
["CELLMLB","LOADDIFFTHD","5","6200"],
["CELLMLB","LOADDIFFTHD","30","1450"],
["CELLMLB","LOADDIFFTHD","15","3200"],
["CELLMLB","LOADOFFSET","10","6200"],
["CELLMLB","LOADOFFSET","10","1450"],
["CELLMLB","LOADOFFSET","10","3200"],
["CELLMLB","MLBMINUENUMOFFSET","5","6200"],
["CELLMLB","MLBMINUENUMOFFSET","10","1450"],
["CELLMLB","MLBMINUENUMOFFSET","5","3200"],
["CELLMLB","MLBMINUENUMTHD","10","6200"],
["CELLMLB","MLBMINUENUMTHD","30","1450"],
["CELLMLB","MLBMINUENUMTHD","10","3200"],
["CELLMLB","MLBUENUMOFFSET","5","6200"],
["CELLMLB","MLBUENUMOFFSET","5","1450"],
["CELLMLB","MLBUENUMOFFSET","5","3200"],
["CELLMLB","UENUMDIFFTHD","15","6200"],
["CELLMLB","UENUMDIFFTHD","15","1450"],
["CELLMLB","UENUMDIFFTHD","15","3200"],
["CELLRESEL","CELLRESELPRIORITY","3","6200"],
["CELLRESEL","CELLRESELPRIORITY","6","1450"],
["CELLRESEL","CELLRESELPRIORITY","4","1306"],
["CELLRESEL","CELLRESELPRIORITY","5","3200"],
["CELLRESEL","SNONINTRASEARCH","3","6200"],
["CELLRESEL","SNONINTRASEARCH","5","1450"],
["CELLRESEL","SNONINTRASEARCH","8","3200"],
["CELLRESEL","SNONINTRASEARCH","5","1306"],
["CELLRESEL","THRSHSERVLOW","3","6200"],
["CELLRESEL","THRSHSERVLOW","5","1450"],
["CELLRESEL","THRSHSERVLOW","8","3200"],
["CELLRESEL","THRSHSERVLOW","5","1306"],
["CSFALLBACKHO","BLINDHOA1THDRSRP","-107","6200"],
["CSFALLBACKHO","BLINDHOA1THDRSRP","-110","1450"],
["CSFALLBACKHO","BLINDHOA1THDRSRP","-110","3200"],
]


for chk in checklist:

    mo = chk[0].upper()
    parameter = chk[1].upper()
    value = chk[2].upper()
    freq = chk[3]

    moExp = distributionCalc(mo, sqldbname).to_dict()

    #print(moExp["FREQ"])
    print("{}:{}".format(mo,parameter))

    try:
        if value[-1] == "1": # 1. koşulda switch alt parametresi olanlar süzülüyor,komut hazırlamada lazım
            valueRev = value[:-1] + "0"
            for ne in moExp["NE"]:
                if value not in moExp[parameter][ne] and valueRev in moExp[parameter][ne]:


                    if "-" not in ne: # enodeb parametresi
                        print("MOD {}:{}={};{}{}{}".format(mo,parameter,value,"{",ne,"}"))
                        final_file.write("MOD {}:{}={};{}{}{}\n".format(mo,parameter,value,"{",ne,"}"))
                    elif freq == "ALL":  # cell parametresi
                        cellid = ne.split("-")[1]
                        enodeb = ne.split("-")[0]
                        print("MOD {}:LOCALCELLID={},{}={};{}{}{}".format(mo, cellid, parameter, value, "{", enodeb,"}"))
                        final_file.write("MOD {}:LOCALCELLID={},{}={};{}{}{}\n".format(mo, cellid, parameter, value, "{", enodeb,"}"))
                    elif freq == moExp["FREQ"][ne]: # cell parametresi
                        cellid = ne.split("-")[1]
                        enodeb = ne.split("-")[0]
                        print("MOD {}:LOCALCELLID={},{}={};{}{}{}".format(mo, cellid, parameter, value, "{", enodeb, "}"))
                        final_file.write("MOD {}:LOCALCELLID={},{}={};{}{}{}\n".format(mo, cellid, parameter, value, "{", enodeb, "}"))

        else:  # 2. koşulda parametre alt switch bulunmayanlar
            for ne in moExp["NE"]:
                if value not in moExp[parameter][ne]:

                    if "-" not in ne:
                        print("MOD {}:{}={};{}{}{}".format(mo,parameter,value,"{",ne,"}"))
                    elif freq == "ALL":  # cell parametresi
                        cellid = ne.split("-")[1]
                        enodeb = ne.split("-")[0]
                        print("MOD {}:LOCALCELLID={},{}={};{}{}{}".format(mo, cellid, parameter, value, "{", enodeb,"}"))
                        final_file.write("MOD {}:LOCALCELLID={},{}={};{}{}{}\n".format(mo, cellid, parameter, value, "{", enodeb,"}"))
                    elif freq == moExp["FREQ"][ne]:  # cell parametresi
                        cellid = ne.split("-")[1]
                        enodeb = ne.split("-")[0]
                        print("MOD {}:LOCALCELLID={},{}={};{}{}{}".format(mo, cellid, parameter, value, "{", enodeb,"}"))
                        final_file.write("MOD {}:LOCALCELLID={},{}={};{}{}{}\n".format(mo, cellid, parameter, value, "{", enodeb,"}"))

    except TypeError:
        print("error {}".format(ne))

final_file.close()








