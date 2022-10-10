from huaweiDumpLibrary import parseDBSite,excList,distributionCalc,exportMO
from dbname import sqldbdate

###########################################
molist = ["NBMPARA"]
paralist = ["NE","RELIABILITYSWITCH.RELIABILITY_RRC_RETRANS_SWITCH"]

filenameEK = "huawei_ps_acces"
###########################################

sqldbname = sqldbdate + ".sqlite"

for mo in molist:
    curMO = exportMO(mo,sqldbname)
    curMO[paralist].to_csv("exports\\06_cell_filtered_by_para_" + sqldbdate + "_" + filenameEK + ".csv")