from huaweiDumpLibrary import parseDBSite,excList,distributionCalc,exportMO
from dbname import sqldbdate

###########################################
molist = ["RRCTRLSWITCH"]
paralist = ["NE","OPTIMIZATIONSWITCH2.RRC_CON_REQ_TIMES_COMPATIBLE_SWITCH"]

filenameEK = "RRCTRLSWITCH"
###########################################

sqldbname = sqldbdate + ".sqlite"

for mo in molist:
    curMO = exportMO(mo,sqldbname)
    curMO[paralist].to_csv("exports\\06_cell_filtered_by_para_" + sqldbdate + "_" + filenameEK + ".csv")