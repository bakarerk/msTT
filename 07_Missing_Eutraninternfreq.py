from huaweiDumpLibrary import tableList,excList,distributionCalc
import sqlite3
from dbname import sqldbdate,sqldbname

sqldbname = sqldbdate + ".sqlite"
folder = 'imports' + '/' + sqldbname

cell = distributionCalc("CELL",sqldbname)

cell["_6200"] = cell["ref"] + ":" + cell["DLEARFCN"] + ":" + "6200"
cell["_1306"] = cell["ref"] + ":" + cell["DLEARFCN"] + ":" + "1306"
cell["_1450"] = cell["ref"] + ":" + cell["DLEARFCN"] + ":" + "1450"
cell["_3200"] = cell["ref"] + ":" + cell["DLEARFCN"] + ":" + "3200"

cell.set_index("ref",inplace=True)
cell = cell[["_6200","_1306","_1450","_3200"]]
cellstacked = cell.stack()
cellstacked = cellstacked.to_frame()

nfreq = distributionCalc("EUTRANINTERNFREQ",sqldbname)

nfreq["0"] = nfreq["ref"] + ":" + nfreq["FREQ"] + ":" + nfreq["DLEARFCN"]
nfreq = nfreq[["0"]]


cellstacked.to_csv("cells.csv")
nfreq.to_csv("nfreqs.csv")

