from huaweiDumpLibrary import tableList,excList,distributionCalc
import sqlite3
from dbname import sqldbdate,sqldbname

ltecell = distributionCalc("CELL",sqldbname)

grouped = ltecell[["NE","LOCALCELLID","DLEARFCN"]].groupby("NE")

df = grouped.aggregate(lambda x: tuple(x))

site_cell_dict = df.to_dict()


for i in site_cell_dict["LOCALCELLID"].keys():
    freqlist = site_cell_dict["DLEARFCN"][i]
    print("{}-{}-{}".format(i,site_cell_dict["LOCALCELLID"][i],site_cell_dict["DLEARFCN"][i]))
