import pandas as pd

from huaweiDumpLibrary import parseDBSite,excList,distributionCalc,groupidList,enodebList,exportFilteredDict,exportFilteredDictFreq
from dbname import sqldbdate
import itertools

folder = 'imports' + '/' + "Overlap_List.csv"
sqldbname = sqldbdate + ".sqlite"

overlaplist = pd.read_csv(folder,usecols=["Site_sector","LOCALCELLID"])

grouped = overlaplist[["Site_sector","LOCALCELLID"]].groupby("Site_sector")
df = grouped.aggregate(lambda x: tuple(x))
overlap2belistDict = df.to_dict()
tobe_dict = {}

for i,j in overlap2belistDict["LOCALCELLID"].items():
    tobe_dict[i] = list(itertools.combinations(j, 2))

ncellOverlap = distributionCalc("EUTRANINTERFREQNCELL",sqldbname)


ncellOverlap = ncellOverlap[["NE","LOCALCELLID","CELLID","OVERLAPIND"]]

ncellOverlap["Site_sector"] = ncellOverlap["NE"] + "-" + ncellOverlap["LOCALCELLID"]

ncellOverlap.set_index("Site_sector",inplace=True)

ncellOverlapDict = ncellOverlap.to_dict()

for site_sektor_i in ncellOverlapDict["NE"].keys():

    try:

        site = ncellOverlapDict["NE"][site_sektor_i]
        localcellid = ncellOverlapDict["LOCALCELLID"][site_sektor_i]
        targetcellid = ncellOverlapDict["CELLID"][site_sektor_i]
        overlap = ncellOverlapDict["OVERLAPIND"][site_sektor_i]
        tobe_set = tobe_dict[site + "-" + localcellid[-1]]


        print("{} {} {} {} {}".format(site,localcellid,targetcellid,overlap,tobe_set))

    except:

        print(site_sektor_i)




