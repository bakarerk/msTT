from huaweiDumpLibrary import parseDBSite,excList,distributionCalc,groupidList,enodebList,exportFilteredDict,exportFilteredDictFreq
from dbname import sqldbdate
import itertools

sqldbname = sqldbdate + ".sqlite"

ltecell = distributionCalc("CELL",sqldbname)
grouped = ltecell[["NE","DLEARFCN"]].groupby("NE")
df = grouped.aggregate(lambda x: tuple(x))
site_cell_dict = df.to_dict()

ltepcc = distributionCalc("PCCFREQCFG",sqldbname)
grouped = ltepcc[["NE","PCCDLEARFCN"]].groupby("NE")
df2 = grouped.aggregate(lambda x: tuple(x))
site_pcccfg_dict = df2.to_dict()

ltescc = distributionCalc("SCCFREQCFG",sqldbname)
grouped = ltescc[["NE","PCCDLEARFCN","SCCDLEARFCN"]].groupby("NE")
df3 = grouped.aggregate(lambda x: tuple(x))
site_scccfg_dict = df3.to_dict()


#Pcc = distributionCalc("PCCFREQCFG",sqldbname).to_dict()
Scc = distributionCalc("SCCFREQCFG",sqldbname).to_dict()

for ne in site_cell_dict["DLEARFCN"].keys():
    if ne[3] != "M":
        pcc_tobe = set(site_cell_dict["DLEARFCN"][ne])

        if len(pcc_tobe)>1:

            try:
                pcc_config = set(site_pcccfg_dict["PCCDLEARFCN"][ne])

                for pairPcc in pcc_tobe:
                    if pairPcc not in pcc_config:
                        print("missing PCCFREQCFG |{}|{}".format(pairPcc,ne))

            except KeyError:
                print("PCC tanımlaması yok|{}|{}".format(pcc_tobe,ne))
            try:
                scc_toBeT = list(itertools.combinations(set(site_cell_dict["DLEARFCN"][ne]), 2))
                scc_configT = list(zip(site_scccfg_dict["PCCDLEARFCN"][ne],site_scccfg_dict["SCCDLEARFCN"][ne]))

                scc_toBe = [list(i) for i in scc_toBeT]
                scc_config = [list(i) for i in scc_configT]

                for pair in scc_toBe:
                    pair = list(pair)
                    revpair = list(reversed(pair))
                    if pair not in scc_config:
                        print("missing SCCFREQCFG |{}|{}".format(pair,ne))
                    if revpair not in scc_config:
                        print("missing SCCFREQCFG |{}|{}".format(revpair,ne))

            except KeyError:
                print("SCC tanımlaması yok|{},{}|{}".format(pair,revpair,ne))
            #print(site_scccfg_dict["PCCDLEARFCN"][ne])
            #print(site_scccfg_dict["SCCDLEARFCN"][ne])


