import sqlite3
import pandas as pd
import numpy as np

groupidList = ["RLCPDCPPARAGROUPID","INTERRATHOUTRANGROUPID","INTERRATHOCOMMGROUPID","INTERRATPOLICYCFGGROUPID","ASPARAGROUPID","RRCCAUSE","INTERFREQHOGROUPID","QCI","PCCDLEARFCN","SCCDLEARFCN","DRXPARAGROUPID","INTERRATHOGERANGROUPID","INTERRATHOUTRANGROUPID","DLEARFCN","SRBINDEX","TRCHID","RABINDEX"]
excList = groupidList + ["ref","ENODEBFUNCTIONNAME","ENODEBID","MCC","MNC","RNCID","GERANCELLID","SCELLENODEBID","NEIGHBOURCELLNAME","LOCALCELLNAME","FREQ","NODEBFUNCTIONNAME","ULOCELLID","UARFCNDOWNLINK","PHYCELLID","LOCALCELLID","NE","CELLID","LOGICRNCID","ENODEBFUNCTIONNAME","CELLNAME","CELLACTIVESTATE","CELLADMINSTATE"] #parametre içermeyen kolonları çıkarmak için


def exportFilteredDictFreq(mo,sqlfile):
    folder = 'imports' + '/' + sqlfile
    conn = sqlite3.connect(folder)
    cur = conn.cursor()
    df = pd.read_sql("select * from {}".format(mo), conn)
    cur.close()

    cols = df.columns.values
    if "LOCALCELLID" in cols:
        df["New"] = df["NE"] + ":" + df["LOCALCELLID"]
        cellDF = pd.read_sql("select * from {}".format("CELL"), conn)
        cellDF["New"] = cellDF["NE"] + "-" + cellDF["LOCALCELLID"]
        cellDF = cellDF[["New","DLEARFCN"]]
        cellDF.rename(columns={"DLEARFCN":"FREQ"}, inplace = True)
        df.set_index("New",inplace=True)
        innerJoin = pd.merge(df, cellDF, on="New", how="inner")

        dfdict = innerJoin.to_dict()
    elif "CELLID" in cols:
        df["New"] = df["NE"] + ":" + df["CELLID"]
        df.set_index("New",inplace=True)
    else:
        df["New"] = df["NE"]
        df.set_index("New", inplace=True)
        df["FREQ"] = "ALL"
        dfdict = df.to_dict()



    return dfdict


def distributionCalc(mo,sqlfile):

    folder = 'imports' + '/' + sqlfile
    conn = sqlite3.connect(folder)
    cur = conn.cursor()
    df = pd.read_sql("select * from {}".format(mo), conn)
    cols = df.columns.values
    if "LOCALCELLID" in cols:
        df["ref"] = df["NE"] + "-" + df["LOCALCELLID"]
        cellDF = pd.read_sql("select * from {}".format("CELL"), conn)
        cellDF["ref"] = cellDF["NE"] + "-" + cellDF["LOCALCELLID"]
        cellDF = cellDF[["ref","DLEARFCN"]]
        cellDF.rename(columns={"DLEARFCN":"FREQ"}, inplace = True)

    elif "CELLID" in cols:
        print("burdayım")
        df["ref"] = df["NE"] + "-" + df["CELLID"]
        cellDF = pd.read_sql("select * from {}".format("UCELL"), conn)
        cellDF["ref"] = cellDF["NE"] + "-" + cellDF["CELLID"]
        cellDF = cellDF[["ref","UARFCNDOWNLINK"]]
        cellDF.rename(columns={"UARFCNDOWNLINK":"FREQ"}, inplace = True)
    else:
        #return "-1"
        ########
        folder = 'imports' + '/' + sqlfile
        conn = sqlite3.connect(folder)
        cur = conn.cursor()
        df = pd.read_sql("select * from {}".format(mo), conn)
        df["FREQ"] = "ALL"
        df["ref"] = df["NE"]
        df.set_index("ref",inplace=True)
        ########
        cur.close()
        return df

    innerJoin = pd.merge(df,cellDF,on = "ref",how = "inner")
    innerJoin.set_index("ref", inplace=True)
    cur.close()

    return innerJoin


def distributionCalcCity(mo, sqlfile,citycode):

    folder = 'imports' + '/' + sqlfile
    conn = sqlite3.connect(folder)
    cur = conn.cursor()
    df = pd.read_sql('select * from {} where substr(NE,2,2) = "{}"'.format(mo,citycode), conn)
    cols = df.columns.values
    if "LOCALCELLID" in cols:
        df["ref"] = df["NE"] + "-" + df["LOCALCELLID"]
        cellDF = pd.read_sql("select * from {}".format("CELL"), conn)
        cellDF["ref"] = cellDF["NE"] + "-" + cellDF["LOCALCELLID"]
        cellDF = cellDF[["ref", "DLEARFCN"]]
        cellDF.rename(columns={"DLEARFCN": "FREQ"}, inplace=True)

    elif "CELLID" in cols:
        df["ref"] = df["NE"] + "-" + df["CELLID"]
        cellDF = pd.read_sql("select * from {}".format("UCELL"), conn)
        cellDF["ref"] = cellDF["NE"] + "-" + cellDF["CELLID"]
        cellDF = cellDF[["ref", "UARFCNDOWNLINK"]]
        cellDF.rename(columns={"UARFCNDOWNLINK": "FREQ"}, inplace=True)
    else:
        # return "-1"
        ########
        folder = 'imports' + '/' + sqlfile
        conn = sqlite3.connect(folder)
        cur = conn.cursor()
        df = pd.read_sql("select * from {}".format(mo), conn)
        df["FREQ"] = "ALL"
        df["ref"] = df["NE"]
        df.set_index("ref", inplace=True)
        ########
        cur.close()
        return df

    innerJoin = pd.merge(df,cellDF,on = "ref",how = "inner")
    innerJoin.set_index("ref", inplace=True)
    cur.close()

    return innerJoin

def parseDBSite(mo,sqlfile):

    folder = 'imports' + '/' + sqlfile
    conn = sqlite3.connect(folder)
    cur = conn.cursor()
    df = pd.read_sql("select * from {}".format(mo), conn)

    cur.close()

    return df


def tableList(sqlfile):
    folder = 'imports' + '/' + sqlfile
    conn = sqlite3.connect(folder)
    cursor = conn.cursor()
    cursor.execute("SELECT NAME FROM sqlite_master WHERE type='table';")
    db_list = cursor.fetchall()
    table_list = pd.Series(np.array(db_list)[:,0], dtype="string").apply(lambda x: str(x).upper())
    cursor.close()

    return table_list


def getCombinations(seq):
    #2 li kombinasyonları bulur, sahadaki butun cellid'leri komşuluk ilişkisi vermesi için kullanılıyor
    #(11,12,31,32) --> (11,12),(11,13),(11,31),(11,31),(11,32),(12,31),(12,32)
    combinations = list()
    for i in range(0,len(seq)):
        for j in range(i+1,len(seq)):
            combinations.append([seq[i],seq[j]])
    return combinations


def exportMO(mo,sqlfile):

    folder = 'imports' + '/' + sqlfile
    conn = sqlite3.connect(folder)
    cur = conn.cursor()
    df = pd.read_sql("select * from {}".format(mo), conn)
    cur.close()

    return df

def enodebList(sqlfile):
    folder = 'imports' + '/' + sqlfile
    conn = sqlite3.connect(folder)
    cur = conn.cursor()
    df = pd.read_sql("select * from ENODEBFUNCTION", conn)
    cur.close()

    return df


def exportFilteredDict(mo,sqlfile):
    folder = 'imports' + '/' + sqlfile
    conn = sqlite3.connect(folder)
    cur = conn.cursor()
    df = pd.read_sql("select * from {}".format(mo), conn)
    cur.close()

    cols = df.columns.values
    if "LOCALCELLID" in cols:
        df["New"] = df["NE"] + ":" + df["LOCALCELLID"]
        df.set_index("New",inplace=True)
    elif "CELLID" in cols:
        df["New"] = df["NE"] + ":" + df["CELLID"]
        df.set_index("New",inplace=True)
    else:
        df["New"] = df["NE"]
        df.set_index("New", inplace=True)

    dfdict = df.to_dict()

    return dfdict


def distributionCalcSingleSite(mo,sqlfile,ne):

    folder = 'imports' + '/' + sqlfile
    conn = sqlite3.connect(folder)
    cur = conn.cursor()
    df = pd.read_sql("select * from {} where 'NE' = '{}' ".format(mo,ne), conn)
    cols = df.columns.values
    if "LOCALCELLID" in cols:
        df["ref"] = df["NE"] + "-" + df["LOCALCELLID"]
        cellDF = pd.read_sql("select * from {}".format("CELL"), conn)
        cellDF["ref"] = cellDF["NE"] + "-" + cellDF["LOCALCELLID"]
        cellDF = cellDF[["ref","DLEARFCN"]]
        cellDF.rename(columns={"DLEARFCN":"FREQ"}, inplace = True)
    elif "CELLID" in cols:
        df["ref"] = df["NE"] + "-" + df["CELLID"]
        cellDF = pd.read_sql("select * from {}".format("UCELL"), conn)
        cellDF["ref"] = cellDF["NE"] + "-" + cellDF["CELLID"]
        cellDF = cellDF[["ref","UARFCNDOWNLINK"]]
        cellDF.rename(columns={"UARFCNDOWNLINK":"FREQ"}, inplace = True)
    else:
        #return "-1"
        ########
        folder = 'imports' + '/' + sqlfile
        conn = sqlite3.connect(folder)
        cur = conn.cursor()
        df = pd.read_sql("select * from {}".format(mo), conn)
        ########
        cur.close()
        return df

    innerJoin = pd.merge(df,cellDF,on = "ref",how = "inner")

    cur.close()

    return innerJoin

