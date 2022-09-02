import sqlite3
import pandas as pd
import numpy as np

groupidList = ["RRCCAUSE","INTERFREQHOGROUPID","QCI","PCCDLEARFCN","SCCDLEARFCN","DRXPARAGROUPID","INTERRATHOGERANGROUPID","INTERRATHOUTRANGROUPID"]
excList = groupidList + ["ref","FREQ","UARFCNDOWNLINK","PHYCELLID","LOCALCELLID","NE","CELLID","LOGICRNCID","ENODEBFUNCTIONNAME","CELLNAME","DLEARFCN","CELLACTIVESTATE","CELLADMINSTATE"] #parametre içermeyen kolonları çıkarmak için


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

    return df

