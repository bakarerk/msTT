import sqlite3
import pandas as pd

excList = ["ref","FREQ","UARFCNDOWNLINK","PHYCELLID","LOCALCELLID","NE","CELLID","LOGICRNCID","ENODEBFUNCTIONNAME","CELLNAME","DLEARFCN","CELLACTIVESTATE","CELLADMINSTATE"] #parametre içermeyen kolonları çıkarmak için


def parseDBCell(mo,sqlfile):
    #LTE parse with earfcn
    conn = sqlite3.connect(sqlfile)
    cur = conn.cursor()
    df = pd.read_sql("select * from {}".format(mo), conn)
    df["ref"] = df["NE"] + "-" + df["LOCALCELLID"]
    ltecellDF = pd.read_sql("select * from {}".format("CELL"), conn)
    ltecellDF["ref"] = ltecellDF["NE"] + "-" + ltecellDF["LOCALCELLID"]
    ltecellDF = ltecellDF[["ref","DLEARFCN"]]

    innerJoin = pd.merge(df,ltecellDF,on = "ref",how = "inner")

    cur.close()

    return innerJoin

def parseDBCell3G(mo,sqlfile):
    #3G parse with earfcn
    conn = sqlite3.connect(sqlfile)
    cur = conn.cursor()
    df = pd.read_sql("select * from {}".format(mo), conn)
    df["ref"] = df["NE"] + "-" + df["CELLID"]
    umtscellDF = pd.read_sql("select * from {}".format("UCELL"), conn)
    umtscellDF["ref"] = umtscellDF["NE"] + "-" + umtscellDF["CELLID"]
    umtscellDF = umtscellDF[["ref","UARFCNDOWNLINK"]]

    innerJoin = pd.merge(df,umtscellDF,on = "ref",how = "inner")

    cur.close()

    return innerJoin

def parseDBSite(mo):

    conn = sqlite3.connect("20220817.sqlite")
    cur = conn.cursor()
    df = pd.read_sql("select * from {}".format(mo), conn)

    cur.close()

    return df


def cellParser(mo,sqlfile):
    #LTE parse with earfcn
    conn = sqlite3.connect(sqlfile)
    cur = conn.cursor()
    df = pd.read_sql("select * from {}".format(mo), conn)
    cols = df.columns.values
    if "CELLID" in cols:
        df["ref"] = df["NE"] + "-" + df["CELLID"]
        cellDF = pd.read_sql("select * from {}".format("UCELL"), conn)
        cellDF["ref"] = cellDF["NE"] + "-" + cellDF["CELLID"]
        cellDF = cellDF[["ref","UARFCNDOWNLINK"]]
        cellDF.rename(columns={"UARFCNDOWNLINK":"FREQ"}, inplace = True)
    elif "LOCALCELLID" in cols:
        df["ref"] = df["NE"] + "-" + df["LOCALCELLID"]
        cellDF = pd.read_sql("select * from {}".format("CELL"), conn)
        cellDF["ref"] = cellDF["NE"] + "-" + cellDF["LOCALCELLID"]
        cellDF = cellDF[["ref","DLEARFCN"]]
        cellDF.rename(columns={"DLEARFCN":"FREQ"}, inplace = True)
    else:
        return "-1"

    innerJoin = pd.merge(df,cellDF,on = "ref",how = "inner")

    cur.close()

    return innerJoin