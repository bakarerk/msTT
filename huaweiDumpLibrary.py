import sqlite3
import pandas as pd
import numpy as np

excList = ["ref","FREQ","UARFCNDOWNLINK","PHYCELLID","LOCALCELLID","NE","CELLID","LOGICRNCID","ENODEBFUNCTIONNAME","CELLNAME","DLEARFCN","CELLACTIVESTATE","CELLADMINSTATE"] #parametre içermeyen kolonları çıkarmak için

def cellParser(mo,sqlfile):

    folder = 'imports' + '/' + sqlfile
    conn = sqlite3.connect(folder)
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
