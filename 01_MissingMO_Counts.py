from huaweiDumpLibrary import tableList,excList,cellParser
import sqlite3
from dbname import sqldbdate

sqldbname = sqldbdate + ".sqlite"
folder = 'imports' + '/' + sqldbname
final_file = open("exports\\01_missingmo_counts_" + sqldbdate +".txt","w")

table_list = tableList(sqldbname)

conn = sqlite3.connect(folder)


#2G
for mo in table_list:
    try:
        cursor = conn.cursor()
        cursor.execute("Select count(*) from (select gcell.cellname,gcell.ne,gcell.CI from gcell LEFT join {} on {}.ne=gcell.ne and {}.cellid=gcell.CI WHERE {}.ne is null)T".format(mo,mo,mo,mo))
        result = cursor.fetchall()
        if result[0][0]>0:
            print("{}:{}".format(mo,result[0][0]))
            final_file.write("{}\t{}\t2G\n".format(mo,result[0][0]))

    except:
        cursor.close()

#3G
for mo in table_list:
    try:
        cursor = conn.cursor()
        cursor.execute("Select count(*) from (select ucell.cellname, ucell.ne, ucell.cellid from ucell LEFT join {} on {}.ne=ucell.ne and {}.cellid=ucell.cellid WHERE {}.ne is null)T".format(mo,mo,mo,mo))
        result = cursor.fetchall()
        if result[0][0]>0:
            print("{}:{}".format(mo,result[0][0]))
            final_file.write("{}\t{}\t3G\n".format(mo,result[0][0]))

    except:
        cursor.close()

#4G
for mo in table_list:
    try:
        cursor = conn.cursor()
        cursor.execute("Select count(*) from (select cell.cellname,cell.ne,cell.LOCALCELLID from ucell LEFT join {} on {}.ne=cell.ne and {}.LOCALCELLID=cell.LOCALCELLID WHERE {}.ne is null)T".format(mo,mo,mo,mo))
        result = cursor.fetchall()
        if result[0][0]>0:
            print("{}:{}".format(mo,result[0][0]))
            final_file.write("{}\t{}\t4G\n".format(mo,result[0][0]))
    except:
        cursor.close()

#node
for mo in table_list:
    try:
        cursor = conn.cursor()
        cursor.execute("Select count(*) from (select NODE.ne from NODE LEFT join {} on {}.ne=NODE.ne WHERE {}.ne is null)T".format(mo,mo,mo))
        result = cursor.fetchall()
        if result[0][0]>0:
            print("{}:{}".format(mo,result[0][0]))
            final_file.write("{}\t{}\tNode\n".format(mo,result[0][0]))

    except:
        cursor.close()

final_file.close()



