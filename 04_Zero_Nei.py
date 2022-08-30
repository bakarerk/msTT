from huaweiDumpLibrary import tableList,excList,cellParser
import sqlite3

###########################################
sqldbdate = "20220823"
###########################################

sqldbname = sqldbdate + ".sqlite"
folder = 'imports' + '/' + sqldbname
final_file = open("exports\\04_zero_nei_" + sqldbdate +".txt","w")

table_list = tableList(sqldbname)

conn = sqlite3.connect(folder)

table_list_3G = ["INTERFREQNCELL","INTRAFREQNCELL","GSMNCELL"]
table_list_4G = ["EUTRANINTRAFREQNCELL","UTRANNCELL","EUTRANINTERFREQNCELL"]



for mo in table_list_3G:
    try:
        cursor = conn.cursor()
        cursor.execute("select ucell.cellname, ucell.ne, ucell.cellid,ucell.ACTSTATUS from ucell LEFT join {} on {}.ne=ucell.ne and {}.cellid=ucell.cellid WHERE {}.ne is null".format(mo,mo,mo,mo))
        result = cursor.fetchall()
        for i in result:
            print(mo)
            print(i)
            final_file.write("3G\t{}\t{}\t{}\t{}\t{}\n".format(mo,i[0],i[1],i[2],i[3]))

    except:
        cursor.close()

for mo in table_list_4G:
    try:
        cursor = conn.cursor()
        cursor.execute("select cell.cellname,cell.ne,cell.LOCALCELLID,cell.CELLACTIVESTATE from cell LEFT join {} on {}.ne=cell.ne and {}.LOCALCELLID=cell.LOCALCELLID WHERE {}.ne is null".format(mo,mo,mo,mo))
        result = cursor.fetchall()
        for i in result:
            print(mo)
            print(i)
            final_file.write("4G\t{}\t{}\t{}\t{}\t{}\n".format(mo,i[0],i[1],i[2],i[3]))

    except:
        cursor.close()

final_file.close()
