import sqlite3
import pandas as pd
from io import StringIO

def sql_to_csv(database, table_name):
    conn = sqlite3.connect(database)
    sqlStm = "SELECT * FROM " + table_name
    sql_table = pd.read_sql_query(sqlStm, conn)
    df = pd.DataFrame(sql_table)
    csv = df.to_csv("list_fault_lines.csv", encoding='utf-8', index=False)
    with open("list_fault_lines.csv", 'r+') as f:
        f.seek(0,2)                    
        size=f.tell()               
        f.truncate(size-1)
    
    result = ''
    with open("list_fault_lines.csv",'r') as csvFile:
        for row in csvFile:
            result += row
        
    return result


def csv_to_sql(csv_content, database, table_name):
    print ("reading csv")
    csv = pd.read_csv(StringIO(csv_content))
    df = pd.DataFrame(csv)
    print('converting merged csv to SQL table')
    conn = None
    conn = sqlite3.connect(database)
    cur = conn.cursor()
    #csv head is automating column creation: cur.execute("CREATE TABLE IF NOT EXISTS %s (%s)", (table_name,csv_head))
    csv_head = ','.join(list(csv.columns))
    #print("CREATE TABLE IF NOT EXISTS {} ({})".format(table_name,csv_head))
    cur.execute("CREATE TABLE IF NOT EXISTS {} ({})".format(table_name,csv_head))
    conn.commit()
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    
    #testing the code:
    #print('Table content')
    #cur.execute("SELECT * FROM " + table_name)
    #for row in cur.fetchall():
    #    #print (row)
    #    pass
    # end of testing the code




#sql_to_csv('all_fault_line.db','fault_lines')
#csv_content = open("list_volcano.csv")
#csv_to_sql(csv_content, "list_volcano.db",'volcanos')