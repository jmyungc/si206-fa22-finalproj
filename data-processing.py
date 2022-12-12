import sqlite3
import os
import re 
import time

import stocks_api
# import news_api
import covid_api

def create_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/' + db_name)
    conn.execute("PRAGMA foreign_keys = 1")
    conn.commit()
    cur = conn.cursor()
    return cur, conn

def stocks_tables(cur, conn):
    for i in range(11):
        stocks_api.main(cur, conn)

        cur.execute('SELECT COUNT (*) FROM Stocks')
        row_count = cur.fetchone()

        if (row_count[0] == 315 or row_count[0] == None):
            print("Stocks table complete")
            break

        time.sleep(20)
    return

def covid_table(cur, conn):
    for i in range(5):
        covid_api.main(cur, conn)
        cur.execute('SELECT COUNT (*) FROM Covid')
        
        row_count = cur.fetchone()

        if (row_count[0] == 416 or row_count[0] == None):
            print("Covid table complete")
            break
        time.sleep(15)
    return

# def news_table(cur,conn):
#     for i in range(15):
#         news_api.main(cur, conn)
#         time.sleep(35)
#     return("News table created")

# def join_stocks_tables(cur,conn):
# Join stocks tables 


# Do math using tables in final.db
# Export math to text file
# Visualizations #

def main():
    cur, conn = create_database('final.db')

    covid_table(cur, conn)
    
    stocks_tables(cur, conn)
   

    # news_table(cur, conn)

if __name__ == "__main__":
    main()
    