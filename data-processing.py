import sqlite3
import os
import re 
import time
import stocks_api

def create_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/' + db_name)
    conn.execute("PRAGMA foreign_keys = 1")
    conn.commit()
    cur = conn.cursor()
    return cur, conn

def stocks_tables(cur, conn):
    for i in range(19):
        stocks_api.main(cur, conn)
        time.sleep(35)

def main():
    cur, conn = create_database('final.db')
    stocks_tables(cur, conn)

if __name__ == "__main__":
    main()
    