import sqlite3
import os
import re 
import time

import stocks_api
# import news_api
# import covid_api

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
    return ("Stocks tables created")

# def covid_table(cur, conn):
#     for i in range(15):
#         covid_api.main(cur, conn)
#         time.sleep(35)
#     return("Covid table created")

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

    stocks_tables(cur, conn)
    # covid_table(cur, conn)
    # news_table(cur, conn)

if __name__ == "__main__":
    main()
    