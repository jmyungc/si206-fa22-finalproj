import sqlite3
import os
from rss_parser import Parser
from requests import get
from datetime import datetime
import pandas as pd
import re


# def open_database(db):
#     path = os.path.dirname(os.path.abspath(__file__))
#     conn = sqlite3.connect(path+'/'+db)
#     cur = conn.cursor()
#     return cur, conn

def create_news_table(cur, conn):
    cur.execute('CREATE TABLE IF NOT EXISTS news (id INTEGER PRIMARY KEY, date TEXT, vaccine TEXT, vaccination TEXT, pfizer TEXT, booster TEXT)')
    conn.commit()

def get_news_data(rss_url):

    xml = get(rss_url)
    parser = Parser(xml=xml.content, limit=500)
    feed = parser.parse()

    i = 0
    news_dict = {}
    news_list =[]
    for item in feed.feed:
        d = pd.to_datetime(item.publish_date).to_pydatetime()
        if d.year >= 2020 and d.year <= 2021:
            dt = d.date()
            if dt not in news_dict.keys():
                news_dict[dt] = item.title
                news_list.append([str(dt),item.title])
                i+=1
                # print(i, d.date(), item.title)
                #print(item.title)
                #print(item.publish_date)
                # dd = datetime.strptime(item.publish_date,'%a, %d %b %Y').date()
                # dt = parser.parse(item.publish_date)
                #print(dt)
    # for n in news_list:
    #     print(i, n[0], n[1])
    return(news_list)

def week_ids(news_data):
    dates_list = []
    tup_list = []

    for line in news_data:
        date = (re.findall('(202[01]-\d{2}-\d{2})', str(line)))
        dates_list.append(date)
    
    for i in range(len(dates_list)):
        week = 0
        new_tup = (, )

    #  # 



def main():
    rss_url = "https://tools.cdc.gov/api/v2/resources/media/132608.rss"
    news_data = get_news_data(rss_url)
    dates = week_ids(news_data)
    print(dates)
    

if __name__ == "__main__":
    main()