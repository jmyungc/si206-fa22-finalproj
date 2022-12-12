import requests
import sqlite3
import os
from rss_parser import Parser
from requests import get
from datetime import datetime
import pandas as pd


def open_database(db):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db)
    cur = conn.cursor()
    return cur, conn

def create_news_table(cur, conn):
    cur.execute('CREATE TABLE IF NOT EXISTS news (id INTEGER PRIMARY KEY, date TEXT, vaccine TEXT, vaccination TEXT, pfizer TEXT, booster TEXT)')
    conn.commit()

def get_news_data(rss_url):

    xml = get(rss_url)
    parser = Parser(xml=xml.content, limit=500)
    feed = parser.parse()

    i = 0
    newsdic = {}
    newslist =[]
    for item in feed.feed:
        d = pd.to_datetime(item.publish_date).to_pydatetime()
        if d.year >= 2020 and d.year <= 2021:
            dt = d.date()
            if dt not in newsdic.keys():
                newsdic[dt] = item.title
                newslist.append([str(dt),item.title])
                i+=1
                # print(i, d.date(), item.title)
                #print(item.title)
                #print(item.publish_date)
                # dd = datetime.strptime(item.publish_date,'%a, %d %b %Y').date()
                # dt = parser.parse(item.publish_date)
                #print(dt)
    for n in newslist:
        print(i, n[0], n[1])

def main():
    rss_url = "https://tools.cdc.gov/api/v2/resources/media/132608.rss"
    get_news_data(rss_url)

if __name__ == "__main__":
    main()