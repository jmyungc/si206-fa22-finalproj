import requests
# from bs4 import BeautifulSoup
from rss_parser import Parser
from requests import get
from datetime import datetime
# from dateutil import parser

import pandas as pd


rss_url = "https://tools.cdc.gov/api/v2/resources/media/132608.rss"
xml = get(rss_url)

# Limit feed output to 5 items
# To disable limit simply do not provide the argument or use None
parser = Parser(xml=xml.content, limit=500)
feed = parser.parse()

# Print out feed meta data
# print(feed.language)
# print(feed.version)

# Iteratively print feed items
i = 0
newsdic = {}
newslist =[]
for item in feed.feed:
    d = pd.to_datetime(item.publish_date).to_pydatetime()
    # if d < datetime(2019, 12, 31) and d >= datetime(2018, 1, 1):
    if d.year <= 2022 and d.year >= 2020:
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
    print(i, n[0]+"---"+n[1])