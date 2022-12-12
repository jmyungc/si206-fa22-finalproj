import sqlite3
import os
from bs4 import BeautifulSoup
from rss_parser import Parser
from requests import get
from datetime import datetime
import pandas as pd
import re


def open_database(db):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db)
    cur = conn.cursor()
    return cur, conn

def create_news_table(cur, conn):
    cur.execute('CREATE TABLE IF NOT EXISTS news (id INTEGER PRIMARY KEY, date TEXT, week_id INTEGER, daily_kw_count INTEGER, weekly_kw_count INTEGER)')
    conn.commit()

def create_date_list():
    date_list = []

    year = 2020
    month = 1
    day = 3

    month_31 = [1, 3, 5, 7, 8, 10, 12]
    month_30 = [4, 6, 9, 11]

    # duration between 2020/01/03 - 2021/12/31
    duration = 729

    i = 0
    week = 0
    week_count = 6
    while i < duration:
        str_month = str(month)
        str_day = str(day)
        if len(str_month) == 1:
            str_month = '0' + str_month
        if len(str_day) == 1:
            str_day = '0' + str_day

        date = str(year) + '-' + str_month + '-' + str_day

        date_list.append((date, week))

        day += 1
        i += 1
        week_count += 1

        if ((day == 31) and (month in month_30)) or ((day == 32) and (month in month_31)):
            month += 1
            day = 1
        if ((month == 2) and (day == 30) and (year == 2020)):
            month += 1
            day = 1
        if ((month == 2) and (day == 29) and (year == 2021)):
            month += 1
            day = 1
        
        if month == 13:
            month = 1
            year += 1

        if week_count == 7:
            week_count = 0
            week += 1

    return date_list

def initialize_table(cur, conn, list):
    cur.execute('SELECT COUNT(*) FROM news')
    index = cur.fetchone()[0]
    count = 0
    if index >= 100:
        print(f'There are {index} lines in the news table. Inserting the rest')
        while index < len(list):
            cur.execute('INSERT INTO news (date, week_id, daily_kw_count, weekly_kw_count) VALUES (?, ?, ?, ?)', (list[index][0], list[index][1], 0, 0)) 
            index += 1
    else:
        print(f'There are {index} lines in the news table. Inserting 25 more')
        while index < len(list) and count < 25:
            cur.execute('INSERT INTO news (date, week_id, daily_kw_count, weekly_kw_count) VALUES (?, ?, ?, ?)', (list[index][0], list[index][1], 0, 0)) 

            index += 1
            count += 1

    conn.commit()
    # jan 3rd 2020
    # dec 31st 2021

def get_news_data(rss_url):
    xml = get(rss_url)
    parser = Parser(xml=xml.content, limit=1000)
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
    return news_list

# def week_ids(news_data):
#     dates_list = []
#     tup_list = []

#     for line in news_data:
#         date = (re.findall('(202[01]-\d{2}-\d{2})', str(line)))
#         dates_list.append(date)
    
#     for i in range(len(dates_list)):
#         week = 0
#         new_tup = (, )

    #  # 

def insert_keyword_count(cur, conn, list):
    keywords = ['vaccine', 'vaccination' 'johnson & johnson', 'janssen', 'pfizer', 'corona','covid-19', 'covid',
                'covid-19', 'booster', 'immune', 'immunization', 'cases', 'case', 'increase', 'bio-tech',
                'biotech', 'mask', 'lockdown', 'pandemic', 'spread', 'experts']
    for i in list:
        date = i[0]
        headline = i[1].lower()
        kw_count = 0
        for kw in keywords:
            kw_count += headline.count(kw)

        cur.execute("UPDATE news SET daily_kw_count=(?) WHERE date=(?)", (kw_count, date))

    conn.commit()

def calculate_weekly_count(cur, conn):
    cur.execute('SELECT max(week_id) FROM news')
    max_week = cur.fetchone()[0]
    cur.execute('SELECT min(week_id) FROM news')
    count = cur.fetchone()[0]

    while count <= max_week:
        statement = f'SELECT daily_kw_count FROM news WHERE week_id = {count}'
        cur.execute(statement)
        week_data = cur.fetchall()
        total = 0
        for day in week_data:
            total += day[0]

        insert_statement = f'UPDATE news set weekly_kw_count={total} WHERE week_id={count}'

        cur.execute(insert_statement)

        count += 1

    conn.commit()




def main():
    rss_url = "https://tools.cdc.gov/api/v2/resources/media/132608.rss"

    cur, conn = open_database('final.db')

    create_news_table(cur, conn)

    date_list = create_date_list()

    initialize_table(cur, conn, date_list)

    news_data = get_news_data(rss_url)

    insert_keyword_count(cur, conn, news_data)

    calculate_weekly_count(cur, conn)


if __name__ == "__main__":
    main()