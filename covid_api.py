import json
import unittest
import os
import requests
import sqlite3

def database_setup(db):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + '/' + db)
    cur = conn.cursor()
    return cur, conn

def create_table(cur, conn):
    cur.execute('CREATE TABLE IF NOT EXISTS "Covid" ("id" INTEGER PRIMARY KEY, "date" TEXT, "week_id" INTEGER, "positive_cases" INTEGER, "weekly_avg" REAL)')
    conn.commit()

def get_data(url):
    try:
        r = requests.get(url)
        covid_data = json.loads(r.text)
        return covid_data
    except:
        print("Exception")

def data_cleanup(list):
    table = []
    for day in list:
        date = str(day['date'])

        date = date[0:4] + '-' + date[4:6] + '-' + date[6:]

        data_point = (date, day['positive'])
        table.append(data_point)
    
    # dropping dates prior to Jan 17th.
    table = table[:-4]

    table.reverse()

    return table

def insert_data(cur, conn, data):
    cur.execute('SELECT COUNT(*) FROM Covid')
    index = cur.fetchone()[0]
    count = 0

    week = index // 7 + 2
    week_counter = max(index % 7 - 1, 0)

    if index >= 100:
        print(f'Already have {index} lines in table. Now inserting the rest.')
        while index < len(data):
            cur.execute('INSERT INTO Covid (date, week_id, positive_cases) VALUES (?, ?, ?)', (data[index][0], week, data[index][1]))
            index += 1
            week_counter += 1
            if week_counter == 7:
                week_counter = 0
                week += 1 
    else:
        print(f'Only have {index} lines in table. Now inserting 25 more lines.')
        while count < 25 and index < len(data):
            cur.execute('INSERT INTO Covid (date, week_id, positive_cases) VALUES (?, ?, ?)', (data[index][0], week, data[index][1]))
            count += 1
            index += 1
            week_counter += 1
            if week_counter == 7:
                week_counter = 0
                week += 1 
    
    conn.commit()

def calculate_week_avg(cur, conn):
    cur.execute('SELECT max(week_id) FROM Covid')
    max_week = cur.fetchone()[0]
    cur.execute('SELECT min(week_id) FROM Covid')
    count = cur.fetchone()[0]

    while count <= max_week:
        statement = f'SELECT positive_cases FROM COVID WHERE week_id = {count}'
        cur.execute(statement)
        week_data = cur.fetchall()
        total = 0
        for day in week_data:
            total += day[0]

        total = round(total / 7, 2)

        insert_statement = f'UPDATE Covid set weekly_avg={total} WHERE week_id={count}'

        cur.execute(insert_statement)

        count += 1

    conn.commit()



# cur.execute('CREATE TABLE IF NOT EXIST Covid (id integer PRIMARY KEY, date varchar(255), positive_cases integer)')

# count = 0
# while count < 25 and count < len(table_i_guess):
#     #add each line in table into database
#     # increment count by 1
#     count += 1


def main():
    url = 'https://api.covidtracking.com/v1/us/daily.json'

    cur, conn = database_setup('final.db')

    create_table(cur, conn)

    covid = get_data(url)
    
    data = data_cleanup(covid)

    insert_data(cur, conn, data)

    calculate_week_avg(cur, conn)



main()