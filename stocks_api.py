import sqlite3
import json
import os
import requests
import re 

API_KEY = "ET388N6DI4NR30RE"

def create_request_url(stock_symbol):
    symbol = stock_symbol

    request_url = "https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol=" + stock_symbol + "&apikey=" + API_KEY

    return request_url

def call_api(request_url):
    try:
        req = requests.get(request_url)

        data = {}
        data = json.loads(req.text)
    except:
        print("Exception")
        return("Exception")

    return data

def data_keys(data):
    week_list = []
    keys = data['Weekly Adjusted Time Series'].keys()

    key_string = ""

    for item in keys:
        key_string += ' ' + item

    week_list = (re.findall('(202[0-1]-\d{2}-\d{2})', key_string))

    return(sorted(week_list))

def create_high_dict(data, week_list):
    high_dict = {}
    for week in week_list:
        high_dict[week] = data['Weekly Adjusted Time Series'][week]['2. high']
    return high_dict

def create_low_dict(data, week_list):
    low_dict = {}
    for week in week_list:
        low_dict[week] = data['Weekly Adjusted Time Series'][week]['3. low'] 
    return low_dict

def gather_stock_data(stock_name, dates):
    # returns a list of tuples [(ID, stock, low, high),(ID, stock, low, high), ...] #
    request = create_request_url(stock_name)
    data = call_api(request)

    highs_dict = create_high_dict(data, dates)
    lows_dict = create_low_dict(data, dates)

    tup_list = []

    for i in range(105):
        new_tup = (i, stock_name, lows_dict[dates[i]], highs_dict[dates[i]])
        tup_list.append(new_tup)
    
    return tup_list

def create_week_id_table(cur, conn, week_list):
    cur.execute("CREATE TABLE IF NOT EXISTS Dates (id INTEGER PRIMARY KEY, week TEXT UNIQUE)")

    cur.execute('CREATE TABLE IF NOT EXISTS Stocks (stock TEXT, low INTEGER, high INTEGER, week_id INTEGER, FOREIGN KEY (week_id) REFERENCES Dates(id))')

    cur.execute("SELECT MAX(id) FROM Dates")
    max_id = cur.fetchone()

    if (max_id[0] == None):
        max_id = 0
    else:
        max_id = max_id[0] + 1
    
    count = 0
    
    while count < 25 and max_id < len(week_list):
        cur.execute('INSERT OR IGNORE INTO Dates (id, week) VALUES (?,?)', (max_id, week_list[max_id]))

        count += 1
        max_id += 1
    
    conn.commit()
    if (max_id == len(week_list)):
        print("Date table created")
    return

def create_first_stock_table(cur, conn, stock_tup_list):
    cur.execute('SELECT MAX(id) FROM Dates')
    max_date_id = cur.fetchone()

    cur.execute('SELECT COUNT (*) FROM Stocks')
    stock_count = cur.fetchone()

    stock_index = stock_count[0]
    count = 0
    
    if (max_date_id[0] == 104 and stock_index <= 105):
        while count < 25 and stock_index < len(stock_tup_list):
            cur.execute('INSERT OR IGNORE INTO Stocks (stock, low, high, week_id) VALUES (?,?,?,?)',(stock_tup_list[0][1], stock_tup_list[stock_index][2], stock_tup_list[stock_index][3], stock_tup_list[stock_index][0]))

            conn.commit()

            count += 1
            stock_index += 1
    else:
        return
    return

def add_second_stock(cur, conn, stock_tup_list):
    cur.execute('SELECT COUNT(*) FROM Stocks')
    stocks_in_table = cur.fetchone()
    
    cur.execute("SELECT MAX(week_id) FROM Stocks WHERE stock = 'PFE'")
    start_id = cur.fetchone()
    if (start_id[0] == None):
        start_index = 0
    else:
        start_index = start_id[0]+1
    
    if (stocks_in_table[0] >= 105 and stocks_in_table[0] < 210):
        while start_index < len(stock_tup_list):
            cur.execute('INSERT OR IGNORE INTO Stocks (stock, low, high, week_id) VALUES (?,?,?,?)',(stock_tup_list[0][1], stock_tup_list[start_index][2], stock_tup_list[start_index][3], stock_tup_list[start_index][0]))
            
            conn.commit()

            start_index += 1
            cur.execute('SELECT COUNT(*) FROM Stocks')
    else:
        return
    return
        
def add_third_stock(cur, conn, stock_tup_list):
    cur.execute('SELECT COUNT(*) FROM Stocks')
    stocks_in_table = cur.fetchone()

    cur.execute("SELECT MAX(week_id) FROM Stocks WHERE stock = 'MRNA'")
    start_id = cur.fetchone()
    if (start_id[0]) == None:
        start_index = 0
    else:
        start_index = start_id[0]+1
    
    if (stocks_in_table[0] >= 210 and stocks_in_table[0] < 315):
        while start_index < len(stock_tup_list):
            cur.execute('INSERT OR IGNORE INTO Stocks (stock, low, high, week_id) VALUES (?,?,?,?)',(stock_tup_list[0][1], stock_tup_list[start_index][2], stock_tup_list[start_index][3], stock_tup_list[start_index][0]))
            
            conn.commit()

            start_index += 1
    else:
        return
    return

def main(cur, conn):
    week_list = data_keys((call_api(create_request_url('JNJ'))))
    
    create_week_id_table(cur, conn, week_list)

    cur.execute('SELECT COUNT(*) FROM Stocks')
    stock_count = cur.fetchone()
    cur.execute('SELECT COUNT(*) FROM Dates')
    date_count = cur.fetchone()

    if (date_count[0] == 105):
        print("Date table complete")

    if (stock_count[0] < 105 and date_count[0] == 105):
        print("Adding first stock")
        jnj_stock_list = gather_stock_data('JNJ', week_list)
        create_first_stock_table(cur, conn, jnj_stock_list)

    elif (stock_count[0] < 210 and stock_count[0] >= 105):
        print("Adding second stock")
        pfe_stock_list = gather_stock_data('PFE', week_list)
        add_second_stock(cur, conn, pfe_stock_list)

    elif (stock_count[0] >= 210 and stock_count[0] < 315):
        print("Adding third stock")
        mrna_stock_list = gather_stock_data('MRNA', week_list)
        add_third_stock(cur, conn, mrna_stock_list)

    elif (stock_count[0] == 315):
        print("Stock table complete")

if __name__ == "__main__":
    main()
    


