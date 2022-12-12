import sqlite3
import os
import time
import pandas as pd
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import seaborn as sns

import stocks_api
import news_scrape
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

        if (row_count[0] == 315):
            print("Stocks table complete")
            break

        time.sleep(20)
    return

def covid_table(cur, conn):
    for i in range(5):
        covid_api.main(cur, conn)
        cur.execute('SELECT COUNT (*) FROM Covid')
        
        row_count = cur.fetchone()

        if (row_count[0] == 416):
            print("Covid table complete")
            break
        time.sleep(15)
    return

def news_table(cur,conn):
    for i in range(15):
        news_scrape.main(cur, conn)
        cur.execute('SELECT COUNT (*) FROM news')
        row_count = cur.fetchone()

        if (row_count[0] == 729):
            print("News table complete")
            break

        time.sleep(35)
    
    return("News table created")

def top_covid_weeks(cur, conn):
    cur.execute('SELECT DISTINCT(c.week_id), c.weekly_avg FROM Covid c ORDER BY c.weekly_avg DESC LIMIT 5')

    result = cur.fetchall()
    total = 0

    list1 = []
    list2 = []

    for i in result:
        total += i[1]
        list1.append(i[0])
        list2.append(i[1])
    total = total/5

    fig = go.Figure(data = [go.Bar(name = "Average covid cases", x=list1, y=list2, marker_color="rgb(255, 184, 243)")])
    fig.add_hline(y=total)
    fig.show()

    return total

def avg_difference(cur, conn):
    cur.execute('SELECT s.stock, AVG (difference) FROM Stocks s GROUP BY s.stock')
    result = cur.fetchall()

    list1 = []
    list2 = []

    for i in result:
        list1.append(i[1])
        list2.append(i[0])

    color = sns.color_palette("pastel")[0:5]
    plt.pie(list1, labels= list2, colors=color, autopct="%.0f%%")
    plt.show()

    return(result)

def diff_per_word(cur, conn):
    cur.execute('''SELECT DISTINCT(c.week_id), s.difference, c.weekly_kw_count 
        FROM news c JOIN Stocks s 
        ON c.week_id=s.week_id 
        ORDER BY s.difference 
        DESC LIMIT 10''')
    result = cur.fetchall()

    difference = 0
    kw = 0

    list1 = []
    list2 = []
    list0 = []

    for i in result:
        difference += i[1]
        kw += i[2]
        list0.append(i[0])
        list1.append(i[1])
        list2.append(i[2])
    difference = difference/len(result)
    kw = kw/len(result)
    avg = difference/kw

    df = pd.DataFrame({'difference': list1, 'keyword_count': list2, 'week_id': list0})
    sns.scatterplot(data=df, x="difference", y="keyword_count", hue='week_id')
    plt.show()

    return(avg)


# Do math using tables in final.db
# Export math to text file
# Visualizations #

def main():
    cur, conn = create_database('final.db')

    covid_table(cur, conn)
    stocks_tables(cur, conn)
    news_table(cur, conn)

    diff_per_word(cur, conn)
    with open('calc.txt', 'w') as fhand:
        fhand.write(f'The average of the top five weeks with the highest covid cases is about: {round(top_covid_weeks(cur, conn))} cases. \n\n')
        fhand.write(f'The average difference between the highest and lowest price of each stock is: \n')
        avg_list = avg_difference(cur, conn)
        for i in avg_list:
            fhand.write(i[0])
            fhand.write(": ")
            fhand.write(str(i[1]))
            fhand.write("\n")
        fhand.write('\n')
        fhand.write(f'The ratio of the average difference of the top ten stocks and the keyword count: {round(diff_per_word(cur,conn), 2)} weekly stock difference/keyword_count. \n')


if __name__ == "__main__":
    main()
    