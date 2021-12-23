import mysql.connector
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from mysql.connector import errorcode
from fetch_raw import create_connection
from pre_process import top_n_hitRate, shows_at_cities_of_all_categories

DB = "opendata"
TABLES = {}
TABLES['topActivity'] = (
    "CREATE TABLE topActivity ("
    "  id INT NOT NULL AUTO_INCREMENT,"
    "  title VARCHAR(150) NOT NULL,"
    "  hitRate INT(10),"
    "  category INT(2),"
    "  categoryCH VARCHAR(10),"
    "  webSales VARCHAR(200),"
    "  sourceWebPromote VARCHAR(200),"
    "  masterUnit VARCHAR(250),"
    "  startDate DATETIME,"
    "  endDate DATETIME,"
    "  PRIMARY KEY (id)"
    "  );"
    )
TABLES['cityCategories'] = (
    "CREATE TABLE cityCategories ("
    "  id INT NOT NULL AUTO_INCREMENT,"
    "  category INT(2),"
    "  categoryCH VARCHAR(10),"
    "  city VARCHAR(3),"
    "  number INT(5),"
    "  PRIMARY KEY (id)"
    "  );"
)

def create_tables(cnx):
    cursor = cnx.cursor()
    try:
        cursor.execute("USE {}".format(DB))
        print("Use {}".format(DB))
    except mysql.connector.Error as err:
        print("Database {} does not exist.".format(DB))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor)
            print("Database {} created successfully.".format(DB))
            cnx.database = DB
        else:
            print(err)
            exit(1)
    for table in TABLES:
        query = TABLES[table]
        try:
            print("Creating table {}: ".format(table), end='')
            cursor.execute(query)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")
    cursor.close()

def insert_top_activities(cnx):
    cursor = cnx.cursor()
    cursor.execute("USE {}".format(DB))
    df = top_n_hitRate(cnx, 100)
    categories = {1: '音樂', 2: '戲曲', 3: '舞蹈', 4: '親子', 5: '獨立音樂', 6: '展覽', 7: '講座',
                  8: '電影', 11: '綜藝', 13: '競賽', 14: '徵選', 15: '其他', 16: '研習課程', 17: '演唱會'}
    add_data = ("INSERT INTO topActivity"
                "(title, hitRate, category, categoryCH, webSales, "
                "sourceWebPromote, masterUnit, startDate, endDate) "
                "VALUES (%(title)s, %(hitRate)s, %(category)s, %(categoryCH)s, %(webSales)s,"
                "%(sourceWebPromote)s, %(masterUnit)s, %(startDate)s, %(endDate)s);"
    )
    for i, row in tqdm(df.iterrows()):
        try:
            startDate = datetime.fromisoformat(str(row['startDate']))
        except:
            startDate = None
        try:
            endDate = datetime.fromisoformat(str(row['endDate']))
        except:
            endDate = None
        data = {
            'title': row['title'],
            'hitRate': int(row['hitRate']),
            'category': int(row['category']),
            'categoryCH': categories[int(row['category'])],
            'webSales': row['webSales'],
            'sourceWebPromote': row['sourceWebPromote'],
            'masterUnit': row['masterUnit'],
            'startDate': startDate,
            'endDate': endDate
        }
        cursor.execute(add_data, data)
        cnx.commit()
    cursor.close()

def insert_city_categories(cnx):
    cursor = cnx.cursor()
    cursor.execute("USE {}".format(DB))
    df = pd.DataFrame(shows_at_cities_of_all_categories(cnx))
    categories = {1: '音樂', 2: '戲曲', 3: '舞蹈', 4: '親子', 5: '獨立音樂', 6: '展覽', 7: '講座',
                  8: '電影', 11: '綜藝', 13: '競賽', 14: '徵選', 15: '其他', 16: '研習課程', 17: '演唱會', 18: '總計'}
    add_data = ("INSERT INTO cityCategories"
                "(category, categoryCH, city, number) "
                "VALUES ( %(category)s, %(categoryCH)s, %(city)s, %(number)s);"
    )
    for i, row in tqdm(df.iterrows()):
        if i not in categories.keys():
            continue
        for city, num in row.items():
            data = {
                'category': i,
                'categoryCH': categories[i],
                'city': city,
                'number': num
            }
            cursor.execute(add_data, data)
            cnx.commit()
    cursor.close()

if __name__ == "__main__":
    cnx = create_connection()
    create_tables(cnx)
    insert_top_activities(cnx)
    insert_city_categories(cnx)
    cnx.close()