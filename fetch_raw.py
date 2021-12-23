import requests
import json
import mysql.connector
from mysql.connector import errorcode
from datetime import datetime
from tqdm import tqdm

DB = 'opendata'

TABLES = {}
TABLES['activity'] = (
    "CREATE TABLE activity ("
    "  uid VARCHAR(24) NOT NULL,"
    "  version FLOAT(2),"
    "  title VARCHAR(150) NOT NULL,"
    "  category INT(2) NOT NULL,"
    "  showUnit VARCHAR(250),"
    "  discountInfo TEXT,"
    "  descriptionFilterHtml TEXT,"
    "  imageUrl VARCHAR(300),"
    "  webSales VARCHAR(200),"
    "  sourceWebPromote VARCHAR(200),"
    "  comment TEXT,"
    "  editModifyDate DATETIME,"
    "  sourceWebName VARCHAR(100),"
    "  startDate DATETIME,"
    "  endDate DATETIME,"
    "  hitRate INT(10),"
    "  PRIMARY KEY (uid)"
    ");"
    )

TABLES['showInfo'] = (
    "CREATE TABLE showInfo ("
    "  id INT NOT NULL AUTO_INCREMENT,"
    "  uid VARCHAR(24),"
    "  time DATETIME,"
    "  location VARCHAR(100),"
    "  locationName VARCHAR(100),"
    "  onSales VARCHAR(20),"
    "  price TEXT,"
    "  latitude DECIMAL(10, 8),"
    "  longitude DECIMAL(11, 8),"
    "  endTime DATETIME,"
    "  PRIMARY KEY (id),"
    "  FOREIGN KEY (uid) REFERENCES activity(uid) ON DELETE CASCADE"
    ");"
)

TABLES['masterUnit'] = (
    "CREATE TABLE masterUnit ("
    "  id INT NOT NULL AUTO_INCREMENT,"
    "  uid VARCHAR(24),"
    "  unit VARCHAR(250),"
    "  PRIMARY KEY (id),"
    "  FOREIGN KEY (uid) REFERENCES activity(uid) ON DELETE CASCADE"
    ");"
)

TABLES['subUnit'] = (
    "CREATE TABLE subUnit ("
    "  id INT NOT NULL AUTO_INCREMENT,"
    "  uid VARCHAR(24),"
    "  unit VARCHAR(250),"
    "  PRIMARY KEY (id),"
    "  FOREIGN KEY (uid) REFERENCES activity(uid) ON DELETE CASCADE"
    ");"
)

TABLES['supportUnit'] = (
    "CREATE TABLE supportUnit ("
    "  id INT NOT NULL AUTO_INCREMENT,"
    "  uid VARCHAR(24),"
    "  unit VARCHAR(250),"
    "  PRIMARY KEY (id),"
    "  FOREIGN KEY (uid) REFERENCES activity(uid) ON DELETE CASCADE"
    ");"
)

TABLES['otherUnit'] = (
    "CREATE TABLE otherUnit ("
    "  id INT NOT NULL AUTO_INCREMENT,"
    "  uid VARCHAR(24),"
    "  unit VARCHAR(250),"
    "  PRIMARY KEY (id),"
    "  FOREIGN KEY (uid) REFERENCES activity(uid) ON DELETE CASCADE"
    ");"
)

def create_connection():
    with open("config.json", "r") as jsonfile:
        config = json.load(jsonfile)
    cnx = mysql.connector.connect(**config)
    return cnx

def create_database(cnx):
    cursor = cnx.cursor()
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB))
        print("Database {} created successfully.".format(DB))
    except mysql.connector.Error as err:
        print("Failed to create database: {}".format(err))
        exit(1)
    cursor.close()

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

def string2datetime(s):
    try:
        return datetime.fromisoformat(s.replace('/', '-'))
    except:
        return None

def string2float(s):
    try:
        return float(s)
    except:
        return None

def insert_data(cnx):
    cursor = cnx.cursor()
    cursor.execute("USE {}".format(DB))
    add_activity = ("INSERT IGNORE INTO activity"
                    "(uid, version, title, category, showUnit, discountInfo, "
                    "descriptionFilterHtml, imageUrl, webSales, sourceWebPromote, "
                    "comment, editModifyDate, sourceWebName, startDate, endDate, hitRate)"
                    "VALUES (%(uid)s, %(version)s, %(title)s, %(category)s, %(showUnit)s,"
                    "%(discountInfo)s, %(descriptionFilterHtml)s, %(imageUrl)s, %(webSales)s,"
                    "%(sourceWebPromote)s, %(comment)s, %(editModifyDate)s, %(sourceWebName)s,"
                    "%(startDate)s, %(endDate)s, %(hitRate)s);"
    )
    add_showinfo = ("INSERT IGNORE INTO showInfo"
                    "(uid, time, location, locationName, onSales, price,"
                    "latitude, longitude, endTime)"
                    "VALUES (%(uid)s, %(time)s, %(location)s, %(locationName)s, %(onSales)s,"
                    "%(price)s, %(latitude)s, %(longitude)s, %(endTime)s);"
    )
    add_unit = ("INSERT IGNORE INTO {}"
                "(uid, unit)"
                "VALUES (%(uid)s, %(unit)s);"
    )
    url = 'https://cloud.culture.tw/frontsite/trans/SearchShowAction.do?method=doFindTypeJ&category='
    for i in tqdm(range(1, 18)):
        r = requests.get(url + str(i))
        for j, activity in enumerate(r.json()):
            activity_data = {
                'uid': activity['UID'],
                'version': float(activity['version']),
                'title': activity['title'],
                'category': int(activity['category']),
                'showUnit': activity['showUnit'],
                'discountInfo': activity['discountInfo'],
                'descriptionFilterHtml': activity['descriptionFilterHtml'],
                'imageUrl': activity['imageUrl'],
                'webSales': activity['webSales'],
                'sourceWebPromote': activity['sourceWebPromote'],
                'comment': activity['comment'],
                'editModifyDate': string2datetime(activity['editModifyDate']),
                'sourceWebName': activity['sourceWebName'],
                'startDate': string2datetime(activity['startDate']),
                'endDate': string2datetime(activity['endDate']),
                'hitRate': int(activity['hitRate'])
            }
            cursor.execute(add_activity, activity_data)
            for show in activity['showInfo']:
                show_data = {
                    'uid': activity['UID'],
                    'time': string2datetime(show['time']),
                    'location': show['location'],
                    'locationName': show['locationName'],
                    'onSales': show['onSales'],
                    'price': show['price'],
                    'latitude': string2float(show['latitude']),
                    'longitude': string2float(show['longitude']),
                    'endTime': string2datetime(show['endTime'])
                }
                cursor.execute(add_showinfo, show_data)
            for unit in activity['masterUnit']:
                unit_data = {
                    'uid': activity['UID'],
                    'unit': unit
                }
                cursor.execute(add_unit.format('masterUnit'), unit_data)
            for unit in activity['subUnit']:
                unit_data = {
                    'uid': activity['UID'],
                    'unit': unit
                }
                cursor.execute(add_unit.format('subUnit'), unit_data)
            for unit in activity['supportUnit']:
                unit_data = {
                    'uid': activity['UID'],
                    'unit': unit
                }
                cursor.execute(add_unit.format('supportUnit'), unit_data)
            for unit in activity['otherUnit']:
                unit_data = {
                    'uid': activity['UID'],
                    'unit': unit
                }
                cursor.execute(add_unit.format('otherUnit'), unit_data)
            print('Sucessfully insert the {}th activity in category {}.'.format(j, i))
        cnx.commit()



if __name__ == "__main__":
    cnx = create_connection()
    create_database(cnx)
    create_tables(cnx)
    insert_data(cnx)
    cnx.close()