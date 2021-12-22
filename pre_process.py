import json
import mysql.connector
import pandas as pd
from re import search
from fetch_raw import create_connection

def check_categories(cnx):
    categories = {1: '音樂', 2: '戲曲', 3: '舞蹈', 4: '親子', 5: '獨立音樂', 6: '展覽', 7: '講座',
                  8: '電影', 11: '綜藝', 13: '競賽', 14: '徵選', 15: '其他', 16: '研習課程', 17: '演唱會'}
    cursor = cnx.cursor(dictionary=True)
    query = ("SELECT category, COUNT(category) AS numCategory "
             "FROM activity GROUP BY category "
             "ORDER BY category;")
    cursor.execute("USE opendata")
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    for r in result:
        r['類別'] = categories[r['category']]
    return pd.DataFrame(result)


def top_n_hitRate(cnx, n=int):
    cursor = cnx.cursor(dictionary=True)
    query = ("SELECT title, hitRate, category, webSales, sourceWebPromote,"
             "COUNT(showInfo.uid) AS numShows, masterUnit.unit AS `masterUnit`, startDate, endDate "
             "FROM activity "
             "LEFT JOIN showInfo ON activity.uid=showInfo.uid "
             "LEFT JOIN masterUnit ON activity.uid=masterUnit.uid "
             "GROUP BY activity.uid "
             "ORDER BY hitRate DESC LIMIT {};".format(n))
    cursor.execute("USE opendata")
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return pd.DataFrame(result)

def shows_at_cities_of_all_categories(cnx):
    cities = ['臺北市', '台北市', '新北市', '基隆市', '桃園市', '新竹縣',
              '新竹市', '苗栗縣', '臺中市', '台中市', '南投縣', '彰化縣',
              '雲林縣', '嘉義縣', '嘉義市', '臺南市', '台南市', '高雄市',
              '屏東縣', '宜蘭縣', '花蓮縣', '臺東縣', '台東縣', '澎湖縣',
              '金門縣', '連江縣', '線上']
    categories = list(range(1, 19))
    cities_count = dict.fromkeys(cities, 0)
    # Create dict to count the number of each category of every city.
    # {'臺北市': {1:0, 2:0, 3:0, ...}, ...}
    for city in cities_count:
        cities_count[city] = dict.fromkeys(categories, 0)
    
    cursor = cnx.cursor(dictionary=True)
    query = "SELECT showInfo.location, activity.category FROM showInfo, activity WHERE showInfo.uid=activity.uid;"
    cursor.execute("USE opendata")
    cursor.execute(query)
    result = cursor.fetchall()

    for r in result:
        location, category = r['location'], r['category']
        for city in cities:
            if search(city, location):
                cities_count[city][category] += 1
                cities_count[city][18] += 1
                
    for i in range(1, 19):
        cities_count['臺北市'][i] += cities_count['台北市'][i]
        cities_count['臺中市'][i] += cities_count['台中市'][i]
        cities_count['臺南市'][i] += cities_count['台南市'][i]
        cities_count['臺東縣'][i] += cities_count['台東縣'][i]
    cities_count.pop('台北市')
    cities_count.pop('台中市')
    cities_count.pop('台南市')
    cities_count.pop('台東縣')

    return cities_count

if __name__ == "__main__":
    cnx = create_connection()
    num_categories = check_categories(cnx)
    print(num_categories)
    counts = shows_at_cities_of_all_categories(cnx)
    print(counts)
    top100 = top_n_hitRate(cnx, 100)
    print(top100)