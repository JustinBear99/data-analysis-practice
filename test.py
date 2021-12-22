import requests
import tqdm

if __name__ == "__main__":
    url = 'https://cloud.culture.tw/frontsite/trans/SearchShowAction.do?method=doFindTypeJ&category='
    activity_keys = ['title', 'UID', 'showUnit', 'discountInfo', 'descriptionFilterHtml', 'imageUrl',
                     'webSales', 'sourceWebPromote', 'comment', 'editModifyDate', 'sourceWebName']
    showinfo_keys = ['time', 'location', 'locationName', 'onSales', 'price']
    max_lengths_activity = dict.fromkeys(activity_keys, 0)
    max_lengths_showinfo = dict.fromkeys(showinfo_keys, 0)
    for i in tqdm.tqdm(range(1, 18)):
        r = requests.get(url + str(i))
        for activity in tqdm.tqdm(r.json(), leave=False):
            for key in max_lengths_activity.keys():
                length = max_lengths_activity[key]
                max_lengths_activity[key] = max(length, len(activity[key]))
            for show in activity['showInfo']:
                for key in max_lengths_showinfo.keys():
                    length = max_lengths_showinfo[key]
                    max_lengths_showinfo[key] = max(length, len(show[key]))
    print(f'{max_lengths_activity=}')
    print(f'{max_lengths_showinfo=}')

    # Results
    # max_lengths_activity={'title': 138, 'UID': 24, 'showUnit': 209, 'discountInfo': 573, 'descriptionFilterHtml': 5715, 'imageUrl': 254, 'webSales': 153, 'sourceWebPromote': 155, 'comment': 637, 'editModifyDate': 19, 'sourceWebName': 16}                  
    # max_lengths_showinfo={'time': 19, 'location': 66, 'locationName': 34, 'onSales': 7, 'price': 494}