# Copyright：朱清岩
import os

from NYC.POI.config import select_api

CUR_PATH = os.getcwd()
LAST_PATH = os.path.abspath(os.path.join(os.getcwd(), "../../.."))
# 从谷歌API获取POI
import urllib.request
from urllib.parse import quote
import string
import json
import codecs
import numpy
import pandas as pd

# 参数
# lonRange = [-74.0178, -73.9102]  # the range of longitude 经度的范围
# latRange = [40.7005, 40.8317]  # the range of latitude 纬度的范围
lonRange = [-74.0039, -73.9050]
latRange = [40.5712, 40.6937]

lonDivision = 0.004  # 分块查询，每格约0.1km
latDivision = 0.004  # 分块查询，每格约0.1km
radius = 100  # 查询参数 半径 100m
TIMEOUT = 30
outfile = LAST_PATH + "/NYC/googleAPI/PART6/output.csv"

#   Google Key
# googleKey = "your-api-key"

# restaurant_j = json_format(res_test)
print('开始爬取')
print('共有' + str(
    ((lonRange[1] - lonRange[0]) / lonDivision + 1) * ((latRange[1] - latRange[0]) / latDivision + 1)) + '次请求')
input()
count = 0
countLine = 0
place_id_list = list()
is_first = True
total_query = 9116

df = pd.read_csv(outfile)
is_first = False
place_id_list = list(set(df.place_id))
countLine = len(place_id_list)

for lon in numpy.arange(lonRange[0], lonRange[1], lonDivision):
    print('已进行' + str(count) + '次请求，得到' + str(countLine) + '条有效信息，共' + str(total_query) + "次查询")
    if total_query > 10000:
        break
    api_key = select_api()
    for lat in numpy.arange(latRange[0], latRange[1], latDivision):
        print('已进行' + str(count) + '次请求，得到' + str(countLine) + '条有效信息，共' + str(total_query) + "次查询")
        #   发请求
        if (count > 508):
            latlon = str(lat) + ',' + str(lon)
            basic_url = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json?key={0}&location={1}&radius={2}'
            url = basic_url.format(api_key, latlon, radius)
            url = quote(url, safe=string.printable)
            req = urllib.request.urlopen(url, timeout=TIMEOUT)
            response = req.read().decode('utf-8')
            responseJSON = json.loads(response)
            for item in responseJSON['results']:
                # 对每个POI
                place_id = item['place_id']
                types = item['types']
                total_query += 1
                # 如果id不在已有的list里
                if not place_id in place_id_list:
                    # 如果类型中有point_of_interest
                    if is_first:
                        df = pd.json_normalize(item)
                        is_first = False
                    else:
                        df2 = pd.json_normalize(item)
                        df = pd.concat([df, df2], ignore_index=True)
                    countLine = countLine + 1
                    place_id_list.append(place_id)
                    '''
                    if "point_of_interest" in types:
                        place_id_list.append(place_id)
                        line = str(item['geometry']['location']['lat']) + ',' + str(item['geometry']['location']['lng'])
                        for type in types:
                            line = line + ',' + str(type)
                        csvFile.write(line + '\n')
                        countLine = countLine + 1
                    '''
        count = count + 1
        if is_first == False:
            df.to_csv(outfile)
        if total_query > 10000:
            break

# df.to_csv(outfile)
print('结束')
