import json

import requests

from NYC.POI.config import API_KEY
from NYC.POI.connect_mysql import connect_mysql
from NYC.POI.get_poi_detail import get_poi_details


def get_poi_photos(num=100, api_key=API_KEY):
    """从dataset中获取所有poi的id，并爬取图片和评论

    :param num: 需要爬取的条目数量
    :param api_key: google 开放平台api
    :return:
    """
    sql = "SELECT id, place_id FROM nyc_poi WHERE photos != '' and id > 5365 LIMIT {}".format(num)
    conn = connect_mysql()
    cur = conn.cursor()
    cur.execute(sql)
    place_list = cur.fetchall()
    for item in place_list:
        print(item[0], item[1])
        retry_time = 0
        while retry_time < 3:
            try:
                # 爬取photos
                resp = get_poi_details(item[1], ["photos"], api_key)
                if "result" in resp and "photos" in resp["result"] and len(resp["result"]["photos"]) != 0:
                    resp = get_poi_details(item[1], ["photos"], api_key)
                    photos = resp["result"]["photos"]
                    p_num = 0
                    for p in photos:
                        if p_num == 5:
                            break  # 最多抓取五张图片
                        url = "https://maps.googleapis.com/maps/api/place/photo?maxwidth=1600&maxheight=1080&photo_reference={}&key={}".format(
                            p["photo_reference"], api_key)
                        payload = {}
                        headers = {}
                        try:
                            response = requests.request("GET", url, headers=headers, data=payload)
                            photo_file_name = "POI_photos/" + str(item[1]) + "_" + str(p_num) + ".jpeg"
                            with open(photo_file_name, "wb") as f:
                                f.write(response.content)
                            p_num += 1
                            print(photo_file_name, "爬取成功")
                        except requests.exceptions.SSLError as e:
                            print(photo_file_name, "爬取失败")
                print("***扫描POI序号：{}".format(item[0]))
                break
            except requests.exceptions.SSLError as e:
                retry_time += 1
                print(">>>爬取失败次数：{}\n".format(retry_time))


if __name__ == "__main__":
    get_poi_photos(num=100000)
