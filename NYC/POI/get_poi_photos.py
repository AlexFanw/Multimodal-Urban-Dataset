import json

import requests

from NYC.POI.connect_mysql import connect_mysql


def get_poi_details(place_id, details, api_key="AIzaSyBRwq8CIk8k9Y4U2PNDXKC2wg9hTDRHnsI"):
    """获取poi详情数据

    :param place_id: poi唯一标识符
    :param details: list，包括所有需要获取的信息名称，如rating，name，photos
    :param api_key: google 开放平台api
    :return: poi相关信息
    """
    url = "https://maps.googleapis.com/maps/api/place/details/json?place_id={}&fields={}&key={}".format(place_id,
                                                                                                        "%2C".join(
                                                                                                            details),
                                                                                                        api_key)
    payload = {}
    headers = {}
    response = requests.request("GET", url, headers=headers, data=payload)
    # print(response.text)
    return json.loads(response.text)


def get_poi_photos(num=100, api_key="AIzaSyBRwq8CIk8k9Y4U2PNDXKC2wg9hTDRHnsI"):
    """从dataset中获取所有poi的id，并爬取图片和评论

    :param num: 需要爬取的条目数量
    :param api_key: google 开放平台api
    :return:
    """
    sql = "SELECT id, place_id FROM nyc_poi WHERE photos != '' and id > 4054 LIMIT {}".format(num)
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
                        if p_num == 5: break
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
    # place_id = "ChIJHxxTHBJawokRb6_IiD4cn8s"
    # details = ["photos", "reviews"]
    # get_poi_details(place_id, details)
    get_poi_photos(num=100000)
