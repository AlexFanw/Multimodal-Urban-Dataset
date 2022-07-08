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


def get_poi_reviews(num=100, api_key="AIzaSyBRwq8CIk8k9Y4U2PNDXKC2wg9hTDRHnsI"):
    """从dataset中获取所有poi的id，并爬取评论

    :param num: 需要爬取的条目数量
    :param api_key: google 开放平台api
    :return:
    """
    sql = "SELECT id, place_id FROM nyc_poi WHERE id > 19583 LIMIT {}".format(num)
    conn = connect_mysql()
    cur = conn.cursor()
    cur.execute(sql)
    place_list = cur.fetchall()
    for item in place_list:
        print(item[0], item[1])
        retry_time = 0
        while retry_time < 3:
            try:
                # 解析reviews
                resp = get_poi_details(item[1], ["reviews"], api_key)
                if "result" in resp and resp["result"] != {} and len(resp["result"]["reviews"]) != 0:
                    for r in resp["result"]["reviews"]:
                        print(r["rating"], r["text"])
                        sql = "INSERT INTO nyc_poi_reviews (place_id, rating, reviews) VALUES ('{}', '{}', '{}')".format(item[1], r["rating"], r["text"].replace("'", "\\'"))
                        try:
                            cur.execute(sql)
                            conn.commit()
                        except:
                            continue

                print("***扫描POI序号：{}".format(item[0]))
                break
            except requests.exceptions.SSLError as e:
                retry_time += 1
                print(">>>爬取失败次数：{}\n".format(retry_time))


if __name__ == "__main__":
    # place_id = "ChIJHxxTHBJawokRb6_IiD4cn8s"
    # details = ["photos", "reviews"]
    # get_poi_details(place_id, details)
    get_poi_reviews(num=100000)
