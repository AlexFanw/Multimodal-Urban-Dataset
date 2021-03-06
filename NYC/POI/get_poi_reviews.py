import requests

from config import REVIEWS_ID, select_api
from connect_mysql import connect_mysql
from get_poi_detail import get_poi_details


def get_poi_reviews(num=100, api_key=""):
    """从dataset中获取所有poi的id，并爬取评论

    :param num: 需要爬取的条目数量
    :param api_key: google 开放平台api
    :return:
    """
    sql = "SELECT id, place_id FROM nyc_poi WHERE id > {} LIMIT {}".format(REVIEWS_ID, num)
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
                # print(resp)
                if "error_message" in resp or resp == {}:
                    api_key = select_api()  # 免费额度使用完毕后切换账号
                    if api_key is None:
                        return
                if "result" in resp and resp["result"] != {} and len(resp["result"]["reviews"]) != 0:
                    for r in resp["result"]["reviews"]:
                        # print(r["rating"], r["text"])
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
