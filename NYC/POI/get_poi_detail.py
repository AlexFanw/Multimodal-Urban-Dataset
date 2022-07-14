import requests
import json

def get_poi_details(place_id, details, api_key):
    """获取poi详情数据

    :param place_id: poi唯一标识符
    :param details: list，包括所有需要获取的信息名称，如rating，name，photos
    :param api_key: google 开放平台api
    :return: poi相关信息
    """
    retry_time = 0
    while retry_time < 3:
        try:
            url = "https://maps.googleapis.com/maps/api/place/details/json?place_id={}&fields={}&key={}".format(place_id,
                                                                                                                "%2C".join(
                                                                                                                    details),
                                                                                                                api_key)
            payload = {}
            headers = {}
            response = requests.request("GET", url, headers=headers, data=payload)
            # print(response.text)
            return json.loads(response.text)
        except:
            retry_time += 1
            print("get_poi_detail失败")
    return {}