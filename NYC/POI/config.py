# Basic Configuration
from NYC.POI.get_poi_detail import get_poi_details

"""
API_KEY中，请前往https://console.cloud.google.com/google/maps-apis/api-list
并为Places API设置配额为15000，以免超出$300金额限制
"""
API_KEY = ["xxxxxxxxx",
           "-AIzaSyBdBtdYuBXZ2reBK0nSKOvpIBxL4o-HY1U",
           "-AIzaSyCkrmymBaCBJ8DLoL57ej7RDO027HvmnRU",
           ]  # 请前往Google Map自行申请
PHOTO_ID = 9156
REVIEWS_ID = 40766


def select_api():
    for i in API_KEY:
        resp = get_poi_details('ChIJY7RPnE1YwokRVXAVy9Fh7fg', ["reviews"], i)
        if "error_message" in resp or resp == {}:
            continue
        else:
            print("切换至免费账户:", i)
            return i


if __name__ == "__main__":
    print(select_api())
