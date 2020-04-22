from corona_data_collector.config import gps_url, gps_url_key
import requests

NOT_FOUND = -1
FROM_WEB = 0
FROM_LIST = 1


def get_coords_from_web(street, city):
    street_accurate = street != city
    response = requests.get(f'{gps_url}?key={gps_url_key}&address={street} {city}')
    if response.status_code == 200:
        response_object = response.json()
        if response_object['status'] == 'OK' and len(response_object['results']) > 0:
            location = response_object['results'][0]['geometry']['location']
            if location is not None:
                return location['lat'], location['lng'], int(street_accurate)
        else:
            return 0, 0, 0
    else:
        print('failed to get gps data from web, code received: ', response.status_code)
        print(response.content)
        return -1, -1, -1
