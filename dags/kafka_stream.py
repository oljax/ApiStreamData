from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1, 10, 00)
}

def get_data():
    import requests
    
    res = requests.get("https://ratings.food.gov.uk/api/open-data-files/FHRS529en-GB.json")
    result = res.json()
    first_item = result["FHRSEstablishment"]["EstablishmentCollection"][0]

    return first_item

def format_data(first_item):
    data = {}
    data['BusinessName'] = first_item['BusinessName']
    data['BusinessType'] = first_item['BusinessType']
    data['RatingValue'] = first_item['RatingValue']
    data['RatingDate'] = first_item['RatingDate']
    data['LocalAuthorityCode'] = first_item['LocalAuthorityCode']
    data['LocalAuthorityName'] = first_item['LocalAuthorityName']
    data['LocalAuthorityWebSite'] = first_item['LocalAuthorityWebSite']
    data['LocalAuthorityEmailAddress'] = first_item['LocalAuthorityEmailAddress']
    
    # 1. Safely fetch the Geocode object (defaults to None if missing)
    geocode = first_item.get('Geocode')

# 2. Check if geocode exists and is not None
    if geocode and 'Longitude' in geocode and 'Latitude' in geocode:
        data['geo_Longitude'] = float(geocode['Longitude'])
        data['geo_Latitude'] = float(geocode['Latitude'])
    else:
    # 3. Provide fallback values (None or 0.0) so your pipeline keeps moving
        data['geo_Longitude'] = None
        data['geo_Latitude'] = None

    
    return data
    
 
def stream_data():
    import json
    res = get_data()
    res = format_data(res)
    print(json.dumps(res, indent=3))
    
   


    

    
    # print(type(result["FHRSEstablishment"]["EstablishmentCollection"]))
    
    
# dag = DAG(
#     'user_automation',
#     default_args=default_args,
#     schedule_interval='@daily',
#     catchup=False
# )

# streaming_task = PythonOperator(
#     task_id = 'stream_data_from_api',
#     python_callable = stream_data
# )

stream_data()

