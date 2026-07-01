import os
import socket
import json
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


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

def get_kafka_broker():
    
    if os.getenv('KAFKA_BOOTSTRAP_SERVERS'):
        return os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    
    
    try:
        
        socket.gethostbyname('broker')
        return 'broker:29092'
    except socket.gaierror:
        
        return '127.0.0.1:9092'
    
 
def stream_data():
    
    from kafka import KafkaProducer
    
    broker_address = get_kafka_broker()
    print(f"Connecting to Kafka broker at: {broker_address}")

    try:
        raw_data = get_data()
        clean_data = format_data(raw_data)
        
        producer = KafkaProducer(
            bootstrap_servers=[broker_address], 
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            max_block_ms=5000
        )

        producer.send('food_ratings', value=clean_data)
        producer.flush()
        print("Message successfully flushed to Kafka.")
    except Exception as e:
        print(f"Failed to execute streaming flow: {e}")
        raise e
    
        


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
}
    
    
with DAG(
    'user_automation',
    default_args=default_args,
    schedule='@daily',
    catchup=False
) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )



