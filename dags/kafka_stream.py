from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests
import uuid
from kafka import KafkaProducer
import time

default_args = {
    "owner": "Phi Nguyen",
    "start_date": datetime(2024, 7, 19, 1, 00)
}

def get_data():
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    return res['results'][0]

def format_data(user_data):
    "Extract information from raw data."
    data = {}
    location = user_data['location']
    data['id'] = str(uuid.uuid4())
    data['first_name'] = user_data['name']['first']
    data['last_name'] = user_data['name']['last']
    data['gender'] = user_data['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = user_data['email']
    data['username'] = user_data['login']['username']
    data['dob'] = user_data['dob']['date']
    data['registered_date'] = user_data['registered']['date']
    data['phone'] = user_data['phone']
    data['picture'] = user_data['picture']['medium']

    return data

def stream_data():
    user_info = get_data()
    user_info = format_data(user_info)
    
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        max_block_ms=5000
    )

    producer.send('user_created', json.dumps(user_info).encode('utf-8'))

stream_data()

# with DAG("user_automation",
#          default_args=default_args,
#          schedule_interval='@daily',
#          catchup=False) as dag:
#     streaming_task = PythonOperator(
#         task_id="stream_data_from_api",
#         python_callable=stream_data
#     )