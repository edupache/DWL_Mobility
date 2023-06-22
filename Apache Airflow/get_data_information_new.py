import datetime
import logging
import json
import os
import psycopg2
import pandas as pd
import requests
import numpy as np

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.timetables.trigger import CronTriggerTimetable


def get_data_information():
    API_URL2 = "https://sharedmobility.ch/station_information.json"
    data = requests.get(API_URL2)
    data = data.json()
    information = data['data']['stations']
    logging.info(f'API2 extracted')
    rows = len(information)
    logging.info(f'{rows} extracted')
    sql = "INSERT INTO infodb (load, station_id,name,lat,lon,provider_id, address,region_id,post_code) VALUES (" \
          "Current_timestamp,%s,%s,%s,%s,%s,%s,%s,%s)"
    val = []
    TotalCounter = 0
    counter = 0
    pg_hook = PostgresHook(postgres_conn_id="rds", schema="sharedmobility")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    for i in range(rows):
        counter += 1
        TotalCounter += 1
        try:
            station_id = information[i]["station_id"]
        except:
            station_id = 'None'
        try:
            name = information[i]["name"]
        except:
            name = 'None'
        try:
            lat = information[i]["lat"]
        except:
            lat = -1.0
        try:
            lon = information[i]["lon"]
        except:
            lon = -1.0
        try:
            provider_id = information[i]["provider_id"]
        except:
            provider_id = 'None'
        try:
            address = information[i]["address"]
        except:
            address = 'None'
        try:
            region_id = information[i]["region_id"]
        except:
            region_id = 'None'
        try:
            post_code = information[i]["post_code"]
        except:
            post_code = 'None'
        val.append((station_id, name, lat, lon, provider_id, address,
                    region_id, post_code))
        if counter == 100:
            cursor.executemany(sql, val)
            connection.commit()
            logging.info(f'{cursor.rowcount} details inserted')
            val = []
            counter = 0
    cursor.executemany(sql, val)
    cursor.close()
    connection.close()



dag = DAG(
    "get_data_information_new",
    schedule_interval='0 6,8,10,12,14,17 * * *',
    start_date=datetime.datetime(2023, 4, 18))


get_data_information = PythonOperator(
   task_id="get_data_information",
   python_callable=get_data_information,
   dag=dag
)


create_table_information = PostgresOperator(
    task_id="create_table_information",
    dag=dag,
    postgres_conn_id='rds',
    sql='''
            CREATE TABLE IF NOT EXISTS infodb (load timestamp, station_id varchar(255), name varchar(255), lat float4, lon float4, provider_id varchar(255), address varchar(255), region_id varchar(255), post_code varchar(255));
        '''
)



# Configure Task Dependencies
create_table_information >> get_data_information

