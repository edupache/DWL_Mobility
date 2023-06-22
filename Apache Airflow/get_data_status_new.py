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


def get_data_status():
    API_URL1 = "https://sharedmobility.ch/station_status.json"
    data = requests.get(API_URL1)
    data = data.json()
    status = data['data']['stations']
    logging.info(f'API1 extracted')
    rows = len(status)
    logging.info(f'{rows} extracted')
    sql = "INSERT INTO statusdb (load, station_id,is_installed,is_renting,is_returning,last_reported, " \
          "num_bikes_available, num_docks_available,provider_id) VALUES (Current_timestamp,%s,%s,%s,%s,%s,%s,%s,%s)"
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
            station_id = status[i]["station_id"]
        except:
            station_id = 'None'
        try:
            is_installed = status[i]["is_installed"]
        except:
            is_installed = 'None'
        try:
            is_renting = status[i]["is_renting"]
        except:
            is_renting = 'None'
        try:
            is_returning = status[i]["is_returning"]
        except:
            is_returning = 'None'
        try:
            last_reported = status[i]["last_reported"]
        except:
            last_reported = -1
        try:
            num_bikes_available = status[i]["num_bikes_available"]
        except:
            num_bikes_available = -1
        try:
            num_docks_available = status[i]["num_docks_available"]
        except:
            num_docks_available = -1
        try:
            provider_id = status[i]["provider_id"]
        except:
            provider_id = 'None'
        val.append((station_id, is_installed, is_renting, is_returning, last_reported, num_bikes_available,
                    num_docks_available, provider_id))
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
    "get_data_status_new",
    schedule_interval='0 6,8,10,12,14,17 * * *',
    start_date=datetime.datetime(2023, 4, 18))


get_data_status = PythonOperator(
   task_id="get_data_status",
   python_callable=get_data_status,
   dag=dag
)


create_table_status = PostgresOperator(
    task_id="create_table_status",
    dag=dag,
    postgres_conn_id='rds',
    sql='''
            CREATE TABLE IF NOT EXISTS statusdb (load timestamp, station_id varchar(255), is_installed text, is_renting text, is_returning text, last_reported int, num_bikes_available int, num_docks_available int, provider_id varchar(255));
        '''
)



# Configure Task Dependencies
create_table_status >> get_data_status