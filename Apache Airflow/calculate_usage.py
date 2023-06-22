#import datetime
#import time
#import logging
#import json
import os
import psycopg2
import pandas as pd
#import requests
#import numpy as np

#from airflow import DAG
#from airflow.operators.python_operator import PythonOperator
#from airflow.operators.postgres_operator import PostgresOperator
#from airflow.hooks.postgres_hook import PostgresHook
#from airflow.timetables.trigger import CronTriggerTimetable


def usage_statusdb():
    # Database credentials
    ENDPOINT = 'sharedmobility.cnls2nxrzngg.us-east-1.rds.amazonaws.com'   #Nithu's shared point
    DB_NAME = 'sharedmobility'
    USERNAME = 'nithu'
    PASSWORD = 'DWH_2023'


    # Connect to the database
    try:
        print("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))

    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)


    # Get cursor to the database    
    try:    
        cur = conn.cursor()

    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)

    # Auto commit to the database
    conn.set_session(autocommit=True)

    # Fetch table from database
    try:
        data = pd.read_sql('SELECT * FROM statusdb', conn)    
        #cur.execute("SELECT * FROM statusdb")
    except psycopg2.Error as e:
        print("Error: Could not get data from the database")
        print(e)

    # Close the connection
    conn.close()
    cur.close()

    # create a new column called 'usage' with the difference between the current num_bikes_available and the previous one
    data['usage'] = data.groupby(['station_id'])['num_bikes_available'].diff()*(-1)
    columns_list = ['load','station_id', 'num_bikes_available', 'usage']
    # Set negative values to 0
    data['usage'] = data['usage'].apply(lambda x: max(0, x))
    usage = data[columns_list]

    # Check one single station_id
    #usage[usage['station_id'] == 'publibike:449']


    return usage



