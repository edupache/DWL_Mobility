import datetime
import time
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


def get_data():
    pg_hook = PostgresHook(postgres_conn_id="rds", schema="sharedmobility")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    API_KEY = "ea28ae06a2d98799f37ebe29e85e6286"
    units = "metric"
    current_weather = []


    #coordinates = cursor.execute(
    #    "SELECT DISTINCT load, lat, lon FROM infodb WHERE lat BETWEEN 46.85 AND 47.52 AND lon BETWEEN 8.05 AND 8.66 ORDER BY load desc limit 1800")
    counter = 0
    #for coord in cursor.fetchall():
    coordinates = [(46.21662, 6.10587), (46.17448, 6.11830), (46.21582, 6.14532), (46.33383, 6.18880),
                   (46.42008, 6.26162), (46.50153, 6.40342), (46.52582, 6.60641), (46.42974, 6.91161),
                   (46.26384, 6.94850), (46.10285, 7.07649), (46.22986, 7.35286), (46.29275, 7.53190),
                   (46.61724, 7.05417), (46.79824, 7.14798), (46.89191, 7.31273), (46.94587, 7.44213),
                   (46.75538, 7.62352), (46.68987, 7.87100), (47.21045, 7.54093), (47.47479, 7.59471),
                   (47.54152, 7.59835), (47.48887, 7.73008), (47.21140, 7.45222), (47.22174, 7.76117),
                   (47.38251, 8.04421), (47.18255, 8.09340), (47.07459, 8.27662), (47.02510, 8.30308),
                   (46.96451, 8.35586), (46.89411, 8.24322), (46.89069, 8.63596), (47.18849, 8.47317),
                   (47.15779, 8.51558), (47.43371, 8.35498), (47.50540, 8.54949), (47.57664, 8.25703),
                   (47.41938, 8.50698), (47.45985, 8.58260), (47.38494, 8.51933), (47.28289, 8.57441),
                   (47.35328, 8.72416), (47.24849, 8.72382), (47.70044, 8.63665), (47.55703, 8.88147),
                   (47.46525, 9.03635), (47.44454, 9.37356), (47.47724, 9.48589), (47.46972, 9.01890),
                   (47.46789, 9.00770), (47.64518, 9.19132), (47.26389, 9.11548), (47.64633, 9.18024),
                   (47.47717, 9.48887), (47.40720, 9.60201), (47.15413, 9.50656), (46.85053, 9.47706),
                   (46.53313, 9.87534), (46.17456, 9.00291), (46.00737, 8.93821), (45.86698, 8.98041)]
    for j in range(len(coordinates)):
        #if counter == 59:
        #    time.sleep(60)
        #    counter = 0
        LAT = coordinates[j][0]
        LON = coordinates[j][1]
        url = f"https://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&&units={units}&appid={API_KEY}"
        response = requests.get(url)
        #counter += 1
        resp = json.loads(response.text)
        current_weather.append(resp)

    rows = len(current_weather)
    logging.info(f'{rows} extracted')

    sql = "INSERT INTO weather (load, longitude, latitude, id_weather, weather_main, weather_description, temp, temp_fells, temp_min, temp_max, " \
          "pressure, humidity, sea_level, ground_level, visibility, wind_speed, wind_degrees, wind_gust, datetime, " \
          "country_code, sunrise, sunset, timezone, location) VALUES (Current_timestamp, %s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s, " \
          "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = []
    counter = 0

    for i in range(rows):
        counter += 1
        longitude = current_weather[i]['coord']['lon']
        latitude = current_weather[i]['coord']['lat']
        id_weather = current_weather[i]['weather'][0]['id']
        weather_main = current_weather[i]['weather'][0]['main']
        weather_description = current_weather[i]['weather'][0]['description']
        temp = current_weather[i]['main']['temp']
        temp_fells = current_weather[i]['main']['feels_like']
        temp_min = current_weather[i]['main']['temp_min']
        temp_max = current_weather[i]['main']['temp_max']
        pressure = current_weather[i]['main']['pressure']
        humidity = current_weather[i]['main']['humidity']
        try:
            sea_level = current_weather[i]['main']['sea_level']
        except KeyError:
            sea_level = -1
        try:
            ground_level = current_weather[i]['main']['grnd_level']
        except KeyError:
            ground_level = -1,
        visibility = current_weather[i]['visibility']
        wind_speed = current_weather[i]['wind']['speed']
        wind_degrees = current_weather[i]['wind']['deg']
        try:
            wind_gust = current_weather[i]['wind']['gust']
        except KeyError:
            wind_gust = -1.0,
        # datetime= datetime.datetime.utcfromtimestamp(current_weather[i]['dt'] / 1000.0).isoformat()
        datetime = None
        country_code = current_weather[i]['sys']['country']
        # sunrise= datetime.datetime.utcfromtimestamp(current_weather[i]['sys']['sunrise'])
        sunrise = None
        # sunset= datetime.datetime.utcfromtimestamp(current_weather[i]['sys']['sunset'])
        sunset = None
        timezone = current_weather[i]['timezone']
        location = current_weather[i]['name']
        val.append((longitude, latitude, id_weather, weather_main, weather_description, temp, temp_fells, temp_min,
                    temp_max, pressure, humidity, sea_level, ground_level, visibility, wind_speed, wind_degrees,
                    wind_gust, datetime, country_code, sunrise, sunset, timezone, location))
        if counter == 20:
            cursor.executemany(sql, val)
            print('20 inserted')
            connection.commit()
            val = []
            counter = 0
    cursor.executemany(sql, val)
    cursor.close()
    connection.close()




dag = DAG(
    "weather_data",
    schedule_interval='0 6,8,10,12,14,17 * * *',
    start_date=datetime.datetime(2023, 4, 23))


get_data = PythonOperator(
   task_id="get_data",
   python_callable=get_data,
   dag=dag
)



create_table_weather = PostgresOperator(
    task_id="create_table_weather",
    dag=dag,
    postgres_conn_id='rds',
    sql='''
            CREATE TABLE IF NOT EXISTS weather (load timestamp, latitude float4, longitude float4, id_weather varchar(255), weather_main varchar(255), weather_description varchar(255), temp float(24), temp_fells float(24), temp_min float(24), temp_max float(24), pressure int, humidity int, sea_level int, ground_level int, visibility int, wind_speed float(24), wind_degrees int, wind_gust float(24), datetime timestamp, country_code varchar(255), sunrise timestamp, sunset timestamp, timezone int, location varchar(255));
        '''
)


# Configure Task Dependencies
create_table_weather >> get_data