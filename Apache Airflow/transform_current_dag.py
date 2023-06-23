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


def transform_current():
    current = datetime.date.today()
    logging.info(f'{current} today')

    # Fetch stat_df column names
    column_query = "SELECT column_name FROM information_schema.columns WHERE table_name = 'statusdb' ORDER BY ordinal_position"
    pg_hook = PostgresHook(postgres_conn_id="rds2", schema="sharedmobility")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(column_query)
    column_rows = cursor.fetchall()
    column_names = [row[0] for row in column_rows]
    logging.info(f'Stat DataFrame columns: {column_names}')


    data_query = f"SELECT * FROM statusdb WHERE load > '{current}'"
    cursor.execute(data_query)
    data_rows = cursor.fetchall()
    stat_df = pd.DataFrame(data_rows, columns = column_names)
    print(stat_df.columns)
    logging.info(f'Stat DataFrame rows:\n{stat_df}')


    # Fetch rentals_df column names
    column_query = "SELECT column_name FROM information_schema.columns WHERE table_name = 'calc_rentals' ORDER BY ordinal_position"
    cursor.execute(column_query)
    column_rows = cursor.fetchall()
    column_names = [row[0] for row in column_rows]
    logging.info(f'Rentals DataFrame columns: {column_names}')


    data_query = f"SELECT * FROM calc_rentals"
    cursor.execute(data_query)
    data_rows = cursor.fetchall()
    rentals_df = pd.DataFrame(data_rows, columns=column_names)
    logging.info(f'Stat DataFrame rows:\n{rentals_df}')

    # Fetch weather column names
    column_query = "SELECT column_name FROM information_schema.columns WHERE table_name = 'weather' ORDER BY ordinal_position"
    cursor.execute(column_query)
    column_rows = cursor.fetchall()
    column_names = [row[0] for row in column_rows]
    logging.info(f'Weather DataFrame columns: {column_names}')

    data_query = f"SELECT * FROM weather WHERE load > '{current}'"
    cursor.execute(data_query)
    data_rows = cursor.fetchall()
    weather = pd.DataFrame(data_rows, columns=column_names)
    logging.info(f'Weather DataFrame rows:\n{weather}')

    # filter for station_ids that are also in rentals_df
    #stat_filtered = stat_df[stat_df['station_id'].isin(rentals_df['station_id'])]
    common_station_ids = set(stat_df['station_id']).intersection(rentals_df['station_id'])
    stat_filtered = stat_df[stat_df['station_id'].isin(common_station_ids)]
    logging.info(f'stat_filtered')
    # drop columns
    stat = stat_filtered.drop(['is_installed', 'is_renting', 'is_returning', 'last_reported', 'num_docks_available'],
                              axis=1)
    logging.info(stat)
    # add column get date and time in stat and weather
    stat['sdate'] = stat['load'].dt.date
    stat['stime'] = stat['load'].dt.time

    # usage
    # create a new column called 'usage' with the difference between the current num_bikes_available and the previous one
    sorted_stat = stat.groupby('station_id').apply(lambda x: x.sort_values('load')).reset_index(drop=True)
    sorted_stat['usage'] = sorted_stat['num_bikes_available'].diff() * (-1)
    # Set negative values to 0
    sorted_stat['usage'] = sorted_stat['usage'].apply(lambda x: max(0, x))
    logging.info(f'sorted_stat')

    # drop columns
    stat = sorted_stat.drop(['num_bikes_available'], axis=1)

    pg_hook2 = PostgresHook(postgres_conn_id="rds_dwh", schema="sharedmobilitydwh")
    connection2 = pg_hook2.get_conn()
    cursor2 = connection2.cursor()
    pg_hook2.insert_rows(table='statusdb_cleaned', rows=stat.values.tolist(), target_fields=stat.columns.tolist())
    logging.info(f'statusdb_cleaned inserted')

    # drop columns
    weather = weather.drop(
        ['id_weather', 'temp_fells', 'temp_min', 'temp_max', 'pressure', 'sea_level', 'ground_level', 'visibility',
         'wind_degrees', 'wind_gust', 'datetime', 'country_code', 'sunrise', 'sunset', 'timezone'], axis=1)

    # add column get date and time in stat and weather

    weather['wdate'] = weather['load'].dt.date
    weather['wtime'] = weather['load'].dt.time

    # clean locations

    weather['location'] = weather['location'].astype(str)
    weather['location'] = weather['location'].str.split('(').str[0]
    weather['location'] = weather['location'].str.split('/').str[0]
    weather['location'] = weather['location'].replace(' ', '', regex=True)
    weather['location'] = weather['location'].replace('Canton of Basel-City', 'Basel', regex=True)
    weather['location'] = weather['location'].replace('CantonofBasel-City', 'Basel', regex=True)
    weather['location'] = weather['location'].replace('Bezirk ', '', regex=True)
    weather['location'] = weather['location'].replace('Amt ', '', regex=True)
    weather['location'] = weather['location'].replace(' District', '', regex=True)
    weather['location'] = weather['location'].replace('Les Avanchets', 'Genève', regex=True)
    weather['location'] = weather['location'].replace('LesAvanchets', 'Genève', regex=True)
    weather['location'] = weather['location'].replace('Les Pâquis', 'Genève', regex=True)
    weather['location'] = weather['location'].replace('LesPâquis', 'Genève', regex=True)
    weather['location'] = weather['location'].replace('Bronschhofen', 'Wil', regex=True)
    weather['location'] = weather['location'].replace('Châtelaine', 'Genève', regex=True)
    weather['location'] = weather['location'].replace('Lucerne', 'Luzern', regex=True)
    weather['location'] = weather['location'].replace('Giubiasco', 'Bellinzona', regex=True)
    weather['location'] = weather['location'].replace('Martigny-Ville', 'Martigny', regex=True)
    weather['location'] = weather['location'].replace('Gerliswil', 'Emmen', regex=True)
    weather['location'] = weather['location'].replace('Ramersberg', 'Sarnen', regex=True)
    weather['location'] = weather['location'].replace('Gerliswil', 'Emmen', regex=True)
    weather['location'] = weather['location'].replace('Neu-Felsberg', 'Felsberg', regex=True)
    weather['location'] = weather['location'].replace('Collombey', 'Collombey-Muraz',
                                                      regex=True)  # If run more than once, it will replace Collombey twice
    weather['location'] = weather['location'].replace('Breganzona', 'Lugano', regex=True)
    weather['location'] = weather['location'].replace('Flamatt', 'Wünnewil-Flamatt',
                                                      regex=True)  # If run more than once, it will replace Flamatt twice
    weather['location'] = weather['location'].replace('Camorino', 'Bellinzona', regex=True)
    weather['location'] = weather['location'].replace('Cointrin', 'Genève', regex=True)
    weather['location'] = weather['location'].replace('Bützberg', 'Thunstetten', regex=True)
    weather['location'] = weather['location'].replace('Pregny', 'Pregny-Chambésy',
                                                      regex=True)  # If run more than once, it will replace Pregny twice
    weather['location'] = weather['location'].replace('Canton of Schaffhausen', 'Schaffhausen', regex=True)
    weather['location'] = weather['location'].replace('CantonofSchaffhausen', 'Schaffhausen', regex=True)
    weather['location'] = weather['location'].replace('Goldswil', 'Ringgenberg', regex=True)
    weather['location'] = weather['location'].replace('SionDistrict', 'Sion', regex=True)
    weather['location'] = weather['location'].replace('BezirkSchaffhausen', 'Schaffhausen', regex=True)
    weather['location'] = weather['location'].replace('BezirkAarau', 'Aarau', regex=True)
    weather['location'] = weather['location'].replace('AmtLuzern', 'Luzern', regex=True)

    pg_hook2.insert_rows(table='weather_cleaned', rows=weather.values.tolist(), target_fields=weather.columns.tolist())

    logging.info(f'weather_cleaned inserted')

    # join
    merged = pd.merge(stat, rentals_df, on='station_id')
    merged['wlat_correct'] = merged['wlat'].round(4)

    merged['Closest'] = np.nan
    merged['MinTimeDiff'] = np.nan

    # iterate over each row in df1 and calculate the time differences with df2
    for i, row1 in merged.iterrows():
        try:
            selection = weather[(weather['latitude'] == row1['wlat_correct']) & (weather['wdate'] == row1['sdate'])]
            time_diffs = abs(selection['load'] - row1['load'])
            # find the index of the row in df2 with the smallest time difference
            min_index = time_diffs.idxmin()
            # update the minimum time difference column in df1 with the smallest time difference
            merged.at[i, 'MinTimeDiff'] = time_diffs[min_index]
            # update the Timestamp1 column in df1 with the Timestamp2 value of the row with the smallest time difference in df2
            merged.at[i, 'Closest'] = selection.at[min_index, 'load']
            print(i)
        except ValueError:
            pass

    merged['Closest'] = merged['Closest'].astype('datetime64[ns]')

    logging.info(f'joined')

    # Fetch rentals_df column names
    column_query = "SELECT column_name FROM information_schema.columns WHERE table_name = 'demographics' ORDER BY ordinal_position"
    cursor2.execute(column_query)
    column_rows = cursor2.fetchall()
    column_names = [row[0] for row in column_rows]
    logging.info(f'Demographics DataFrame columns: {column_names}')

    data_query = f"SELECT * FROM demographics"
    cursor2.execute(data_query)
    data_rows = cursor2.fetchall()
    demo = pd.DataFrame(data_rows, columns=column_names)
    logging.info(f'Demo DataFrame rows:\n{demo}')

    # Closest, wlat_correct
    pre_merged = pd.merge(merged, weather, left_on=['Closest', 'wlat_correct'], right_on=['load', 'latitude'],
                          how='left')
    final_merged = pd.merge(pre_merged, demo, left_on='location', right_on='municipality', how='left')
    final = final_merged.drop(
        ['load_x', 'station_id', 'coordinates_rentals', 'coordinates_weather', 'wlat', 'wlon', 'distance_km', 'key',
         'wlat_correct', 'Closest', 'MinTimeDiff', 'load_y', 'wdate', 'wtime', 'municipality'], axis=1)

    pg_hook2.insert_rows(table='all_merged', rows=final.values.tolist(), target_fields=final.columns.tolist())

    cursor.close()
    connection.close()
    cursor2.close()
    connection2.close()

    logging.info(f'done')

dag = DAG(
    "transform_current_date",
    schedule_interval='0 22 * * *',
    start_date=datetime.datetime(2023, 5, 24))



transform_current_data = PythonOperator(
   task_id="transform_current",
   python_callable=transform_current,
   dag=dag
)



# Configure Task Dependencies
transform_current_data