import json
import os
import psycopg2
import pandas as pd
import requests
import numpy as np
import sqlalchemy
import csv

from sqlalchemy.sql import text
import math

#ENDPOINT = 'sharedmobility.cnls2nxrzngg.us-east-1.rds.amazonaws.com'
#DB_NAME = 'sharedmobility'
#USERNAME = 'nithu'
#PASSWORD = 'DWH_2023'

'''

try:
    print("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
    conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))

except psycopg2.Error as e:
    print("Error: Could not make connection to the Postgres database")
    print(e)

try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error: Could not get curser to the Database")
    print(e)

# Auto commit is very important
conn.set_session(autocommit=True)

### CREATE 2 TABLES

cur.execute("CREATE TABLE IF NOT EXISTS calc_rentals (station_id varchar(255), coordinates_rentals varchar(255), rlat float8, "
            "rlon float8, coordinates_weather varchar(255), wlat float8, wlon float8, distance_km float8, "
            "key int8);")

# Auto commit is very important
conn.set_session(autocommit=True)
'''
df = pd.read_excel('calc_rentals.xlsx')
#df['coordinates_rentals'] = df['coordinates_rentals'].astype(str)
#df['coordinates_weather'] = df['coordinates_weather'].astype(str)

#types = df.dtypes
#print(types)
# Insert the dataframe into the database
#df = df.drop(['coordinates_rentals', 'coordinates_weather'], axis=1)

df.to_sql('calc_rentals', con=engine, if_exists='replace', index=False)

# Close the database connection
conn.close()

'''


url = 'postgresql+psycopg2://nithu:DWH_2023@sharedmobility.cnls2nxrzngg.us-east-1.rds.amazonaws.com:5432/sharedmobility'
engine = sqlalchemy.create_engine(url)

sql = "SELECT * FROM infodb;"
with engine.connect() as conn:
    query = conn.execute(text(sql))
df = pd.DataFrame(query.fetchall())
pd.set_option('display.float_format', '{:.9f}'.format)


df_lat_lon = df[['station_id', 'lat', 'lon']]
print(df_lat_lon)

df_lat_lon.drop_duplicates(inplace=True)

# transform the DataFrame into a list of coordinates
coordinates = list(df_lat_lon[['lat', 'lon']].apply(lambda row: tuple(row), axis=1))

print(coordinates)


def calculate_distance(lat1, lon1, lat2, lon2):
    R = 6371  # radius of the Earth in kilometers

    # convert coordinates to radians
    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)

    # calculate the differences between the coordinates
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # apply the Haversine formula
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c

    return distance


# create an empty dataframe with the desired columns
df = pd.DataFrame(columns=['coordinates_1', 'coordinates_2', 'distance'])

# list of coordinates to compare
coordinates_1 = coordinates
coordinates_2 = [(46.21662, 6.10587), (46.17448, 6.11830), (46.21582, 6.14532), (46.33383, 6.18880),
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
# calculate the distance between each pair of coordinates
for coord1 in coordinates_1:
    for coord2 in coordinates_2:
        lat1, lon1 = coord1
        lat2, lon2 = coord2
        distance = calculate_distance(lat1, lon1, lat2, lon2)

        # create a row with the results and append it to the dataframe
        row = {'coordinates_1': coord1, 'coordinates_2': coord2, 'distance': distance}
        df = df.append(row, ignore_index=True)

# display the dataframe
print(df)

df_weather = pd.DataFrame({'coordinates': coordinates_2})
df_weather['key'] = range(1, 61)
df_weather =df_weather.rename(columns={'coordinates': 'coordinates_2'})
df_weather.head()


df = df.sort_values('distance')
df

# drop duplicates of coordinates, keeping only the first (which has the lowest distance)
df_cleaned = df.drop_duplicates(subset='coordinates_1')
df_cleaned

merged_df = pd.merge(df_cleaned, df_weather, on='coordinates_2', how='left')
merged_df

df_lat_lon['coordinates_1'] = list(zip(df_lat_lon['lat'], df_lat_lon['lon']))
df_lat_lon = df_lat_lon.drop(['lat', 'lon'], axis=1)
df_lat_lon

df_lat_lon['coordinates_1'] = list(zip(df_lat_lon['lat'], df_lat_lon['lon']))

df_rentals_final = pd.merge(df_lat_lon, merged_df, on='coordinates_1', how='left')
df_rentals_final = df_rentals_final.rename(columns={'coordinates_1':'coordinates_rentals', 'coordinates_2':'coordinates_weather'})
df_rentals_final

df_rentals_final['coordinates_rentals'] = df['my_tuples'].apply(lambda x: tuple(str(elem).replace(',', '.') for elem in x))
'''