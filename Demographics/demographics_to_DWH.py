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

#!pip install boto3
# make --> pip install boto3
import boto3
import pandas as pd
import io


ENDPOINT = 'sharedmobilitydwh.cnls2nxrzngg.us-east-1.rds.amazonaws.com'
DB_NAME = 'sharedmobilitydwh'
USERNAME = 'nithu'
PASSWORD = 'DWH_2023'



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

cur.execute("CREATE TABLE IF NOT EXISTS demographics (municipality varchar(255), population int8, average_age float8);")

# Auto commit is very important
conn.set_session(autocommit=True)



#get data from S3 & transform

# Accessing the S3 buckets using boto3 client
s3_client =boto3.client('s3')
s3_bucket_name='dwldatasetage'
s3 = boto3.resource('s3',
                    aws_access_key_id='ASIA2C2HI7F63TDRK2GN',
                    aws_secret_access_key='siRd2V7tIUVAwOHbxnXu7oPVgk608YSTNu+YBF3T',
                    aws_session_token='FwoGZXIvYXdzELf//////////wEaDHDlgwNzxiGcPWOx4CK/AXwX+Ivo9pdapAk9ZwSGU2/DQt4b7oPjWyaHo2xadZceNMKwbROfNaUJEs9BIGRZ8zG8IWm0H4CHdWRInwmgRzjCdwa/OA1KkgedOFCu6b+9AkrjbBUzhBC9gSRdBKJd1Tm/45RBy0mwtepSjCI0ume9C/5w3AvqRTeLSrkDEfImML8ofR/EBCkFztuuCsO9I5w3HLBfInzioHEg8hx2giQAwasVGfB4slNRiuEZi4s/dhI2ttZcvp3D0d1+7TdDKL6L+aIGMi1SmdOptpteT+Mz6rdn2uwdoR0lxpJRc9V5K7qAhYgsYLutXRgNeaZo1RbI288=')

# Getting data files from the AWS S3 bucket as denoted above and printing the first 10 file names having prefix "2019/7/8"
my_bucket=s3.Bucket(s3_bucket_name)

for my_bucket_object in my_bucket.objects.all():
    response = my_bucket_object.get()

data = response['Body'].read().decode('utf-8')
df = pd.read_csv(io.StringIO(data), sep=';')

# Data Transformation
#first only keep values in column 'Zipcode' which are between 0001 and 6810
df['zipcode'] = df['zipcode'].astype(str)
df = df[(df['zipcode'] >= '0001') & (df['zipcode'] <= '6810')]

#only keep values with a length of 4. This gives you a list of all municipalities.
df = df[df['zipcode'].str.len() == 4]

#cut off the first 11 caracters
df['name'] = df['name'].str[11:]

#rename the column 'name' to 'municipality'
df = df.rename(columns={'name': 'municipality'})

# Add average age column
age_cols = [col for col in df.columns if col != 'zipcode' and col != 'municipality']
df['average_age'] = round((df[age_cols] * range(len(age_cols))).sum(axis=1) / df[age_cols].sum(axis=1),2)


#drop all other columns except zipcode name, country, gender, agegroups and total-age
#df = df.loc[:, ['zipcode', 'municipality', 'average_age']]


# make agegroups
df['0-17 years'] = df.loc[:, '0 years':'17 years'].sum(axis=1)
df['18-34 years'] = df.loc[:, '18 years':'34 years'].sum(axis=1)
df['35-50 years'] = df.loc[:, '35 years':'50 years'].sum(axis=1)
df['51-69 years'] = df.loc[:, '51 years':'69 years'].sum(axis=1)
df['70+ years'] = df.loc[:, '70 years':'100 years or older'].sum(axis=1)
df['population'] = df.loc[:, '0-17 years':'70+ years'].sum(axis=1)

# drop unneeded columns except name, country, gender, agegroups and total-age
df_demograph = df.loc[:, ['municipality', 'population', 'average_age']]

# Cleaning municipality column to match with demographics table
df_demograph['municipality'] = df_demograph['municipality'].str.split('(').str[0]
df_demograph['municipality'] = df_demograph['municipality'].replace(' ', '', regex=True)
# reset index
df_demograph = df_demograph.reset_index(drop=True)

df_demograph.info()

#load transformed demographics to DWH

url = 'postgresql+psycopg2://nithu:DWH_2023@sharedmobilitydwh.cnls2nxrzngg.us-east-1.rds.amazonaws.com:5432/sharedmobilitydwh'
engine = sqlalchemy.create_engine(url)


df_demograph.to_sql('demographics', con=engine, if_exists='replace', index=False)






