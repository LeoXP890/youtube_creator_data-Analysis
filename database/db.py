import psycopg2 as psy
import pandas as pd
import streamlit as st

ytGlobal = pd.read_csv("global_youtube_creator_data_large.csv")

df = pd.DataFrame(ytGlobal)

try:
    conn = psy.connect(
        database="global_youtube",
        user="postgres",
        password="postgre",
        host="localhost",
        port="5432"
    )
    print("Conectado no banco de dados global_youtube")
    
except:
    print("nao foi possivel conectar")
    
cur = conn.cursor()

create_schema_query = '''CREATE SCHEMA IF NOT EXISTS bronze'''

create_table_query = '''
    
    CREATE TABLE IF NOT EXISTS bronze.global_yt_statics_2023(
    rank INTEGER PRIMARY KEY NOT NULL,
    youtuber_channel VARCHAR(22) NOT NULL,
    subscribers INTEGER NOT NULL,
    views FLOAT NOT NULL,
    category VARCHAR(22),
    title VARCHAR(22) NOT NULL,
    uploads INTEGER NOT NULL,
    country VARCHAR(22),
    country_abbreviation VARCHAR(5) NOT NULL,
    channel_type VARCHAR(22) NOT NULL,
    rank_views FLOAT,
    country_rank FLOAT,
    channel_type_rank FLOAT,
    video_views_last_30_days FLOAT,
    lowest_monthly_earnings FLOAT,
    highest_monthly_earnings FLOAT,
    lowest_yearly_earnings FLOAT,
    highest_yearly_earnings FLOAT,
    subscribers_for_last_30_days FLOAT,
    created_year FLOAT,
    created_month VARCHAR(22),
    created_date FLOAT,
    education_enrollment FLOAT,
    population FLOAT,
    unemployment_rate FLOAT,
    urban_population FLOAT,
    latitude FLOAT,
    longitude FLOAT
    )'''
    
    
cur.execute(create_schema_query)
conn.commit()

cur.execute(create_table_query)
conn.commit()

print("Tabela criada com sucesso!!")

cursor = conn.cursor()

file_path = 'global_youtube_creator_data_large.csv'

with open(file_path, 'r', encoding='utf-8') as f:
    next(f)
    
    cursor.copy_expert(
        "COPY bronze.youtube_metrics(date_time, video_id, category, language, region, duration_sec, views, likes, comments, shares, sentiment_score, ads_enabled) FROM STDIN WITH DELIMITER ','",f
    )
    
conn.commit()
cursor.close()
conn.close()
