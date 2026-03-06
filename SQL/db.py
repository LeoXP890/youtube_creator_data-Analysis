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
    
    CREATE TABLE IF NOT EXISTS bronze.youtube_metrics(
        date_time TIMESTAMP NOT NULL,
        video_id VARCHAR(12) NOT NULL,
        category VARCHAR(15) NOT NULL,
        language VARCHAR(20) NOT NULL,
        region VARCHAR(20) NOT NULL,
        duration_sec INTEGER NOT NULL,
        views INTEGER NOT NULL,
        likes INTEGER NOT NULL,
        comments INTEGER NOT NULL,
        shares INTEGER NOT NULL,
        sentiment_score DECIMAL(4,2) NOT NULL,
        ads_enabled BOOLEAN NOT NULL,
        PRIMARY KEY(video_id, date_time)
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
