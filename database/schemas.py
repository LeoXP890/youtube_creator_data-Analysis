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

create_schema_bronze = '''CREATE SCHEMA IF NOT EXISTS bronze'''

create_schema_silver = '''CREATE SCHEMA SILVER IF NOT EXISTS silver'''

create_schema_gold = '''CREATE SCHEMA SILVER IF NOT EXISTS gold'''

cur.execute(create_schema_bronze)
cur.execute(create_schema_silver)
cur.execute(create_schema_gold)
conn.commit()
cursor.close()

