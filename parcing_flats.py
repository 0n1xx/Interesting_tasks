#Для датафрейма:
import pandas as pd
import numpy as np

#Для визуализации:
import seaborn as sns
import matplotlib.pyplot as plt

#Для парсинга:
import requests
from hyper.contrib import HTTP20Adapter
from bs4 import BeautifulSoup as bs
from time import sleep

#Для airflow и clickhouse:
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from clickhouse_driver import Client

#Для бота:
import telegram
import io

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2022, 9, 3),
}
schedule_interval = "00 22 * * 7"

client = Client(host='localhost')

my_token = "5711117341:AAEiU75eEX5RTbMEncnGh32JwsnhTSHtDrM"
bot = telegram.Bot(token=my_token)
chat_id = -1001786025908