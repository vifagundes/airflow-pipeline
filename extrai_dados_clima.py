import os
from os.path import join
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

# intervalo de datas

data_inicio = datetime.today()
data_fim = data_inicio + timedelta(days=7)

#formatando datas
data_inicio = data_inicio.strftime('%Y-%m-%d')
data_fim = data_fim.strftime('%Y-%m-%d')


load_dotenv("config.env")
key = os.getenv("API_TOKEN")

city = 'Boston'

URL = join("https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/",
          f"{city}/{data_inicio}/{data_fim}?unitGroup=metric&include=days&key={key}&contentType=csv")

dados = pd.read_csv(URL)

file_path = f'/home/vifagundes/Documents/airflow-alura/airflow-pipeline/semana={data_inicio}/'
os.mkdir(file_path)

dados.to_csv(file_path + 'raw.csv')
dados[['datetime','tempmin','temp','tempmax']].to_csv(file_path + 'temperaturas.csv')
dados[['datetime','description','icon']].to_csv(file_path + 'condicoes.csv')