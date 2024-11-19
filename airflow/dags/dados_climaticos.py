from airflow import DAG
import pendulum
import airflow.operators.bash_operator import BashOperator
import airflow.operatos.python_operator import PythonOperator
import os
from os.path import join
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import airflow.macros import ds_add

with DAG(
    'dados_climaticos',
    star_date=pendulum.datetime(2024,11,19, tz="UTC"),
    schedule_interval='0 0 * * 1'
) as dag:
    tarefa_1 = BashOperator(
        task_id = 'cria_pasta',
        bash_command = 'mkdir -p "/home/vifagundes/Documents/airflow-alura/airflow-pipeline/airflow/pasta"'
    )

    def extrai_dados(data_interval_end):
        load_dotenv("config.env")
        key = os.getenv("API_TOKEN")

        city = 'Boston'

        URL = join("https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/",
                f"{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={key}&contentType=csv")

        dados = pd.read_csv(URL)

        file_path = f'/home/vifagundes/Documents/airflow-alura/airflow-pipeline/airflow/semana={data_interval_end}/'

        dados.to_csv(file_path + 'raw.csv')
        dados[['datetime','tempmin','temp','tempmax']].to_csv(file_path + 'temperaturas.csv')
        dados[['datetime','description','icon']].to_csv(file_path + 'condicoes.csv')

    tarefa_2 = PythonOperator(
        task_id='extrai_dados',
        python_callable = extrai_dados,
        op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )

    tarefa_1 >> tarefa_2