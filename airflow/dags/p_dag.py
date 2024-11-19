from airflow.models import DAG
from airflow.utils.dates import days_ago
import airflow.operators.bash_operator import BashOperator

with DAG(
    'p_DAG',
    start_date=days_ago(1),
    scheduler_interval='@daily'
) as dag:
    
    tarefa_1 = EmptyOperator(task_id='tarefa_1')
    tarefa_2 = EmptyOperator(task_id='tarefa_2')
    tarefa_3 = EmptyOperator(task_id='tarefa_3')
    tarefa_4 = BashOperator(
        task_id = 'cria_pasta',
        bash_command = 'mkdir -p "/home/vifagundes/Documents/airflow-alura/airflow-pipeline/airflow/pasta"'
    )

    tarefa_1 >> [tarefa_2, tarefa_3]
    tarefa_3 >> tarefa_4

