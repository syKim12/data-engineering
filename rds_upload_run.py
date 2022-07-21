from datetime import datetime
from airflow.models import DAG
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook


def upload_to_rds() -> None:
    #mysql setting
    mysql = MySqlHook('mysql_default')
    mysql.run(
        """
        LOAD DATA LOCAL INFILE '/home/ec2-user/airflow/data/s3_downloaded_HR.csv' 
        INTO TABLE performance 
        FIELDS TERMINATED BY ',' 
        LINES TERMINATED BY '\n';
        """
    )
with DAG(
    dag_id='rds_upload_run',
    schedule_interval='@daily',
    start_date=datetime(2022, 3, 1),
    catchup=False
) as dag:
   task_upload_to_rds = PythonOperator(
           task_id='upload_to_rds',
           python_callable=upload_to_rds
           )
    task_upload_to_rds
