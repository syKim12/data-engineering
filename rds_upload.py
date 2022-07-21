from datetime import datetime
from airflow.models import DAG
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from sqlalchemy import create_engine
from airflow.providers.mysql.hooks.mysql import MySqlHook

import pandas as pd



def upload_to_rds() -> None:
    #mysql setting
    mysql = MySqlHook('mysql_default')
    cursor = mysql.cursor()

    #import csv
    df = pd.read_csv('/home/ec2-user/airflow/data/s3_downloaded_HR.csv', encoding='utf-8-sig')
    df.columns = ['age', 'attrition', 'business_travel', 'daily_rate',
    'department', 'distance_from_home', 'education', 'education_field',
    'employee_count', 'employee_number', 'environment_satisfaction', 
    'gender', 'hourly_rate', 'job_involvement', 'job_level', 'job_role',
    'job_satisfaction', 'marital_status', 'monthly_income', 'monthly_rate',
    'num_companies_worked', 'over_18', 'over_time', ' percent_salary_hike',
    'performance_rating', 'relationship_satisfaction', 'standard_hours',
    'stock_option_level', 'total_working_years', 'training_times_last_year',
    'work_life_balance', 'years_at_company', 'years_in_current_role', 
    'years_since_last_promotion', 'years_with_curr_manager']
    
    #save csv file in database
    engine = create_engine('mysql+pymysql://sooyeon:soomysql@sykim-mysql.cyzopmvjcvzw.us-east-1.rds.amazonaws.com:3306/employee?charset=utf8', encoding = "utf-8")
    conn = engine.connect()
    df.to_sql(name="performance", con=engine, if_exists='append', index=False)
    conn.close()
with DAG(
    dag_id='rds_upload',
    schedule_interval='@daily',
    start_date=datetime(2022, 3, 1),
    catchup=False
) as dag:
   task_upload_to_rds = PythonOperator(
           task_id='upload_to_rds',
           python_callable=upload_to_rds
           )

    task_upload_to_rds