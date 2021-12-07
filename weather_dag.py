
import json
from datetime import datetime
from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import requests
import mysql.connector

def get_weather_report(city):
  url = 'http://api.openweathermap.org/data/2.5/weather?q={}&units=metric&appid=a7c52d647dea5fa06c03e1440e4c1bad'
  dataSet = requests.get(url.format(city)).json() 
  return dataSet

def save_data_into_db():
  db = mysql.connector.connect(host='54.174.141.226',user='root',passwd='password',db='mysql_testdb')
  cursor = db.cursor()
  date = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
  print(date)
  allCity = ['Bangkok','Tokyo']
  for city in allCity:
    weather = get_weather_report(city)
    data = weather
    temperature = data['main']['temp'],
    humidity =  data['main']['humidity'],
    description = data['weather'][0]['description'],
    icon = data['weather'][0]['icon']
    temperature = float('.'.join(str(ele) for ele in temperature))
    humidity = int(''.join(map(str, humidity)))
    cursor.execute('INSERT INTO weatherDB (city, temperature,humidity,description,icon,date)'
                    'VALUES("%s","%s","%s","%s","%s","%s")',(str(city), str(temperature),str(humidity),str(description),str(icon),date))
  db.commit() 
  print("Record inserted successfully into testcsv table")
  cursor.close()

default_args = {
    'owner': 'khunkook',
    'start_date': datetime(2021, 11, 27),
    'email': ['63606015@kmitl.ac.th'],
}

with DAG('weather',
         schedule_interval='@daily',
         default_args=default_args,
         description='weather report',
         catchup=False) as dag:

    t1 = PythonOperator(
        task_id='save_data_into_db',
        python_callable=save_data_into_db
    )


    t1
