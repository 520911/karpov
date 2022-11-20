# coding=utf-8

from datetime import datetime, timedelta
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20221020'
}


# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'd-rudakov',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 5),
}


# Интервал запуска DAG
schedule_interval = '0 11 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def report_hw():

    @task()
    def feed_extract():
        
        feed_yesterday = '''
                        select toDate(time) as date,
                               count(distinct user_id) as DAU,
                               countIf(action = 'like') as likes,
                               countIf(action = 'view') as views,
                               likes / views as CTR
                        from simulator_20221020.feed_actions
                        WHERE
                                toDate(time) = yesterday()
                        group by date
                        '''
        
        feed = pandahouse.read_clickhouse(query=feed_yesterday, connection=connection)
        return feed
    
    @task()
    def week_extract():

        feed_week = '''
                    select toDate(time) as date,
                           count(distinct user_id) as WAU,
                           countIf(action = 'like') as likes,
                           countIf(action = 'view') as views,
                           likes / views as CTR
                    from simulator_20221020.feed_actions
                    WHERE
                          date between today() -7 and today() - 1
                    group by date
                    '''
        
        week = pandahouse.read_clickhouse(query=feed_week, connection=connection)
        return week 
    

    @task
    def message_tg(feed):
        string = 'Date: ' + str(feed["date"][0])[:10] + '\n' + 'DAU: ' + str(feed["DAU"][0]) + '\n' + 'Views: ' +  str(feed["views"][0]) + \
        '\n' + 'Likes: ' + str(feed["likes"][0]) + '\n' + 'CTR: ' + str(round(feed["CTR"][0],7))
        return string
        
    @task
    def pick_tg(week):
        plt.figure(figsize=(10,10))
        plt.subplot(3, 1, 1)
        sns.lineplot(data=week.iloc[:,1:2])
        plt.title('WAU')
        plt.subplot(3, 1, 2)
        sns.lineplot(data=week.iloc[:,-1:])
        plt.title('CTR')
        plt.subplot(3, 1, 3)
        sns.lineplot(data=week.iloc[:,2:4])
        plt.title('Views Likes')
        report = io.BytesIO()
        plt.savefig(report)
        report.seek(0)
        report.name = 'report.png'
        plt.close()
        
        return report
    
    @task
    def send_tg(string, report):
        key = '5725722292:AAHqk8iarmtpktV8z_YinY8RKLYXhBLaeio'
        bot = telegram.Bot(token=key)
        chat_id = -817148946
        bot.sendMessage(chat_id=chat_id, text=string)
        bot.sendPhoto(chat_id=chat_id, photo=report)
        

    feed = feed_extract()
    week = week_extract()
    message = message_tg(feed)
    pick = pick_tg(week)
    send_tg(message, pick)
    
    
report_hw = report_hw()
