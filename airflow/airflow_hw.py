# coding=utf-8

from datetime import datetime, timedelta
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

test_connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': '656e2b0c9c',
    'user': 'student-rw',
    'database': 'test'
}


# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'd-rudakov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 5),
}


# Интервал запуска DAG
schedule_interval = '@daily'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def airflow_hw_dag():

    @task()
    def feed_extract():
        
        feed = '''
        select user_id id,
               toDate(time) event_date,
               gender,
               age,
               os,
               sum(action = 'like') as likes,
               sum(action = 'view') as views
        from simulator_20221020.feed_actions
        WHERE
                toDate(time) = yesterday()
        group by user_id, gender, age, os, event_date
        '''
        
        feed = pandahouse.read_clickhouse(query=feed, connection=connection)
        return feed
    
    @task()
    def message_extract():

        messages = '''select id,
                             event_date,
                             gender,
                             age,
                             os,
                             messages_send,
                             users_send,
                             messages_received,
                             users_received
                             from
                        (select user_id id,
                                toDate(time) event_date,
                               gender,
                               age,
                               os,
                               count(reciever_id) messages_send,
                               count(distinct reciever_id) users_send
                        from simulator_20221020.message_actions
                        WHERE
                                toDate(time) = yesterday()
                        group by user_id, gender, age, os, event_date) sending
                        full outer join
                        (select reciever_id id,
                                toDate(time) event_date,
                               gender,
                               age,
                               os,
                               count(user_id) messages_received,
                               count(distinct user_id) users_received
                        from simulator_20221020.message_actions
                        WHERE
                                toDate(time) = yesterday()
                        group by reciever_id, gender, age, os, event_date) reciever
                        on sending.id = reciever.id
                '''
        
        messages = pandahouse.read_clickhouse(query=messages, connection=connection)
        return messages 
    
    @task
    def merge_feed_message(feed, messages):
        mr = feed.merge(messages, how='inner', suffixes=('_m', '_r'))
        return mr
    
    @task
    def groupby_gender(mr):
        gender = mr.groupby(['event_date', 'gender'])\
        [['likes', 'views', 'messages_received', 'messages_send', 'users_received', 'users_send']]\
        .sum() \
        .reset_index()
        
        gender.insert(1, 'dimension', 'gender')
        gender.rename(columns={'gender': 'dimension_value'}, inplace=True)
        
        return gender

    @task
    def groupby_os(mr):
        os= mr.groupby(['event_date', 'os'])\
        [['likes', 'views', 'messages_received', 'messages_send', 'users_received', 'users_send']]\
        .sum()\
        .reset_index()
        
        os.insert(1, 'dimension', 'os')
        os.rename(columns={'os': 'dimension_value'}, inplace=True)
        
        return os
    
    @task
    def groupby_age(mr):
        age = mr.groupby(['event_date', 'age'])\
        [['likes', 'views', 'messages_received', 'messages_send', 'users_received', 'users_send']]\
        .sum()\
        .reset_index()
        
        age.insert(1, 'dimension', 'age')
        age.rename(columns={'age': 'dimension_value'}, inplace=True)
        
        return age
    
    @task
    def concat_gender_os_age(gender, os, age):
        
        result = pd.concat([gender, os, age])
        
        return result
    
    @task
    def load(df):

        create_table = '''CREATE TABLE IF NOT EXISTS test.rudakov_dv
                        (
                        event_date Date,
                        dimension String,
                        dimension_value String,
                        likes Int64,
                        views Int64,
                        messages_received Int64,
                        messages_send Int64,
                        users_received Int64,
                        users_send Int64
                        ) 
                        ENGINE = MergeTree()
                        ORDER BY event_date
                        '''
        
        pandahouse.execute(query=create_table, connection=test_connection)
        pandahouse.to_clickhouse(df, table='rudakov_dv', index=False, connection=test_connection)
        
        
        
    df_feed = feed_extract()
    df_message = message_extract()
    df_merge = merge_feed_message(df_feed, df_message)
    df_groupby_gender = groupby_gender(df_merge)
    df_groupby_os = groupby_gender(df_merge)
    df_groupby_age = groupby_gender(df_merge)
    df_concat = concat_gender_os_age(df_groupby_gender, df_groupby_os, df_groupby_age)
    load(df_concat)
    
    
airflow_hw = airflow_hw_dag()
