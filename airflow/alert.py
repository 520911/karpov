import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse
from sklearn.cluster import KMeans, DBSCAN
from sklearn.preprocessing import StandardScaler


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
    'start_date': datetime(2022, 11, 11),
}


# Интервал запуска DAG
schedule_interval = 15 * * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def alert_hw():

    @task()
    def feed_extract():
        
        feed = '''SELECT toStartOfFifteenMinutes(time) as ts,
                         toDate(ts) as date,
                         formatDateTime(ts, '%R') as hm,
                         uniqExact(user_id) as users,
                         sum(action = 'like') likes,
                         sum(action = 'view') views
                  FROM simulator_20221020.feed_actions
                       WHERE ts >=  today() - 14 and ts < toStartOfFifteenMinutes(now())
                  GROUP BY ts, date, hm
                  ORDER BY ts
               '''
        
        feed = pandahouse.read_clickhouse(query=feed, connection=connection)
        return feed
    
    @task
    def db_scan(feed):
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(feed['users'].values.reshape(-1,1))
        model = DBSCAN(eps=0.2)
        cl = model.fit_predict(X_scaled)
        labels = model.labels_
        labels = np.array([1 if label == -1 else 0 for label in labels])
        res = []
        for id, i in enumerate(labels):
            if i == 1:
                alert = True
                res.append(id)
        if len(res) > 0:
            alert = True
        else:
            alert = False
        return labels, res, alert
    
    @task
    def message_tg(feed, res, alert):
         if alert:
            users_now = feed.iloc[res][-1:]['users'].values[0]
            current_ts = feed.iloc[res]['ts'].max()
            day_ago = current_ts - pd.DateOffset(days=1)
            users_day_ago = feed[feed['ts'] == day_ago]['users'].values[0]
            p = str(round((users_day_ago * 100 / users_now), 2))
            string = f"Метрика users в срезе {feed.iloc[res]['ts'].max()}. Текущее значение {users_day_ago}. Отклонение более {p}%"
            return string
         else:
            return 'No alerts'
 
    @task
    def pick_tg(feed, labels, res):
        sns.set(rc={'figure.figsize': (12, 12)}) 
        ax = sns.scatterplot(x=feed['hm'], y=feed['users'], c=labels)
        for ind, label in enumerate(ax.get_xticklabels()):
            if ind % 15 == 0:
                label.set_visible(True)
            else:
                label.set_visible(False)
        ax.set(xlabel='time')
        sns.lineplot(x=feed['hm'][res], y=feed['users'][res])
        ax.legend(['Outliers', 'Average'])
        plt.title('Alert')
        alert = io.BytesIO()
        plt.savefig(alert)
        alert.seek(0)
        alert.name = 'alert.png'
        plt.close()
        
        return alert
    
    @task
    def send_tg(string, pick, alert):
        key = '5725722292:AAHqk8iarmtpktV8z_YinY8RKLYXhBLaeio'
        bot = telegram.Bot(token=key)
        chat_id = 1005875700
        # chat_id = -817148946
        bot.sendMessage(chat_id=chat_id, text=string)
        bot.sendPhoto(chat_id=chat_id, photo=pick)
        

    feed = feed_extract()
    labels, res, alert = db_scan(feed)
    message = message_tg(feed, res, alert)
    pick = pick_tg(feed, labels, res)
    send_tg(message, pick, alert)
    
    
alert_hw = alert_hw()
