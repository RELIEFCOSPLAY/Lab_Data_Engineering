# %%
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import requests
import pytz

# %%
def print_welcome():
    print('Welcome to Newsfeed Application')

# %%
def print_date():
    utc_time = datetime.today().date()
    IST = pytz.timezone('Asia/Kolkata') 
    print('Today is {}'.format(datetime.today().date()), 'Time:', datetime.now(IST))

# %%                            
def fetch_news():
    # BBC news api
    query_params = {
      "source": "bbc-news",
      "sortBy": "top",
      "apiKey": "5ebee8aee0024a59b8db0974bcc5751f"
    }
    main_url = " https://newsapi.org/v1/articles"
    # fetching data in json format
    res = requests.get(main_url, params=query_params)
    open_bbc_page = res.json()
    # getting all articles in a string article
    article = open_bbc_page["articles"]
 
    # empty list which will contain all trending news
    results = []
    for ar in article:
        results.append(ar["title"])
         
    for i in range(len(results)):
        # printing all trending news
        print(i + 1, results[i])

dag = DAG(
    'welcome_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 19 * * *',
    catchup=False
)

print_welcome_task = PythonOperator(
    task_id='print_welcome',
    python_callable=print_welcome,
    dag=dag
)

print_date_task = PythonOperator(
    task_id='print_date',
    python_callable=print_date,
    dag=dag
)

print_news_task = PythonOperator(
    task_id='print_news',
    python_callable=fetch_news,
    dag=dag
)
# Set the dependencies between the tasks
print_welcome_task >> print_date_task >> print_news_task