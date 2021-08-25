from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryExecuteQueryOperator

BQ_CONN_ID = "google_cloud_default"
BQ_PROJECT = "fresh-delight-323115"
BQ_DATASET = "sendwave_project"

default_args = {
    "owner": "Kenny",
    "depends_on_past": True,
    "start_date": datetime(2021, 8, 1),
    "end_date": datetime(2021, 8, 31),
    "email": ["kogunyale01@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=10),
}

# Set Schedule: Run pipeline daily
schedule_interval = "@daily"


# Define DAG: Set ID and assign default args and schedule interval
dag = DAG(
    "sendwave_hackernews_project",
    default_args=default_args,
    schedule_interval=schedule_interval
)

# Task 1: check that the hackernews data has a stories table
t1 = BigQueryCheckOperator(
    task_id="check_hackernews_stories",
    sql='''
    #standardSQL
    SELECT
        id
    FROM 
        `bigquery-public-data.hacker_news.stories`
    ''',
    use_legacy_sql=False,
    gcp_conn_id=BQ_CONN_ID,
    dag=dag
)

# Execute the query to generate the necessary column as required
'''
1. Where was the article originally published ?
2. How many articles has this author published? 
3. How many days have past since the author first published a story?
4. What is their average story score? 
5. How many stories has this author published in the last 30 days?
6. Has this author ever published to this website before?
'''

t2 = BigQueryExecuteQueryOperator(
    task_id="write_result_to_sendwave_project",
    sql='''
    #standardSQL
    WITH
      step1 AS (
      SELECT
        id,
        author,
        SPLIT(url,'/')[SAFE_OFFSET(2)] AS url
      FROM
        `bigquery-public-data.hacker_news.stories`
      WHERE
        SPLIT(url,'/')[SAFE_OFFSET(2)] IS NOT NULL ),
      step2 AS (
      SELECT
        id, author,url, count(*) OVER(PARTITION BY author, url) as times_published
        from step1
    )
    SELECT
      hacker_news.author,
      time_ts,
      (COUNT(hacker_news.author) OVER(PARTITION BY hacker_news.author ORDER BY UNIX_DATE(CAST(time_ts AS DATE)) RANGE BETWEEN 30 PRECEDING
          AND CURRENT ROW)) AS authorStoryCountInLast30Days,
      (COUNT(hacker_news.author) OVER(PARTITION BY hacker_news.author ORDER BY CAST(time_ts AS DATE) ROWS BETWEEN UNBOUNDED PRECEDING
          AND CURRENT ROW)) AS authorPublishedStoryCount,
      ROUND((AVG(score) OVER(PARTITION BY hacker_news.author ORDER BY CAST(time_ts AS DATE) ROWS BETWEEN UNBOUNDED PRECEDING
            AND CURRENT ROW)), 2) AS AvgScore,
      TIMESTAMP_DIFF(time_ts, FIRST_VALUE(time_ts) OVER (PARTITION BY hacker_news.author ORDER BY CAST(time_ts AS DATE)), DAY) AS daysFromFirstPublished,
      step2.url,
      (CASE
          WHEN step2.times_published > 1 THEN TRUE
        ELSE
        FALSE
      END
        ) AS publishedOnSiteBefore
    FROM
      `bigquery-public-data.hacker_news.stories` AS hacker_news
    JOIN
      step2
    ON
      hacker_news.id = step2.id
    ''',
    destination_dataset_table='{0}.{1}.sendwave_result'.format(BQ_PROJECT, BQ_DATASET),
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    use_legacy_sql=False,
    gcp_conn_id=BQ_CONN_ID,
    dag=dag
)

# Setting dependencies
t2.set_upstream(t1)
