import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

source_path = "/opt/airflow/data/source/"
bronze_path = "/opt/airflow/data/bronze/"
silver_path = "/opt/airflow/data/silver/"

dag = DAG(
    dag_id="sparking_flow",
    default_args={
        "owner": "Group C - Steam Reviews",
        "start_date": airflow.utils.dates.days_ago(1)
    },
)

start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Jobs started"),
    dag=dag
)

health_check = SparkSubmitOperator(
    task_id="health_check",
    conn_id="spark-conn",
    application="jobs/python/healthcheck_wordcount.py",
    dag=dag
)

load_sample_job = SparkSubmitOperator(
    task_id="load_sample",
    conn_id="spark-conn",
    application="jobs/python/load_sample.py",
    application_args=["--source_path", source_path,
                      "--num_partition", "80",
                      "--bronze_path", bronze_path],
    dag=dag
)

load_games_to_bronze_job = SparkSubmitOperator(
    task_id="load_games_to_bronze",
    conn_id="spark-conn",
    application="jobs/python/load_games_to_bronze.py",
    application_args=["--source_path", source_path,
                      "--bronze_path", bronze_path],
    dag=dag
)

partition_data_job = SparkSubmitOperator(
    task_id="partition_data",
    conn_id="spark-conn",
    application="jobs/python/partition_data.py",
    application_args=["--bronze_path", bronze_path,
                      "--silver_path", silver_path],
    dag=dag
)

translate_reviews_job = SparkSubmitOperator(
    task_id="translate_reviews",
    conn_id="spark-conn",
    application="jobs/python/translate_reviews.py",
    application_args=["--model_name", "/opt/airflow/data/source/opus-mt-mul-en",
                      "--silver_path", silver_path],
    dag=dag
)


include_games_data_job = SparkSubmitOperator(
    task_id="include_games_data",
    conn_id="spark-conn",
    application="jobs/python/include_games_data.py",
    application_args=["--bronze_path", bronze_path,
                      "--silver_path", silver_path],
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> health_check >> load_sample_job >> load_games_to_bronze_job >> partition_data_job >> translate_reviews_job >> include_games_data_job >> end