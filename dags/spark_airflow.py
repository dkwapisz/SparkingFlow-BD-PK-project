import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id="sparking_flow",
    default_args={
        "owner": "Group C - Steam Reviews",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval="@daily"
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

make_partition_job = SparkSubmitOperator(
    task_id="make_partition",
    conn_id="spark-conn",
    application="jobs/python/spark_make_partition.py",
    application_args=["--input_path", "/opt/data/steam_reviews.csv",
                      "--num_partition", "10",
                      "--output_path", "/opt/data/bronze"],
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> health_check >> make_partition_job >> end