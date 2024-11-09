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

health_check = SparkSubmitOperator(
    task_id="health_check",
    conn_id="spark-conn",
    application="jobs/python/healthcheck_wordcount.py",
    dag=dag
)

start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Jobs started"),
    dag=dag
)

make_partition_job = SparkSubmitOperator(
    task_id="make_partition",
    conn_id="spark-conn",
    application="jobs/python/spark_make_partition.py",
    dag=dag
)

# print_job = SparkSubmitOperator(
#     task_id="print_sample_job",
#     conn_id="spark-conn",
#     application="jobs/python/spark_print_sample.py",
#     dag=dag
# )

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> health_check >> make_partition_job >> end