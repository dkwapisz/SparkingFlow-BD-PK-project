import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

source_path = "/opt/airflow/data/source/"
bronze_path = "/opt/airflow/data/bronze/"
silver_path = "/opt/airflow/data/silver/"
gold_path = "/opt/airflow/data/gold/"


def create_dag_own(dag_id, job_type, translate: bool, no_parts="80"):
    dag = DAG(
        dag_id=dag_id,
        default_args={
            "owner": "Group C - Steam Reviews",
            "start_date": airflow.utils.dates.days_ago(1),
        },
    )

    start = PythonOperator(
        task_id="start", python_callable=lambda: print("Jobs started"), dag=dag
    )

    health_check = SparkSubmitOperator(
        task_id="health_check",
        conn_id="spark-conn",
        application="jobs/python/healthcheck_wordcount.py",
        dag=dag,
    )

    if job_type == "sample":
        load_sample_job = SparkSubmitOperator(
            task_id="load_sample",
            conn_id="spark-conn",
            application="jobs/python/load_sample.py",
            application_args=[
                "--source_path",
                source_path,
                "--num_partition",
                no_parts,
                "--bronze_path",
                bronze_path,
            ],
            dag=dag,
        )

    elif job_type == "full":
        load_full_job = SparkSubmitOperator(
            task_id="load_full",
            conn_id="spark-conn",
            application="jobs/python/load_full.py",
            application_args=[
                "--source_path",
                source_path,
                "--num_partition",
                "80",
                "--bronze_path",
                bronze_path,
            ],
            dag=dag,
        )

    if job_type in ["sample", "full"]:
        load_games_to_bronze_job = SparkSubmitOperator(
            task_id="load_games_to_bronze",
            conn_id="spark-conn",
            application="jobs/python/load_games_to_bronze.py",
            application_args=[
                "--source_path",
                source_path,
                "--bronze_path",
                bronze_path,
            ],
            dag=dag,
        )

    partition_data_job = SparkSubmitOperator(
        task_id="partition_data",
        conn_id="spark-conn",
        application="jobs/python/partition_data.py",
        application_args=["--bronze_path", bronze_path, "--silver_path", silver_path],
        dag=dag,
    )

    if translate:
        translate_reviews_job = SparkSubmitOperator(
            task_id="translate_reviews",
            conn_id="spark-conn",
            application="jobs/python/translate_reviews.py",
            application_args=[
                "--model_name",
                "/opt/airflow/data/source/opus-mt-mul-en",
                "--silver_path",
                silver_path,
            ],
            dag=dag,
        )

    include_games_data_job = SparkSubmitOperator(
        task_id="include_games_data",
        conn_id="spark-conn",
        application="jobs/python/include_games_data.py",
        application_args=["--bronze_path", bronze_path, "--silver_path", silver_path],
        dag=dag,
    )

    save_to_db_job = SparkSubmitOperator(
        task_id="save_to_db",
        conn_id="spark-conn",
        application="jobs/python/save_to_db.py",
        application_args=["--silver_path", silver_path],
        dag=dag,
        jars="/opt/airflow/jars/postgresql-42.2.29.jre7.jar",
    )

    analytics_best_games = SparkSubmitOperator(
        task_id="analyze_best_games",
        conn_id="spark-conn",
        application="jobs/python/analyze_best_games.py",
        application_args=["--gold_path", gold_path],
        dag=dag,
        jars="/opt/airflow/jars/postgresql-42.2.29.jre7.jar",
    )

    analytics_most_reviewed_game = SparkSubmitOperator(
        task_id="analyze_most_reviewed_game",
        conn_id="spark-conn",
        application="jobs/python/analyze_most_reviewed_game.py",
        application_args=["--gold_path", gold_path],
        dag=dag,
        jars="/opt/airflow/jars/postgresql-42.2.29.jre7.jar",
    )

    analytics_most_reviews_by_user = SparkSubmitOperator(
        task_id="analyze_most_reviews_by_user",
        conn_id="spark-conn",
        application="jobs/python/analyze_most_reviews_by_user.py",
        application_args=["--gold_path", gold_path],
        dag=dag,
        jars="/opt/airflow/jars/postgresql-42.2.29.jre7.jar",
    )

    analytics_top_games_casual = SparkSubmitOperator(
        task_id="analyze_top_games_casual",
        conn_id="spark-conn",
        application="jobs/python/analyze_top_games_casual.py",
        application_args=["--gold_path", gold_path],
        dag=dag,
        jars="/opt/airflow/jars/postgresql-42.2.29.jre7.jar",
    )

    analytics_top_games_non_casual = SparkSubmitOperator(
        task_id="analyze_top_games_non_casual",
        conn_id="spark-conn",
        application="jobs/python/analyze_top_games_non_casual.py",
        application_args=["--gold_path", gold_path],
        dag=dag,
        jars="/opt/airflow/jars/postgresql-42.2.29.jre7.jar",
    )

    analytics_top_games_publishers = SparkSubmitOperator(
        task_id="analyze_top_games_publishers",
        conn_id="spark-conn",
        application="jobs/python/analyze_top_games_publishers.py",
        application_args=["--gold_path", gold_path],
        dag=dag,
        jars="/opt/airflow/jars/postgresql-42.2.29.jre7.jar",
    )

    analytics_top_helpful_reviews = SparkSubmitOperator(
        task_id="analyze_top_helpful_reviews",
        conn_id="spark-conn",
        application="jobs/python/analyze_top_helpful_reviews.py",
        application_args=["--gold_path", gold_path],
        dag=dag,
        jars="/opt/airflow/jars/postgresql-42.2.29.jre7.jar",
    )

    end = PythonOperator(
        task_id="end",
        python_callable=lambda: print("Jobs completed successfully"),
        dag=dag,
    )

    if job_type == "sample":
        start >> health_check >> [load_sample_job, load_games_to_bronze_job] >> partition_data_job

    elif job_type == "full":
        start >> health_check >> [load_full_job, load_games_to_bronze_job] >> partition_data_job

    else:
        start >> health_check >> partition_data_job

    if translate:
        partition_data_job >> translate_reviews_job >> include_games_data_job
    else:
        partition_data_job >> include_games_data_job

    include_games_data_job >> save_to_db_job >> [analytics_best_games,
                                                 analytics_top_games_casual, analytics_top_games_publishers,
                                                 analytics_top_games_non_casual, analytics_most_reviewed_game,
                                                 analytics_most_reviews_by_user, analytics_top_helpful_reviews] >> end

    return dag


def create_analytics_dag_own(dag_id):

    dag = DAG(
        dag_id=dag_id,
        default_args={
            "owner": "Group C - Steam Reviews",
            "start_date": airflow.utils.dates.days_ago(1),
        },
    )

    start = PythonOperator(
        task_id="start", python_callable=lambda: print("Jobs started"), dag=dag
    )

    analytics_job = SparkSubmitOperator(
        task_id="analytics",
        conn_id="spark-conn",
        application="jobs/python/analytics.py",
        application_args=["--gold_path", gold_path],
        dag=dag,
        jars="/opt/airflow/jars/postgresql-42.2.29.jre7.jar",
    )

    end = PythonOperator(
        task_id="end",
        python_callable=lambda: print("Jobs completed successfully"),
        dag=dag,
    )
    start >> analytics_job >> end

    return dag


sample_dag = create_dag_own("sample_dag", job_type="sample", translate=False)

full_dag = create_dag_own("full_dag", job_type="full", translate=False)

test_dag = create_dag_own("test_dag", job_type=None, translate=False)

translate_dag = create_dag_own(
    "translate_dag", job_type="full", translate=True, no_parts="512"
)

translate_only_dag = create_dag_own(
    "translate_only_dag", job_type=None, translate=True, no_parts="512"
)

analytics_dag = create_analytics_dag_own("analytics_dag")


globals()["sample_dag"] = sample_dag
globals()["full_dag"] = full_dag
globals()["test_dag"] = test_dag
globals()["translate_dag"] = translate_dag
globals()["analytics_dag"] = analytics_dag
globals()["translate_only_dag"] = translate_only_dag
