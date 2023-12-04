from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.dates import days_ago


spark_master = "spark://54d83a44bf2e:7077"
spark_app_name = "pipeline"


args = {
    'owner': 'Airflow',
}

with DAG(
    dag_id='spark_submit_fake',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['test'],
) as dag:

    start = DummyOperator(task_id="start", dag=dag)

    spark_job = SparkSubmitOperator(
        task_id="spark_job",
        application='/opt/spark/apps/faketocsv.py',
        name="fake",
        conn_id="spark_default",
        verbose=1,
        conf={"spark.master":spark_master},
        dag=dag
    )

    # spark_create_es = SparkSubmitOperator(
    #     task_id="spark_create_es",
    #     application='/opt/spark/apps/createes.py',
    #     name="spark_es",
    #     conn_id="spark_default",
    #     verbose=1,
    #     conf={"spark.master":spark_master},
    #     dag=dag
    # )

    spark_input_es = SparkSubmitOperator(
        task_id="spark_input_es",
        application='/opt/spark/apps/testping.py',
        name="spark_es",
        conn_id="spark_default",
        verbose=1,
        conf={"spark.master":spark_master},
        dag=dag
    )




    end = DummyOperator(task_id="end", dag=dag)

    start >> spark_job >>spark_input_es>>end