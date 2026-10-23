
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="orders_etl_pipeline",
    description="Transform orders dan order_items menggunakan PySpark",
    start_date=datetime(2026, 10, 23),
    schedule=None,
    catchup=False,
    tags=["pyspark", "etl"],
) as dag:

    transform_orders = BashOperator(
        task_id="transform_orders",
        bash_command="""
        spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.hadoop.fs.permissions.umask-mode=007 \
        /spark/scripts/transform_orders.py
        """,
    )

    transform_orders
