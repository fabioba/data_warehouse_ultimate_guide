import logging

logger = logging.getLogger(__name__)

import pendulum

from airflow.decorators import dag, task_group
from airflow.operators.postgres_operator import PostgresOperator
from demo_dwh_sales.dags.resources.utils import get_query



@dag(
    start_date=pendulum.datetime(2025, 3, 1),
    catchup=False,
    default_args={
        "retries": 2,  # If a task fails, it will retry 2 times.
    },
    tags=["sales"],
)
def SALES_transaction():

    @task_group(group_id="extract")
    def extract():
        
        extract_sales = PostgresOperator(
            task_id = "extract_sales",
            sql = get_query('include/sql/extract/ingest_new_records.sql'),
            postgres_conn_id = 'postgres_conn'
        )

        extract_sales


    @task_group(group_id="load_transform")
    def load_transform():
            
        @task_group(group_id="dim")
        def dim():


            dim_product = PostgresOperator(
                task_id = "dim_product",
                sql = get_query('include/sql/load_transform/dim_product.sql'),
                postgres_conn_id = 'postgres_conn'
            )

            dim_customer = PostgresOperator(
                task_id = "dim_customer",
                sql = get_query('include/sql/load_transform/dim_customer.sql'),
                postgres_conn_id = 'postgres_conn'
            )

            dim_payment = PostgresOperator(
                task_id = "dim_payment",
                sql = get_query('include/sql/load_transform/dim_payment.sql'),
                postgres_conn_id = 'postgres_conn'
            )

            [
                dim_product,
                dim_customer,
                dim_payment
            ]


        @task_group(group_id="stg")
        def stg():
            
            truncate_stg_sales = PostgresOperator(
                task_id = "truncate_stg_sales",
                sql = get_query('include/sql/load_transform/truncate_stg_sales.sql'),
                postgres_conn_id = 'postgres_conn'
            )

            insert_stg_sales = PostgresOperator(
                task_id = "insert_stg_sales",
                sql = get_query('include/sql/load_transform/insert_stg_sales.sql'),
                postgres_conn_id = 'postgres_conn'
            )

            truncate_stg_sales >> insert_stg_sales

        @task_group(group_id="fct")
        def fct():

            delete_update_fct_sales = PostgresOperator(
                task_id = "delete_update_fct_sales",
                sql = get_query('include/sql/load_transform/delete_update_fct_sales.sql'),
                postgres_conn_id = 'postgres_conn'
            )

            insert_fct_sales = PostgresOperator(
                task_id = "insert_fct_sales",
                sql = get_query('include/sql/load_transform/insert_fct_sales.sql'),
                postgres_conn_id = 'postgres_conn'
            )

            delete_update_fct_sales >> insert_fct_sales

        dim() >> stg() >> fct()
    
    extract() >> load_transform()

SALES_transaction()