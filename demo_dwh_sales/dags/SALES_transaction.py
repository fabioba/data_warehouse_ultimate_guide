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
            sql = get_query('include/sql/extract/extract_sales.sql'),
            postgres_conn_id = 'postgres_conn'
        )

    
        upload_cfg_flow_manager_extract = PostgresOperator(
            task_id = "upload_cfg_flow_manager_extract",
            sql = get_query('include/sql/cfg_flow_manager/upload_cfg_flow_manager.sql'),
            postgres_conn_id = 'postgres_conn',
            parameters={
                'PROCESS' : 'EXTRACT'
            }
        )

        extract_sales >> upload_cfg_flow_manager_extract

    @task_group(group_id="transform")
    def transform():

        
        truncate_transform_sales = PostgresOperator(
            task_id = "truncate_transform_sales",
            sql = get_query('include/sql/transform/truncate_transform_sales.sql'),
            postgres_conn_id = 'postgres_conn'
        )

        transform_sales = PostgresOperator(
            task_id = "transform_sales",
            sql = get_query('include/sql/transform/transform_sales.sql'),
            postgres_conn_id = 'postgres_conn'
        )

        upload_cfg_flow_manager_transform = PostgresOperator(
            task_id = "upload_cfg_flow_manager_transform",
            sql = get_query('include/sql/cfg_flow_manager/upload_cfg_flow_manager.sql'),
            postgres_conn_id = 'postgres_conn',
            parameters={
                'PROCESS' : 'TRANSFORM'
            }
        )

        truncate_transform_sales >> transform_sales >> upload_cfg_flow_manager_transform

    @task_group(group_id="load_transform")
    def load_transform():
            


        @task_group(group_id="load")
        def load():
            
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

        @task_group(group_id="fct")
        def fct():


            @task_group(group_id="fct_transactions_current_run")
            def fct_transactions_current_run():

                truncate_fct_transactions_current_run = PostgresOperator(
                    task_id = "truncate_fct_transactions_current_run",
                    sql = get_query('include/sql/load_transform/fct/fct_transactions_current_run/truncate_fct_current_run.sql'),
                    postgres_conn_id = 'postgres_conn'
                )

                insert_fct_transactions_current_run = PostgresOperator(
                    task_id = "insert_fct_transactions_current_run",
                    sql = get_query('include/sql/load_transform/fct/fct_transactions_current_run/insert_fct_current_run.sql'),
                    postgres_conn_id = 'postgres_conn'
                )

                truncate_fct_transactions_current_run >> insert_fct_transactions_current_run

            @task_group(group_id="fct_transactions")
            def fct_transactions():

                delete_update_fct_transactions = PostgresOperator(
                    task_id = "delete_update_fct_transactions",
                    sql = get_query('include/sql/load_transform/fct/fct_transactions/delete_update_fct_transactions.sql'),
                    postgres_conn_id = 'postgres_conn'
                )

                insert_fct_transactions = PostgresOperator(
                    task_id = "insert_fct_transactions",
                    sql = get_query('include/sql/load_transform/fct/fct_transactions/insert_fct_transactions.sql'),
                    postgres_conn_id = 'postgres_conn'
                )

                delete_update_fct_transactions >> insert_fct_transactions

            fct_transactions_current_run() >> fct_transactions()


        upload_cfg_flow_manager_load_transform = PostgresOperator(
            task_id = "upload_cfg_flow_manager_load_transform",
            sql = get_query('include/sql/cfg_flow_manager/upload_cfg_flow_manager.sql'),
            postgres_conn_id = 'postgres_conn',
            parameters={
                'PROCESS' : 'LOAD_TRANSFORM'
            }
        )

        load() >> dim() >> fct() >> upload_cfg_flow_manager_load_transform



    
    extract() >> transform() >> load_transform()

SALES_transaction()