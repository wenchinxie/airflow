import pendulum
from airflow import DAG


with DAG(
    "ParseFinancialStatement",
    schedule_interval="@once",
    start_date=pendulum.datetime(2023, 1, 12, 9, 0, tz="Asia/Taipei"),
    catchup=False,
    tags=["quarterly financial statement"],
) as dag:
    
    import re
    import os
    from airflow.operators.python import ExternalPythonOperator
    from airflow.configuration import conf



    def pdftoText(target_folder:str):
        for root,_,files in os.walk(target_folder):
            for file in files:
                file_path = os.path.join(root,file):
                
            

    
    def get_target_page_range(file_path):

    def call_poppler(filename:str,page_start:int,page_end):


    save_inds_comps_to_db = ExternalPythonOperator(
        task_id="save_industries_info",
        python=conf.get("core", "virtualenv"),
        python_callable=call_inds_scraper,
        op_args=[get_db_conn],
    )

    save_inds_comps_to_db
