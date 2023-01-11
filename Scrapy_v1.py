
from datetime import datetime, timedelta
from textwrap import dedent
import pendulum

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
with DAG(
    "Sci_crawler_test",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        #"email": ["wenchin.xie@gmail.com"],
        "email_on_failure": True,
        "email_on_retry": False,
        #"retries": 1,
        #"retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },

    description="A web crawler for raw material price",
    schedule=timedelta(days=7),
    start_date=pendulum.datetime(2022, 12, 16,22,23, tz="Asia/Taipei"),
    catchup=False,
    tags=["Scarpy",'raw material'],

) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id="Sci_cralwer",
        bash_command="""docker start financial_data_crawler-sci_crawler-1 -h localhost://2375"""
        )
    
    
    t2 = BashOperator(
        task_id="sleep",
        depends_on_past=False,
        bash_command="sleep 5",
        retries=3,
    )
    t1.doc_md = dedent(
        """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
    **Image Credit:** Randall Munroe, [XKCD](https://xkcd.com/license.html)
    """
    )

    dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG; OR
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # otherwise, type it like this
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
    {% endfor %}
    """
    )

    t3 = BashOperator(
        task_id="templated",
        depends_on_past=False,
        bash_command=templated_command,
    )

    t1 >> [t2, t3]