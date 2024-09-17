import pendulum
from airflow.decorators import dag
from airflow.models.param import Param
from airflow.operators.bash import BashOperator


@dag(
    "scrapy__parse_cynes_news",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2023, 1, 12, 9, 0, tz="Asia/Taipei"),
    catchup=False,
    tags=["Scarpy", "News", "MongoDB"],
    params={
        "start_date": Param(
            pendulum.yesterday().to_date_string(),
            type="string",
            format="date",
        ),
        "end_date": Param(
            pendulum.today().to_date_string(),
            type="string",
            format="date",
        ),
    },
)
def parse_cynes_news():
    parse_news = BashOperator(
        task_id="parse_cynes",
        bash_command="scripts/scrape_cynews.sh",
        params={
            "start": "{{ params.start_date }}",
            "end": "{{ params.end_date }}",
        },
    )
    parse_news


to_parse = parse_cynes_news()
