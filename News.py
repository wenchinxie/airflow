
from datetime import datetime, timedelta
from textwrap import dedent
import pendulum


# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.decorators import task
# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import ExternalPythonOperator

AT_WEB_PATH='/home/wenchin/AT_WEB/bin/python3'

with DAG(
    
    "News_Feed",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization

    description="Only crawl the data on cnyes so far",
    #schedule="@daily",
    schedule_interval="0 21 * * *",
    start_date=pendulum.datetime(2023, 1, 12,9,0, tz="Asia/Taipei"),
    catchup=False,
    tags=["Scarpy",'News'],

) as dag:

    
    cynes_news=BashOperator(
        task_id='Scrapy_Cynes_News',
        bash_command="source /home/wenchin/AT_WEB/bin/activate;"\
        "cd /mnt/c/Users/s3309/AT/Financial_data_crawler/Scrapy/news/news;"\
        "scrapy crawl News"

    )

    cynes_news.doc_md = dedent(
        """\
    #### The whole first step begin with fetch news on cynes
    """
    )

    cynes_news

    '''
    
    #We have to change the path in windows to the ubuntu format
    #Instead I adapt Bashoperator to streamline
    AT_WEB_PATH=r'/home/wenchin/AT_WEB/bin/python3'
    def Scrapy_Cynes_News():

        
        import pkg_resources
        print([(p.project_name,p.version) for p in pkg_resources.working_set])

        from twisted.internet import reactor
        import scrapy
        from scrapy.crawler import CrawlerProcess
        from scrapy.utils.log import configure_logging
        from scrapy.utils.project import get_project_settings

        from Financial_data_crawler.Scrapy.news.news.spiders import News_spider

        runner=CrawlerProcess(get_project_settings())
        d = runner.crawl(News_spider)
        d.addBoth(lambda _: reactor.stop())
        reactor.run()
    
    cynes_news= ExternalPythonOperator(
        task_id='Scrapy_Cynes_News',
        python=AT_WEB_PATH,
        python_callable=Scrapy_Cynes_News,
    )
    
    cynes_news.doc_md = dedent(
        """\
    #### The whole first step begin with fetch news on cynes
    """
    )
    cynes_news
    '''
