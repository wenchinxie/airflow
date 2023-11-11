def get_scrapy_crawl_command(folder: str, project: str, spider: str, args: str):
    command = f"""
    cd /home/wenchin/airflow/dags/scrapy_crawler/{folder}/{project} &&
    conda run -n airflow scrapy crawl {spider}  {args}
    """

    return command
