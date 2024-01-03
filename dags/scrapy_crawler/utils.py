def get_scrapy_crawl_command(folder: str, project: str, spider: str, args: str):
    command = f"""
    source /root/miniconda3/etc/profile.d/conda.sh &&
    cd /root/crawler/scrapy_crawler/{folder}/{project} &&
    conda activate airflow && scrapy crawl {spider}  {args}
    """

    return command
