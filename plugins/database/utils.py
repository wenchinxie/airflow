from sqlalchemy import MetaData, create_engine

from airflow.configuration import conf
from plugins.database import module

uri = conf.get("at_web", "db_uri")
engine = create_engine(uri)
metadata = MetaData(bind=engine)
metadata.reflect()


def create_table_if_not_exists(table_name):
    table = getattr(module, table_name)
    table.create(engine, checkfirst=True)
