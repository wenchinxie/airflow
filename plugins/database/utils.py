from database import models
from sqlalchemy import MetaData, create_engine

from airflow.configuration import conf

uri = conf.get("at_web", "db_uri")
engine = create_engine(uri)
metadata = MetaData(bind=engine)
metadata.reflect()


def create_table_if_not_exists(table_name):
    table = getattr(models, table_name)
    table.create(engine, checkfirst=True)
