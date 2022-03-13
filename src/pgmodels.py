from peewee import *

connection = PostgresqlDatabase('postgres', # use postgres database for simplicity
    host='host.docker.internal',
    port=5432,
    user='postgres',
    password='pass'
)

class BaseModel(Model):
    class Meta:
        database=connection

class PGCrawlerRecord(BaseModel):
    normalized_url=TextField(primary_key=True, unique=True)
    crawled=BooleanField()
    title=TextField(True)
    body=TextField(True)
    success=BooleanField(True)
    crawled_date=DateTimeField()


def initialize():
    connection.connect(False)
    PGCrawlerRecord.create_table()