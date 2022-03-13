from datetime import datetime
from pony.orm import *

db = Database()
db.bind(
    provider='postgres', 
    host='host.docker.internal', 
    user='postgres',
    password='pass', 
    database='postgres'
)

class PGCrawlerRecord(db.Entity):
    normalized_url = PrimaryKey(str)
    crawled = Required(bool)
    success = Optional(bool)
    title = Optional(str)
    body = Optional(str)
    crawled_date = Optional(datetime)

db.generate_mapping(create_tables=True)
