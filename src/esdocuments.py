"""
Collection of all elasticsearch document definitions.
"""
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl import Document, Text, Keyword, Boolean, Date

# Number of shards to allocate to each index. Note: for a large-scale operation we should
# consider increasing shards and doing a generated index scheme
NUMBER_OF_SHARDS = 2

class ESCrawlDocument(Document):
    """
    Main document responsible for maintaining records of all records that have been and need
    to be processed in the future.

    In the initial state, the onluy field that will be populated is "crawled:false" indicating
    that this normalized url needs to be crawled in the future. When it is crawled, it will
    update the record with all of the fields populated and "crawled:true".

    Note: by "normalized_url" we mean a URL without a scheme, and with trailing slashes removed.
          This will also include query params if any have been provided.
    """
    title: Text()
    body: Text()
    normalized_url: Keyword()
    crawled: Boolean()
    crawled_date:  Date()
    success: Boolean()

    class Index:
        name = 'crawl_records'
        settings = { 'number_of_shards': NUMBER_OF_SHARDS }



def initialize():
    "Initialize all indexes"
    # creating default es connection.
    connections.create_connection(hosts=['localhost'])

    # creating indexes
    ESCrawlDocument.init()