import requests
from concurrent.futures import ProcessPoolExecutor
import os
import logging
from urllib.parse import urlparse
from elasticsearch_dsl.response import Response

from esdocuments import ESCrawlDocument

# seed url is used for initial url crawl. it is needed because at genesis
# we don't have any stored urls to crawl.
SEED_URL = 'google.com'

# number of documents to pull from elasticsearch in each batch
WORK_QUEUE_SIZE = 500

class CrawlerDispatcher:
    """
    We need a dispatcher to dispatch work to each of our crawlers. We maintain a work queue
    and dispatch work to each of the crawlers so they may process each url async.

    Right now I use a ProcessPool executor. because of how this works, we pull a chunk, then
    send it to pool, process the chunk, and restart the process. In the future, I would like
    to implement it in a streamed fashion so that we have no downtime in pool waiting for
    chunks to be loaded.
    """
    def __init__(self, nproc:int=30):
        self.executor = ProcessPoolExecutor(max_workers=nproc)
        self.workqueue = []

    def begin(self):
        """
        Start the crawling process. This will continue until we receive a keyboard interrupt.
        """
        try:
            while True:
                # searching for crawl records. Will continue from last checkpoint.
                response:Response = ESCrawlDocument.search() \
                    .query('match', crawled=False) \
                    [0:WORK_QUEUE_SIZE].execute()

                # if there are no records to crawl, we assume we have not yet seeded
                # the database. We will seed and then jump to next iteration.
                if len(response) == 0:
                    logging.debug('No records to crawl. Seeding ES and restarting.')
                    ESCrawlDocument(normailzed_url=SEED_URL, crawled=False).save()
                    continue

                # setting workqueue to the normalized_url of results
                self.workqueue = [hit['normailzed_url'] for hit in response.hits]

                # submitting work to pool and waiting for finish
                self.executor.submit(self.workqueue).result()


        except (KeyboardInterrupt):
            logging.info('Keyboard interrupt detected.')


def normalizeurl(url: str) -> str:
    """
    Normalize a URL by removing trailing slash and scheme
    """
    parsed = urlparse(url)

    # host + path
    normalized = parsed.netloc + parsed.path

    # remove trailing slash
    normalized = normalized.rstrip('/')

    # adding query if one is there
    if len(parsed.query) > 0:
        normalized += '?' + parsed.query

    return normalized


def process(url:str):
    pass

