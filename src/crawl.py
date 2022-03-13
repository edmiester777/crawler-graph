from elasticsearch_dsl import UpdateByQuery
import requests
from concurrent.futures import ProcessPoolExecutor
import os
import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from datetime import datetime
from pgmodels import PGCrawlerRecord

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
                # we want to find items queued to be crawled
                nextitems = PGCrawlerRecord.select() \
                    .limit(WORK_QUEUE_SIZE) \
                    .where(PGCrawlerRecord.crawled == False)

                if len(nextitems) == 0:
                    # we did not receive any records. assume this is our first time running and
                    # add the seed record. then continue loop
                    PGCrawlerRecord.create(
                        normalized_url=SEED_URL,
                        crawled=False,
                        crawled_date=datetime.now()
                    )
                    continue

                #processing this chunk
                futures = [self.executor.submit(process, record) for record in nextitems]
                
                # getting unique list of all results from chunk
                results = []
                for f in futures:
                    results += f.result()
                results = set(results)

                # removing any results that already exist in pg
                exists = [r.normalized_url for r in PGCrawlerRecord.filter(PGCrawlerRecord.normalized_url << results)]
                results -= set(exists)

                # adding any new urls to future crawl list
                newrecords = [PGCrawlerRecord(normalized_url=url, crawled=False, crawled_date=datetime.now()) for url in results]
                PGCrawlerRecord.bulk_create(newrecords)
                print(f'[{os.getpid()}] Created {len(newrecords)} future crawl records.')
                

        except (KeyboardInterrupt):
            print('Keyboard interrupt detected.')


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

def get_links_from_normalized_url(normalized_url: str) -> tuple[str, str, set[str]]:
    """
    Make request to url and parse HTML for other links.
    """
    response = requests.get('https://' + normalized_url)
    if response.status_code != 200:
        print(f'[{os.getpid()}] URL {normalized_url} responded with status code {response.status_code}')
        return None

    # early fail if HTML has not been returned
    if 'text/html' not in response.headers['content-type']:
        print(f'[{os.getpid()}] HTML content type not returned from {normalized_url}')
        return None

    # parse content
    soup = BeautifulSoup(response.text, "html.parser")

    # get title string
    titletag = soup.find('title')
    titlestr = "" if titletag is None else titletag.string

    # extract all links from document
    linktags = soup.find_all('a', attrs={'href': re.compile('https://')})

    # return a set of all links found
    return titlestr, response.text, set([normalizeurl(link.get('href')) for link in linktags])

def process(record:PGCrawlerRecord) -> set[str]:
    """
    Main processpool entrypoint. Will do the following:

    1. Fetch content from URL
    2. Extract all normalized urls from links in content
    3. Add records to elasticsearch if necessary
    4. Update corresponding this url record in elasticsearch with fetched content
    """
    response = get_links_from_normalized_url(record.normalized_url)

    # if we encountered some issues in get step, we will set success flag to false
    if response is None:
        print(f'[{os.getpid}] Failed to process {record.normalized_url}.')
        record.update(
            sucess=False,
            crawled_date=datetime.now()
        )

    # updating pg record with data
    title, text, normalized_urls = response
    record.update(
        success=True,
        title=title,
        body=text,
        crawled_date=datetime.now(),
        crawled=True
    )

    print(f'[{os.getpid()}] Processed {record.normalized_url}')

    return normalized_urls
        