from asyncio import wait_for
from signal import SIGKILL
from socket import timeout
from threading import Timer
from elasticsearch_dsl import UpdateByQuery
import requests
from concurrent.futures import ProcessPoolExecutor, wait, ALL_COMPLETED
import os
import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from datetime import datetime
from pgmodels import PGCrawlerRecord
import neomodels
from neomodels import Record as NeoRecord, initialize as neoinit
import traceback
from pony.orm import *
from time import sleep

# seed url is used for initial url crawl. it is needed because at genesis
# we don't have any stored urls to crawl.
SEED_URLS = [
    'amazon.com',
    'google.com',
    'bing.com',
    'youtube.com',
    'facebook.com'
]

class CrawlerDispatcher:
    """
    We need a dispatcher to dispatch work to each of our crawlers. We maintain a work queue
    and dispatch work to each of the crawlers so they may process each url async.

    Right now I use a ProcessPool executor. because of how this works, we pull a chunk, then
    send it to pool, process the chunk, and restart the process. In the future, I would like
    to implement it in a streamed fashion so that we have no downtime in pool waiting for
    chunks to be loaded.
    """
    def __init__(self, nproc:int=16):
        self.nproc = nproc
        self.chunksize = nproc * 3
        self.executor = ProcessPoolExecutor(max_workers=nproc)
        self.workqueue = []

    def begin(self):
        """
        Start the crawling process. This will continue until we receive a keyboard interrupt.
        """
        while True:
            try:
                self.main_iter()
            except KeyboardInterrupt:
                print('Process interrupted.')
                break
            except Exception:
                # simply continue loop here.
                traceback.print_exc()
                sleep(0.4)
    @db_session
    def main_iter(self):
        """
        Main work function. This handles the bulk of the work for dispatching events
        and maintaining state.
        """
        # we want to find items queued to be crawled
        nextitems = select(item for item in PGCrawlerRecord if item.crawled == False)[:self.chunksize]

        if len(nextitems) == 0:
            # we did not receive any records. assume this is our first time running and
            # add the seed record. then continue loop
            nextitems = [PGCrawlerRecord(normalized_url=url, crawled=False) for url in SEED_URLS]
            commit()

        #processing this chunk
        futures = [self.executor.submit(process, item.normalized_url) for item in nextitems]
        results, failed = wait(futures, return_when=ALL_COMPLETED, timeout=25)

        # if we timeout any futures, we will kill
        if len(failed) > 0:
            print(f'[{os.getpid}] Timeout has been detected. Killing subprocesses for recreation later.')
            for _, proc in self.executor._processes.items():
                proc.kill()
            self.executor = ProcessPoolExecutor(max_workers=self.nproc)

        uresults = []
        for result in results:
            uresults += result.result()
        uresults = set(uresults)

        # adding any new urls to future crawl list
        # we need to catch unique constraint errors because pony orm doesn't support
        # "where in" clauses for us to grab a list of records.
        numnewrecs = 0
        for nurl in uresults:
            if not PGCrawlerRecord.exists(normalized_url=nurl):
                PGCrawlerRecord(normalized_url=nurl, crawled=False)
                numnewrecs += 1
        
        print(f'[{os.getpid()}] Created {numnewrecs} future crawl records.')


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
    try:
        response = requests.get('https://' + normalized_url, timeout=5)
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
        titlestr = "" if titletag is None else titletag.get_text()

        # extract all links from document
        linktags = soup.find_all('a', attrs={'href': re.compile('https://')})

        # return a set of all links found
        return titlestr, response.text, set([normalizeurl(link.get('href')) for link in linktags])
    except:
        traceback.print_exc()
    
    return None

def get_or_create_graph_node(url, title:str|None=None) -> NeoRecord:
    "Get a graph node if one exists, else create it."
    neorepo = neomodels.neorepo
    node:NeoRecord = neorepo.match(NeoRecord, url).first()
    if node is None:
        node = NeoRecord(
            url=url,
            title=title
        )
        neorepo.create(node)

    if node.title != title:
        node.title = title
        neorepo.save(node)
    return node

@db_session
def process(url:str) -> set[str]:
    """
    Main processpool entrypoint. Will do the following:

    1. Fetch content from URL
    2. Extract all normalized urls from links in content
    3. Add records to elasticsearch if necessary
    4. Update corresponding this url record in elasticsearch with fetched content

    Upon a 5 second timeout, this process will be killed.
    """
    try:
        record = PGCrawlerRecord.get_for_update(normalized_url=url)

        neoinit()
        neorepo = neomodels.neorepo
        response = get_links_from_normalized_url(record.normalized_url)

        # if we encountered some issues in get step, we will set success flag to false
        if response is None:
            print(f'[{os.getpid()}] Failed to process {record.normalized_url}.')
            record.crawled = True
            record.success = False
            record.crawled_date = datetime.now()
            commit()
            return set()

        # updating pg record with data
        title, text, normalized_urls = response
        record.success = True
        record.title = title
        record.body = text
        record.crawled_date = datetime.now()
        record.crawled = True
        commit()

        # adding links to graph
        gfrom = get_or_create_graph_node(record.normalized_url, title)
        to_save = [gfrom]
        for linkurl in normalized_urls:
            gto = get_or_create_graph_node(linkurl)
            gfrom.linked_to.add(gto)
            gto.linked_from.add(gfrom)
            to_save.append(gto)

        # saving additional links to graph
        neorepo.save(*to_save)

        print(f'[{os.getpid()}] Processed {record.normalized_url}')
        return normalized_urls
    except Exception as ex:
        print(f'[{os.getpid()}] {ex}')

    return set()   