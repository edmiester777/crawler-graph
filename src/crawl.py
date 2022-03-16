from itertools import islice
import os
import re
import traceback
from asyncio import Future, wait_for
from concurrent.futures import ALL_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, wait
from datetime import datetime
from time import sleep
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from pony.orm import *
from py2neo.bulk import merge_nodes, merge_relationships

import neomodels
from pgmodels import PGCrawlerRecord

# seed url is used for initial url crawl. it is needed because at genesis
# we don't have any stored urls to crawl.
SEED_URLS = [
    'amazon.com',
    'google.com',
    'bing.com',
    'youtube.com',
    'facebook.com'
]

class CrawlResult:
    "Structure for a result from a completed crawl."
    def __init__(
        self, 
        url:str,
        crawled_time:datetime,
        success:bool,
        title:str|None=None,
        body:str|None=None,
        links:set[str]|None=None
    ) -> None:
        self.url = url
        self.crawled_time = crawled_time
        self.success = success
        self.title = title
        self.body = body
        self.links = links if links is not None else set()
        

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
        self.chunksize = nproc * 7
        self.executor = ProcessPoolExecutor(max_workers=nproc)

        # use threadpool because multiprocessing is not supported in neo4j driver
        self.write_executor = ThreadPoolExecutor(max_workers=nproc)
        self.write_futures = []

        self.workqueue = []

    def begin(self):
        """
        Start the crawling process. This will continue until we receive a keyboard interrupt.
        """
        neomodels.initialize()
        while True:
            try:
                self.write_futures += self.main_iter()
            except KeyboardInterrupt:
                print('Process interrupted.')
                break
            except Exception:
                # simply continue loop here.
                traceback.print_exc()
                sleep(0.4)

    def main_iter(self) -> list[Future]:
        """
        Main work function. This handles the bulk of the work for dispatching events
        and maintaining state.

        :return Future for write process
        """
        nextitems = self.get_next_records()

        #processing this chunk
        futures = [self.executor.submit(process_crawl, item) for item in nextitems]
        results, failed = wait(futures, return_when=ALL_COMPLETED, timeout=60)

        # if we timeout any futures, we will kill
        if len(failed) > 0:
            print(f'[{os.getpid}] Timeout has been detected. Killing subprocesses for recreation later.')
            for _, proc in self.executor._processes.items():
                proc.kill()
            self.executor = ProcessPoolExecutor(max_workers=self.nproc)

        # unwrapping results
        results = [result.result() for result in results]

        # if there are ongoing write operations, wait for them to complete
        wait(self.write_futures, return_when=ALL_COMPLETED)

        # Processing write operations asynchronously
        return [*[self.write_executor.submit(process_write_neo4j, result, self) for result in results],
            self.write_executor.submit(process_write_postgres, results)]
    
    @db_session
    def get_next_records(self) -> set[str]:
        """
        Get next URLs to be crawled. If none are available, we will seed and return
        the seeds.
        """

        # we want to find items queued to be crawled
        nextitems = select(item for item in PGCrawlerRecord if item.crawled == False)[:self.chunksize]

        if len(nextitems) == 0:
            # we did not receive any records. assume this is our first time running and
            # add the seed record. then continue loop
            nextitems = [PGCrawlerRecord(normalized_url=url, crawled=False) for url in SEED_URLS]
            commit()

        return set([i.normalized_url for i in nextitems])


def normalizeurl(url: str) -> str:
    """
    Normalize a URL by removing trailing slash and scheme
    """
    parsed = urlparse(url.strip())

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
    except Exception as ex:
        print(f'[{os.getpid()} Encountered issue while downloading {normalized_url}: {ex}')
    
    return None

def process_write_neo4j(result:CrawlResult, dispatcher: CrawlerDispatcher):
    """
    Async writer process. This is done so we may continue to crawl while we perform
    our intense neo4j write operations.

    In this process, we will
    1. Update existing neo4j records with data from crawl results
    2. Link together all crawl results with their corresponding links
    3. Bulk write modified data

    :param results list of results from crawl operations
    """
    try:
        graph = neomodels.graph

        # Merging nodes
        gkeys = ['url', 'title']
        ndata = [[result.url, result.title]] \
             + [[url, ''] for url in result.links]
        merge_nodes(graph.auto(), ndata, ('Record', 'url'), keys=gkeys, preserve=['url'])
        print(f'[{os.getpid()}] Added/updated {len(ndata)} nodes.')

        # Linking all data together
        ltodata = []
        lfromdata = []
        for tolink in result.links:
            ltodata.append((result.url, [], tolink))
            lfromdata.append((tolink, [], result.url))

        merge_relationships(
            graph.auto(),
            ltodata,
            'LINKED_TO',
            start_node_key=('Record', 'url'),
            end_node_key=('Record', 'url'),
            keys=['url']
        )

        merge_relationships(
            graph.auto(),
            lfromdata,
            'LINKED_FROM',
            start_node_key=('Record', 'url'),
            end_node_key=('Record', 'url'),
            keys=['url']
        )
        print(f'[{os.getpid()}] Created {len(lfromdata) + len(ltodata)} relationships for {result.url}.')

    except Exception as ex:
        # requeueing to re-process at a later time
        print(f'[{os.getpid()}] Failed to update nodes for {result.url}. Requeuing: {ex}')
        dispatcher.write_futures += [dispatcher.executor.submit(process_write_neo4j, result, dispatcher)]

@db_session
def process_write_postgres(results:list[CrawlResult]):
    """
    Async writer process. This is done so we may continue to crawl while we perform
    our intense postgres write operations.

    In this process, we will
    1. Update all dispatched crawl records with the results
    2. Add new crawl records for any new URLs to be crawled in the future

    :param results list of results from crawl operations
    """
    try:
        # organizing data
        bucketized_results:dict[str, CrawlResult] = {cr.url: cr for cr in results}
        all_crawled_urls = [cr.url for cr in results]
        all_new_links = set([link for cr in results for link in cr.links])

        # Updating all crawled records
        crawled_records:list[PGCrawlerRecord] = select(r for r in PGCrawlerRecord if r.normalized_url in all_crawled_urls)[:]
        for pgrecord in crawled_records:
            crecord = bucketized_results[pgrecord.normalized_url]
            pgrecord.crawled = True
            pgrecord.crawled_date = crecord.crawled_time
            pgrecord.success = crecord.success

            if crecord.title is not None:
                pgrecord.title = crecord.body

            if crecord.body is not None:
                pgrecord.body = crecord.body

        print(f'[{os.getpid()}] Updated {len(crawled_records)} crawl records.')

        # Finding all links that have already been added to our crawl records
        exists_pgrecords:list[PGCrawlerRecord] = \
            select(r for r in PGCrawlerRecord if r.normalized_url in all_new_links)[:]
        existsurls = [r.normalized_url for r in exists_pgrecords]

        # Adding new crawl records for any that do not already exists
        newlinkurls = [url for url in all_new_links if url not in existsurls]
        for newlink in newlinkurls:
            PGCrawlerRecord(
                normalized_url=newlink,
                crawled=False
            )

        commit()
        print(f'[{os.getpid()}] Added {len(newlinkurls)} new URLs for crawling.')
    except Exception as ex:
        print(f'[{os.getpid()}] {ex}')

@db_session
def process_crawl(url:str) -> CrawlResult:
    """
    Main processpool entrypoint. Will do the following:

    1. Fetch content from URL
    2. Extract all normalized urls from links in content
    3. Return results to dispatcher.

    Upon a 5 second timeout, this process will be killed.

    :return result of process
    """
    try:
        response = get_links_from_normalized_url(url)

        # if we encountered some issues in get step, we will set success flag to false
        if response is None:
            print(f'[{os.getpid()}] Failed to process {url}.')
            return CrawlResult(
                url=url,
                crawled_time=datetime.now(),
                success=False
            )

        # Processing complete. We can return our result
        print(f'[{os.getpid()}] Processed {url}')
        title, text, normalized_urls = response
        return CrawlResult(
            url=url,
            crawled_time=datetime.now(),
            success=True,
            title=title,
            body=text,
            links=normalized_urls
        )
    except Exception as ex:
        print(f'[{os.getpid()}] {ex.with_traceback()}')

    return url, set()  
