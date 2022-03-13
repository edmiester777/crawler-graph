"""
entrypoint for our crawler. creates connections and initializes crawler/query tool
"""
# TODO: parse commands
from crawl import CrawlerDispatcher

dispatcher = CrawlerDispatcher()

dispatcher.begin()