"""
entrypoint for our crawler. creates connections and initializes crawler/query tool
"""
# TODO: parse commands
import pgmodels
from crawl import CrawlerDispatcher

pgmodels.initialize()

dispatcher = CrawlerDispatcher()

dispatcher.begin()