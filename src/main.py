"""
entrypoint for our crawler. creates connections and initializes crawler/query tool
"""
from asyncore import dispatcher
import logging

# TODO: parse commands
import esdocuments
from crawl import CrawlerDispatcher

logging.basicConfig(level=logging.DEBUG)

esdocuments.initialize()

dispatcher = CrawlerDispatcher()

dispatcher.begin()