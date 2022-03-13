"""
entrypoint for our crawler. creates connections and initializes crawler/query tool
"""
from argparse import ArgumentParser
import argparse

def main():
    parser = ArgumentParser()
    parser.add_argument('-crawl', action=argparse.BooleanOptionalAction)
    parser.add_argument('-query', action=argparse.BooleanOptionalAction)
    parser.add_argument('--domain', type=str, default=None)
    parser.add_argument('--workers', type=int, default=4)
    args = parser.parse_args()

    if args.crawl and args.query:
        print('You must specify only crawl or query.')
        return

    if args.crawl:
        from crawl import CrawlerDispatcher
        dispatcher = CrawlerDispatcher(args.workers)
        dispatcher.begin()

    elif args.query:
        if not args.domain:
            print('you must supply a --domain for query')
            return

        import query
        query.query_root_domain(args.domain)

    else:
        print('you must specify an action.')

main()