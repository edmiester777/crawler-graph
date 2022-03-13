import neomodels
from neomodels import Record as NeoRecord, initialize as initneo
from pgmodels import PGCrawlerRecord
from pony.orm import *
from urllib.parse import urlparse
from tabulate import tabulate

@db_session
def query_root_domain(domain:str):
    """
    Query a root domain (like google.com or mail.google.com) for stats about it's
    references. This will print a table for the resulting stats in stdout.

    :arg:domain root level domain to query for
    """
    initneo()
    neorepo = neomodels.neorepo

    # finding all crawled records with this root domain
    pgrecords = select(
        r for r in PGCrawlerRecord
        if r.crawled == True
        and r.success == True
        and r.normalized_url.startswith(domain)
    )[:]

    # Loading graph nodes
    neonodes:list[NeoRecord] = []
    for pgrecord in pgrecords:
        node = neorepo.match(NeoRecord, pgrecord.normalized_url).first()
        if node is not None:
            neonodes.append(node)

    # Calculating links from each root domain
    links = {}
    for tonode in neonodes:
        for fromnode in tonode.linked_from:
            rootdomain = urlparse(f'https://{fromnode.url}').netloc

            if rootdomain not in links:
                links[rootdomain] = 1
            else:
                links[rootdomain] += 1

    print(f'Breakdown of connections from different root domains for {domain}')
    print('')
    table = [[link, links[link]] for link in links.keys()]
    print(tabulate(table, headers=["From Domain", "Number of links to this domain"]))

query_root_domain('www.messenger.com')