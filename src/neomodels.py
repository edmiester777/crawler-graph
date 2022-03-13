from py2neo.ogm import Repository, Model, Property, RelatedTo

neorepo:Repository = None

class Record(Model):
    """
    Model describing a node in the link graph.
    """
    __primarykey__ = 'url'
    url = Property()
    title = Property()

    linked = RelatedTo('Record', 'LINKED_TO')

def initialize():
    "Initialize neo4j connections."
    global neorepo
    neorepo = Repository('bolt://host.docker.internal:7687')