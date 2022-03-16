from py2neo import Graph

graph:Graph = None

def initialize():
    "Initialize neo4j connections."
    global graph
    graph = Graph('bolt://host.docker.internal:7687')