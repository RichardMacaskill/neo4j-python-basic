from graphdatascience import GraphDataScience

# Use Neo4j URI and credentials according to your setup
gds = GraphDataScience("bolt+s://cas-testlab-neo4j.co.uk:7687", auth=("neo4j", "Berlin99!"))
gds.set_database('europeanroads')