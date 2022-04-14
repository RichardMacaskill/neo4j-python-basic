import pandas as pd
import numpy as np

import neo4j



con = neo4j.Neo4jConnector("bolt+s://cas-testlab-neo4j.co.uk:7687", "scratchuser", "Berlin99!")

# Run any Cypher query (e.g. remove all nodes and relationships)
# con.query("MATCH (n) DETACH DELETE n")
con.query("MATCH (n) RETURN n")

############################
# pandas DataFrame example #
############################

# Prepare some data: DataFrame with x and y column with every (x, y)
# combinations for -100 <= x < 100 and -100 <= y < 100.
data = np.stack(
    np.meshgrid(np.arange(-100, 100), np.arange(-100, 100)), -1
).reshape(-1, 2)

df = pd.DataFrame(data, columns=["x", "y"])

# df = con.save_nodes_df
# Create a node for every row in the DataFrame. The nodes will be labeled
# `DFNode` and have the `x` and `y` properties.
con.save_nodes_df(df, "DFNode")
# Side note: please use Cypher's UNWIND and range function to create those
# nodes in a real world scenario. This is just an example.

# Fetch all nodes with label `DFNode` again
nodes = con.load_nodes_df("DFNode")
print(nodes)


# Prepare some more data: DataFrame with 1 million rows and 5 columns:
# `sx`, `sy`, `z`, `tx`, and `ty`.
# Each will be a random number between -100 (inclusive) and 100 (exclusive).
data = np.random.randint(-100, 100, size=(1_000_000, 5))
df = pd.DataFrame(data, columns=["sx", "sy", "z", "tx", "ty"])

# CREATE (i.e. always create even if already exists) a relationship between
# the source node with label `DFNode`, `x`=`sx`, and `y`=`sy` and the target
# node with label `DFNode`, `x`=`tx`, and `y`=`ty`. The relationship will have
# the label `DFRel` and the property `z` with the value of `z`.

    # con.save_rel_df(df, "DFNode", "sx", "DFNode", "tx", "DFRel",
    #             src_attributes=["sy"], dst_attributes=["ty"],
    #             src_creation_mode=neo4py.RelNodeCreationMode.MATCH,
    #             dst_creation_mode=neo4py.RelNodeCreationMode.MATCH,
    #             creation_mode=neo4py.RelCreationMode.CREATE,
    #             df_key_to_attr_map={"sx": "x", "sy": "y",
    #                                 "tx": "x", "ty": "y"})
    #

# Fetch all relationships with label `DFRel` again.
# This is 1 million entries and reading cannot be parallelized.
# Hence, this will take a while.
relationships = con.load_rel_df("DFRel")
print(relationships)