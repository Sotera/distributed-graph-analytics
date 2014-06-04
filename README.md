distributed-graph-analytics
===========================

Distributed Graph Analytics (DGA) is a compendium of graph analytics written for Bulk-Synchronous-Parallel (BSP) processing frameworks such as Giraph and GraphX.

Currently, DGA supports the following analytics:

###### Giraph
- Weakly Connected Components
- Leaf Compression
- Page Rank
- High Betweenness Set Extraction

###### GraphX
- Louvain Modularity

Eventually, the following analytics will be supported:

- Louvain Modularity
- Weakly Connected Components
- High Betweenness Set Extraction
- Leaf Compression
- Page Rank


How to Build:
=============
gradle clean distDGA

The jar with config files will be output to the following directory: dga-giraph/build/dist/

Note: You must have the hadoop jars included elsewhere.  This only builds DGA and include Giraph Dependencies that are not included with
hadoop.