distributed-graph-analytics
===========================

Distributed Graph Analytics (DGA) is a compendium of graph analytics written for Bulk-Synchronous-Parallel (BSP) processing frameworks such as Giraph and GraphX.

Currently, DGA supports the following analytics:

###### Giraph
- Weakly Connected Components
- Leaf Compression
- Page Rank
- High Betweenness Set Extraction
- Louvain

###### GraphX
- Louvain Modularity (initial stage)
- Weakly Connected Components
- High Betweenness Set Extraction
- Leaf Compression
- Page Rank
- Neighboring Communities

###### dga-giraph
dga-giraph is the project that contains our Giraph implementation of DGA.  For more information, go here: [dga-giraph README.md](https://github.com/Sotera/distributed-graph-analytics/tree/master/dga-giraph)

###### documentation
[http://sotera.github.io/distributed-graph-analytics](http://sotera.github.io/distributed-graph-analytics/)

###### Scala 
Supported Scala version: 2.11.1
Supported Scala installation location: /opt/scala

wget http://downloads.typesafe.com/scala/2.11.1/scala-2.11.1.tgz
tar xvf scala-2.11.1.tgz
sudo mv scala-2.11.1 /opt/scala

###### Spark Core and GraphX
Supported Spark Core and GraphX version: 1.3.0
Supported Spark installation location: /opt/spark
