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




### Steps to run Louvain GraphX
#### Download The CentOS VM
https://github.com/Sotera/seam-team-6-vm

#### Required Scala Version
Scala version: 2.11.1
Scala installation location: /opt/scala

wget http://downloads.typesafe.com/scala/2.11.1/scala-2.11.1.tgz
tar xvf scala-2.11.1.tgz
sudo mv scala-2.11.1 /opt/scala

#### Required Spark Core and GraphX
Spark Core and GraphX version: 1.3.0
Spark installation location: /opt/spark

#### Start Spark with one master and four worker instances
cd /usr/lib/spark/sbin
./start-master.sh
echo "export SPARK_WORKER_INSTANCES=4" >> spark-env.sh
./start-slaves.sh

#### Spark Web UI: Browse to the master web UI to make sure
#### the master and all the workers are started correctly
http://localhost:8080/

#### Copy the example data to HDFS:
wget http://sotera.github.io/distributed-graph-analytics/data/example.csv
hdfs dfs -put example.csv hdfs://localhost:8020/tmp/dga/louvain/input/

#### Clone the repository and build the code
cd /vagrant
git clone https://github.com/Sotera/distributed-graph-analytics
cd distributed-graph-analytics
gradle clean dist
cd /vagrant/distributed-graph-analytics/dga-graphx/build/dist
./run.sh

### You can find the output in:
hdfs://localhost:8020/tmp/dga/louvain/output
