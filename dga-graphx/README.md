# dga-graphx 

- GraphX Algorithms

The dga-graphX package contains several pre-built executable graph algorithms built on Spark using the GraphX framework.  

### pre-requisites

 * [Spark]  (http://spark.apache.org/)   1.0.x
 * [GraphX]  (http://spark.apache.org/docs/latest/graphx-programming-guide.html)   
 * [Gradle] (http://www.gradle.org/) 

### build

If necessary, edit the build.gradle and the dga-graphx run file to set your version of spark and graphx.

> gradle clean dist

Check the build/dist folder for the required files to run the graphx analytics.   


# Algorithms 

## High Betweenness Set Extraction

### About HBSE

HBSE is useful for detecting the critical points of a graph.

### Running HBSE
```
./dga-graphx hbse -i /path/to/input/example.csv -o /path/to/output/ -s /opt/spark -n NameGoesHere -m spark://spark.hostname:7077
```
## Weakly Connected Components

### About WCC

WCC is useful for detecting the individual components in a particular graph.

### Running WCC

To run our implementation:
```
./dga-graphx wcc -i /path/to/input/example.csv -o /path/to/output/ -s /opt/spark -n NameGoesHere -m spark://spark.hostname:7077
```
To run the graphx implementation:
```
./dga-graphx wccGraphX -i /path/to/input/example.csv -o /path/to/output/ -s /opt/spark -n NameGoesHere -m spark://spark.hostname:7077
```
## Leaf Compression

### About LC

LC is useful for compressing the graph into a smaller subset of the graph that only contains nodes with multiple edges.

### Running LC
```
./dga-graphx lc -i /path/to/input/example.csv -o /path/to/output/ -s /opt/spark -n NameGoesHere -m spark://spark.hostname:7077
```
## PageRank

### About PR

PageRank is useful for finding the nodes that carry the highest popularity in the graph.

### Running PR

To run our implementation:
```
./dga-graphx pr -i /path/to/input/example.csv -o /path/to/output/ -s /opt/spark -n NameGoesHere -m spark://spark.hostname:7077
```
To run the graphx implementation:
```
./dga-graphx prGraphX -i /path/to/input/example.csv -o /path/to/output/ -s /opt/spark -n NameGoesHere -m spark://spark.hostname:7077
```
## Louvain Modularity

### About Louvain

Louvain distributed community detection is a parallelized version of this work:
```
Fast unfolding of communities in large networks, 
Vincent D Blondel, Jean-Loup Guillaume, Renaud Lambiotte, Etienne Lefebvre, 
Journal of Statistical Mechanics: Theory and Experiment 2008 (10), P10008 (12pp)
```
In the original algorithm each vertex examines the communities of its neighbors and makes a chooses a new community based on a function to maximize the calculated change in modularity.  In the distributed version all vertices make this choice simultaneously rather than in serial order, updating the graph state after each change.  Because choices are made in parallel some choice will be incorrect and will not maximize modularity values, however after repeated iterations community choices become more stable and we get results that closely mirror the serial algorithm.

### Running Louvain
```
./dga-graphx louvain -i /path/to/input/example.csv -o /path/to/output/ -s /opt/spark -n NameGoesHere -m spark://spark.hostname:7077
```
Spark produces alot of output, so sending stderr to a log file is recommended.  Examine the test_output folder. you should see

```
test_output/
├── level_0_edges
│   ├── _SUCCESS
│   └── part-00000
├── level_0_vertices
│   ├── _SUCCESS
│   └── part-00000
└── qvalues
    ├── _SUCCESS
    └── part-00000
```

```
cat test_output/level_0_vertices/part-00000 
(7,{community:8,communitySigmaTot:13,internalWeight:0,nodeWeight:3})
(4,{community:4,communitySigmaTot:21,internalWeight:0,nodeWeight:4})
(2,{community:4,communitySigmaTot:21,internalWeight:0,nodeWeight:4})
(6,{community:8,communitySigmaTot:13,internalWeight:0,nodeWeight:4})
(8,{community:8,communitySigmaTot:13,internalWeight:0,nodeWeight:3})
(5,{community:4,communitySigmaTot:21,internalWeight:0,nodeWeight:4})
(9,{community:8,communitySigmaTot:13,internalWeight:0,nodeWeight:3})
(3,{community:4,communitySigmaTot:21,internalWeight:0,nodeWeight:4})
(1,{community:4,communitySigmaTot:21,internalWeight:0,nodeWeight:5})

cat test_output/qvalues/part-00000 
(0,0.4134948096885813)
```

Note: the output is laid out as if you were in hdfs even when running local.  For each level you see an edges directory and a vertices directory.   The "level" refers to the number of times the graph has been "community compressed".  At level 1 all of the level 0 vertices in community X are represented by a single vertex with the VertexID: X.  For the small example all modulairyt was maximized with no community compression so only level 0 was computed.  The vertices show the state of each vertex while the edges file specify the graph structure.   The qvalues directory lists the modularity of the graph at each level of compression.  For this example you should be able to see all of vertices splitting off into two distinct communities (community 4 and 8 ) with a final qvalue of ~ 0.413


### Running dga on a cluster

To run on a cluster be sure your input and output paths are of the form "hdfs://<namenode>/path" and ensure you provide the --master and --sparkhome options.  The --jars option is already coded into the 
default config options

### Parallelism

To change the level of parallelism use the --ca parallelism=400.  If this option is not set parallelism will be based on the layout of the input data in HDFS.  The number of partitions
 of the input file sets the level of parallelism.   

### How To Run:

./dga-graphx wcc -i hdfs://spark.hostname:8020/path/to/input/example.csv -o hdfs://spark.hostname:8020/path/to/output/ -s /opt/spark -n NameGoesHere -m spark://spark.hostname:7077

### Advanced

If you would like to include any of these analytics in your own pipeline, then all you need to do is extend the abstract runner in either of the analytic packages.
