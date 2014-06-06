dga-giraph
============================
The dga-giraph project is a giraph implementation of the DGA library.

###### Pre-requisites
- Java 7
- CDH 5.0.0 (MRv1)
- Gradle 1.12 (or use the gradlew wrapper included with this project)
- Giraph 1.1.0-SNAPSHOT built for CDH 5.0.0 (instructions: <link to wiki page>)

###### Build Instructions
Assuming you have built giraph and installed it into your local maven repository (mvn clean install [other options])
'''bash
git clone https://github.com/Sotera/distributed-graph-analytics.git
cd distributed-graph-analytics
gradle clean distDGA
cp -r dga-giraph/build/dist /path/of/your/choosing'''

###### Using dga-giraph
'''
usage: hadoop jar dga-giraph-0.0.1.jar com.soteradefense.dga.DGARunner <analytic> <input-path> <output-path> [options]
  Allowed Analytics:
               louvain - Louvain Modularity
               hbse - High Betweenness Set Extraction
               wcc - Weakly Connected Components
               lc - Leaf Compression
               // pr - Page Rank - coming soon!
  -ca <arg>   Any custom arguments to pass in to giraph
  -D <arg>    System parameters to pass through to be added to the conf
  -h          Prints this help documentation and exits
  -q          Run analytic in quiet mode
  -w <arg>    The number of giraph workers to use for the analytic
'''

###### Configuring dga-giraph
DGA Giraph has 3 means of configuration; defaults, configuration file values, and command line configuration settings.
Any configuration key specified in the default will be overridden by that same key if it is given in the configuration file.
Any configuration key specified in either the default or the configuration file will be overridden by the property if specified on the command line.

Configuration file changes are meant to apply to dga-giraph for the general use of the install.  Things like a reasonable memory setting for
mapred.map.child.javaa.opts or number of workers or giraph.zkList are excellent values to place in the configuration file.  Then, if you need more memory or more workers, you can
override the configuration file by passing in the same arguments via the command line.

The options we allow you to specify are broken into 2 main types; -D key=value and -ca key=value.  In the XML configuration file, we have 3 types total; one of which are the base flags
built into Giraph (things like -w or -q) (which we call system (-D parameters), custom (-ca parameters), and default -
do not confuse this default with the default configuration values hard coded into the class com.soteradefense.dga.DGARunner)

A full list of the options that you can provide to each analytic can be found on this page: (DGA Giraph Options)[https://github.com/Sotera/distributed-graph-analytics/wiki/DGA-Giraph-Options]

###### Errata
- Unfortunately, the -D key=value pair is slightly different than Giraph's, which requires it in the form of -Dkey=value (no space).
Please note it operates exactly like Giraph's and is how you can pass [Giraph Options](http://giraph.apache.org/options.html) through to the system.
- There is a current bug where the dga-config.xml located in distributed-graph-analytics/dga-giraph/main/resources is included in the uber jar that
distDGA builds; this is a legacy issue from our attempt at adding this resource to YARN - which we don't even fully support yet.
To make configuration changes for your cluster, please make them in this file and rebuild the uber jar.  You can still override
every setting in the configuration file via the command line.

