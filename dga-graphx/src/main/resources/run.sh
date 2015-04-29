# ./dga-mr1-graphx louvain -i hdfs://localhost:8020/tmp/dga/louvain/input/example.csv -o hdfs://localhost:8020/tmp/dga/louvain/output/ -s /opt/spark -n NameOfJob -m spark://localhost.localdomain:7077 --S spark.executor.memory=1g --ca parallelism=2 --S spark.worker.timeout=400 --S spark.cores.max=12

# ./dga-mr1-graphx louvain -i tmp/dga/louvain/input/example.csv -o tmp/dga/louvain/output/ -s /opt/spark -n NameOfJob -m spark://localhost.localdomain:7077 --S spark.executor.memory=1g --ca parallelism=2 --S spark.worker.timeout=400 --S spark.cores.max=12

./dga-mr1-graphx louvain -i tmp/dga/louvain/input/example.csv -o tmp/dga/louvain/output/ -s /opt/spark -n ExampleData -m spark://localhost.localdomain:7077 --S spark.executor.memory=1g --ca parallelism=2 --S spark.worker.timeout=400 --S spark.cores.max=12

# ./dga-mr1-graphx louvain -i tmp/dga/louvain/input/jeb.tsv -o tmp/dga/louvain/output/ -s /opt/spark -n JebBushData -m spark://localhost.localdomain:7077 --S spark.executor.memory=1g --ca parallelism=2 --S spark.worker.timeout=400 --S spark.cores.max=12

# ./dga-mr1-graphx louvain -i tmp/dga/louvain/input/jeb.csv -o tmp/dga/louvain/output/ -s /opt/spark -n JebBushData -m spark://localhost.localdomain:7077 --S spark.executor.memory=1g --ca parallelism=2 --S spark.worker.timeout=400 --S spark.cores.max=12