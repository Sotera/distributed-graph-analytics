#! /bin/bash

java -cp "build/dist/*" com.soteradefense.dga.graphx.louvain.Main -i examples/small_edges.tsv -o output
