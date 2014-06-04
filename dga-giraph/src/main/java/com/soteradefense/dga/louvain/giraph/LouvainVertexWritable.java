/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.soteradefense.dga.louvain.giraph;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * Writable class to represent community information for compressing a graph
 * by its communities.
 */
public class LouvainVertexWritable implements Writable {

    private static Logger logger = LoggerFactory.getLogger(LouvainVertexWritable.class);

    private long weight;
    private Map<String, Long> edges;

    public LouvainVertexWritable() {
        this(0, new HashMap<String, Long>());
    }

    public LouvainVertexWritable(long weight, Map<String, Long> edges) {
        this.weight = weight;
        this.edges = edges;
    }

    public long getWeight() {
        return weight;
    }

    public void setWeight(long weight) {
        this.weight = weight;
    }

    public Map<String, Long> getEdges() {
        return edges;
    }

    public void setEdges(Map<String, Long> edges) {
        this.edges = edges;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        weight = in.readLong();
        int edgeSize = in.readInt();
        edges = new HashMap<String, Long>(edgeSize);
        for (int i = 0; i < edgeSize; i++) {
            edges.put(WritableUtils.readString(in), in.readLong());
        }

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(weight);
        out.writeInt(edges.size());
        for (Map.Entry<String, Long> entry : edges.entrySet()) {
            WritableUtils.writeString(out, entry.getKey());
            out.writeLong(entry.getValue());
        }
    }

    public static LouvainVertexWritable fromTokens(String weight, String edges) {
        LouvainVertexWritable vertex = new LouvainVertexWritable();
        vertex.setWeight(Long.parseLong(weight));
        Map<String, Long> edgeMap = vertex.getEdges();
        if (edges.length() > 0) {
            for (String edgeTuple : edges.split(",")) {
                String[] edgeTokens = edgeTuple.split(":");
                try {
                    edgeMap.put(edgeTokens[0], Long.parseLong(edgeTokens[1]));
                } catch (NumberFormatException e) {
                    logger.error("Unable to parse edgeTuple {} from group of edges, {}", edgeTuple, edges);
                }
            }
        }
        return vertex;
    }

}
