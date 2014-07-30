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
package com.soteradefense.dga.hbse;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Keeps Shortest path data for a single source vertex to a single target vertex.
 * <p/>
 * Maintains Shortest path, predecessors, and number of shortest paths from the source
 * to each predecessor.
 */
public class ShortestPathList implements Writable {

    /**
     * The distance from source to this vertex on the shortest path.
     */
    private BigInteger distance;

    /**
     * The map of predecessor to number of shortest paths from source to that predecessor
     */
    private Map<String, BigInteger> predecessorPathCountMap;

    /**
     * Create a new shortest empty Path List
     */
    public ShortestPathList() {
        distance = BigInteger.valueOf(Long.MAX_VALUE);
        setPredecessorPathCountMap(new HashMap<String, BigInteger>());
    }

    /**
     * Create a new shortest path list based on a
     * shortest path message.
     *
     * @param data Path data to add to the map.
     */
    public ShortestPathList(PathData data) {
        this();
        this.distance = data.getDistance();
        predecessorPathCountMap.put(data.getFrom(), data.getNumPaths());
    }

    /**
     * @return The number of shortest paths from source to this vertex
     */
    public BigInteger getShortestPathCount() {
        BigInteger paths = BigInteger.valueOf(0L);
        for (BigInteger predecessor : predecessorPathCountMap.values()) {
            paths = paths.add(predecessor);
        }
        return paths;
    }

    /**
     * Update This shortest path list based on a new shortest path message
     *
     * @param data A new path data message.
     * @return true if the ShortestPathList is modified in anyway, otherwise false.
     */
    public boolean update(PathData data) {
        if (data.getDistance().equals(this.distance)) {
            if (!this.predecessorPathCountMap.containsKey(data.getFrom())) {
                predecessorPathCountMap.put(data.getFrom(), data.getNumPaths());
                return true;
            } else {
                BigInteger oldValue = predecessorPathCountMap.get(data.getFrom());
                boolean update = !oldValue.equals(data.getNumPaths());
                if (update) {
                    predecessorPathCountMap.put(data.getFrom(), data.getNumPaths());
                }
                return update;
            }
        } else if (data.getDistance().compareTo(this.distance) < 0) {
            this.distance = data.getDistance();
            this.predecessorPathCountMap.clear();
            predecessorPathCountMap.put(data.getFrom(), data.getNumPaths());
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, distance.toString());
        out.writeInt(this.predecessorPathCountMap.size());
        for (Entry<String, BigInteger> entry : predecessorPathCountMap.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue().toString());
        }

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.predecessorPathCountMap.clear();
        setDistance(new BigInteger(Text.readString(in)));
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            predecessorPathCountMap.put(Text.readString(in), new BigInteger(Text.readString(in)));
        }
    }

    /**
     * @return The distance from a source to this vertex.
     */
    public BigInteger getDistance() {
        return distance;
    }

    /**
     * Sets the distance from a source to this vertex.
     *
     * @param distance The distance to set it to.
     */
    public void setDistance(BigInteger distance) {
        this.distance = distance;
    }

    /**
     * Gets the Predecessor Count Map.
     *
     * @return The HashMap
     */
    public Map<String, BigInteger> getPredecessorPathCountMap() {
        return predecessorPathCountMap;
    }

    /**
     * Sets the Predecessor Path Count Map.
     *
     * @param predPathCountMap The Map to set it to.
     */
    public void setPredecessorPathCountMap(Map<String, BigInteger> predPathCountMap) {
        this.predecessorPathCountMap = predPathCountMap;
    }

}