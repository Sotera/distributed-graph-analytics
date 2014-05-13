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
package com.soteradefense.dga.highbetweenness;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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
    private long distance;

    /**
     * The map of predecessor to number of shortest paths from source to that predecessor
     */
    private Map<String, Long> predPathCountMap;


    /**
     * Create a new shortest empty Path List
     */
    public ShortestPathList() {
        distance = Long.MAX_VALUE;
        setPredPathCountMap(new HashMap<String, Long>());
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
        predPathCountMap.put(data.getFrom(), data.getNumPaths());
    }


    /**
     * @return The number of shortest paths from source to this vertex
     */
    public long getNumShortestPaths() {
        long paths = 0L;
        for (long pred : predPathCountMap.values()) {
            paths += pred;
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
        if (data.getDistance() == this.distance) {
            if (!this.predPathCountMap.containsKey(data.getFrom())) {
                predPathCountMap.put(data.getFrom(), data.getNumPaths());
                return true;
            } else {
                long oldValue = predPathCountMap.get(data.getFrom());
                boolean update = oldValue != data.getNumPaths();
                if (update) {
                    predPathCountMap.put(data.getFrom(), data.getNumPaths());
                }
                return update;
            }
        } else if (data.getDistance() < this.distance) {
            this.distance = data.getDistance();
            this.predPathCountMap.clear();
            predPathCountMap.put(data.getFrom(), data.getNumPaths());
            return true;
        } else {
            return false;
        }
    }


    // I/ O

    public void write(DataOutput out) throws IOException {
        out.writeLong(distance);
        out.writeInt(this.predPathCountMap.size());
        for (Entry<String, Long> entry : predPathCountMap.entrySet()) {
            out.writeBytes(entry.getKey());
            out.writeLong(entry.getValue());
        }

    }


    public void readFields(DataInput in) throws IOException {
        setDistance(in.readLong());
        int size = in.readInt();
        this.predPathCountMap.clear();
        for (int i = 0; i < size; i++) {
            //TODO: EXPLORE THE READ WRITE WITH STRINGS
            predPathCountMap.put(in.readLine(), in.readLong());
        }
    }


    // GETTERS / SETTERS

    /**
     * @return The distance from a source to this vertex.
     */
    public long getDistance() {
        return distance;
    }

    /**
     * Sets the distance from a source to this vertex.
     *
     * @param distance The distance to set it to.
     */
    public void setDistance(long distance) {
        this.distance = distance;
    }

    /**
     * Gets the Predecessor Count Map.
     *
     * @return The HashMap
     */
    public Map<String, Long> getPredPathCountMap() {
        return predPathCountMap;
    }

    /**
     * Sets the Predecessor Path Count Map.
     *
     * @param predPathCountMap The Map to set it to.
     */
    public void setPredPathCountMap(Map<String, Long> predPathCountMap) {
        this.predPathCountMap = predPathCountMap;
    }

}