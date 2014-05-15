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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


/**
 * Stores all required data for a vertex
 * <ul>
 * <li>Map of Sources to ShortestPathLists (for shortest path phase)</li>
 * <li>Map of Sources to PartialDependency (for pair dependency phase)</li>
 * <li>approximated betweenness value</li>
 * </ul>
 */
public class VertexData implements Writable {

    /**
     * The map of source vertex to shortest path data
     */
    private Map<String, ShortestPathList> pathDataMap = new HashMap<String, ShortestPathList>();

    /**
     * map of source vertex to pair dependency data
     */
    private Map<String, PartialDependency> partialDepMap = new HashMap<String, PartialDependency>();

    /**
     * current approximated betweenness value
     * ( equals actual betweenness when / if all vertices are used as sources)
     */
    private double approxBetweenness = 0.0;

    /**
     * Add the new path to the list of shortest paths, if it is indeed a shortest path
     *
     * @param data A shortest path message
     * @return The updated ShortestPathList, or null if no update was made.
     */
    public ShortestPathList addPathData(PathData data) {
        String source = data.getSource();
        ShortestPathList list = getPathDataMap().get(source);

        // if the list was empty ad the first item and return
        if (list == null) {
            list = new ShortestPathList(data);
            getPathDataMap().put(source, list);
            return list;
        } else {
            return (list.update(data)) ? list : null;
        }

    }

    /**
     * Updates pair dependency data
     *
     * @param src            The source vertex
     * @param dep            The partial dependency value of this vertex on the source
     * @param successorDelta The change in number of successors remaining to accumulate. -1 if accumulating a dependency, 1 if adding a successor.
     * @return the PartialDependency object that was updated.
     */
    public PartialDependency addPartialDep(String src, double dep, int successorDelta) {
        PartialDependency current;
        if (partialDepMap.containsKey(src)) {
            current = partialDepMap.get(src);
            current.addSuccessors(successorDelta);
            current.addDependency(dep);
        } else {
            current = new PartialDependency(successorDelta, dep);
            partialDepMap.put(src, current);
        }
        return current;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(approxBetweenness);
        out.writeInt(pathDataMap.size());
        for (Entry<String, ShortestPathList> entry : pathDataMap.entrySet()) {
            Text.writeString(out, entry.getKey());
            entry.getValue().write(out);
        }

        out.writeInt(this.partialDepMap.size());
        for (Entry<String, PartialDependency> entry : partialDepMap.entrySet()) {
            Text.writeString(out, entry.getKey());
            entry.getValue().write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        //Reset Maps
        pathDataMap.clear();
        partialDepMap.clear();

        setApproxBetweenness(in.readDouble());
        // read the path data map
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            ShortestPathList list = new ShortestPathList();
            list.readFields(in);
            pathDataMap.put(key, list);
        }

        size = in.readInt();
        for (int i = 0; i < size; i++) {
            String src = Text.readString(in);
            PartialDependency dep = new PartialDependency();
            dep.readFields(in);
            partialDepMap.put(src, dep);
        }

    }

    /**
     * Gets the Partial Dependency Map
     *
     * @return The Partial Dependency Map
     */
    public Map<String, PartialDependency> getPartialDependencyMap() {
        return partialDepMap;
    }

    /**
     * Sets the Partial Dependency Map
     *
     * @param partialDepMap The Partial Dependency map to set it to.
     */
    public void setPartialDepMap(Map<String, PartialDependency> partialDepMap) {
        this.partialDepMap = partialDepMap;
    }

    /**
     * Gets the Approx Betweenness Value
     *
     * @return The approx betweenness value.
     */
    public double getApproxBetweenness() {
        return approxBetweenness;
    }

    /**
     * Sets the Approx. Betweenness value.
     *
     * @param approxBetweenness The approx. value to set it to.
     */
    public void setApproxBetweenness(double approxBetweenness) {
        this.approxBetweenness = approxBetweenness;
    }

    /**
     * Gets the Path Data Map.
     *
     * @return The path data map.
     */
    public Map<String, ShortestPathList> getPathDataMap() {
        return pathDataMap;
    }

    /**
     * Sets the path data map.
     *
     * @param pathDataMap The path data map to set it to.
     */
    public void setPathDataMap(Map<String, ShortestPathList> pathDataMap) {
        this.pathDataMap = pathDataMap;
    }


}
