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
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * A Simple PivotSetWritable.
 */
public class PivotSetWritable implements Writable {

    /**
     * A set to store Vertex Ids.
     */
    private Set<String> pivots;

    /**
     * Constructor that accepts a Collection of Pivots for initialization.
     * @param pivots A Collection of pivots.
     */
    public PivotSetWritable(Collection<String> pivots) {
        super();
        this.pivots = new HashSet<String>();
        this.pivots.addAll(pivots);
    }

    /**
     * Default constructor that initializes with an empty HashSet.
     */
    public PivotSetWritable() {
        this(new HashSet<String>());
    }

    /**
     * Adds all pivots of another PivotSet to this PivotSet.
     * @param other A PivotWritable object.
     */
    public void aggregate(PivotSetWritable other) {
        pivots.addAll(other.getPivots());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(pivots.size());
        for (String pivot : pivots) {
            Text.writeString(out, pivot);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        pivots.clear();
        int size = in.readInt();
        for (int i = 0; i < size; ++i) {
            pivots.add(Text.readString(in));
        }
    }

    /**
     * Gets the set of pivots.
     * @return A HashSet of Pivots.
     */
    public Set<String> getPivots() {
        return pivots;
    }
}