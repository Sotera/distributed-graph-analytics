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

/**
 * Represents a source vertex's dependency on a specific target vertex.
 * <p/>
 * For more information about pair dependencies and betweenness centrality see
 * "U. Brandes, A Faster Algorithm for Betweenness Centrality"
 */
public class PartialDependency implements Writable {

    /**
     * The Dependency of a single node and a single source.
     */
    private double dependency;

    /**
     * The number of successors that still need to be accumulated before approx. betweenness is finished.
     */
    private int successors;


    /**
     * Default Constructor for a PartialDependency.  Dependency and Successors are both set to 0.
     */
    public PartialDependency() {
        dependency = 0;
        successors = 0;
    }

    /**
     * Constructor that accepts successors and dependency for initialization.
     *
     * @param successors The number of successors that still have yet to be processed.
     * @param dependency The dependency of a single node and a single source.
     */
    public PartialDependency(int successors, double dependency) {
        this.successors = successors;
        this.dependency = dependency;
    }


    /**
     * Gets the dependency.
     *
     * @return The dependency value.
     */
    public double getDependency() {
        return dependency;
    }

    /**
     * Sets the dependency value.
     *
     * @param dependency The value to set dependency to.
     */
    public void setDependency(double dependency) {
        this.dependency = dependency;
    }

    /**
     * Gets the number of successors that need to be processed.
     *
     * @return The successors value.
     */
    public int getSuccessors() {
        return successors;
    }

    /**
     * Sets the number of successors.
     *
     * @param successors The value to set successors to.
     */
    public void setSuccessors(int successors) {
        this.successors = successors;
    }


    /**
     * Update the successor count by a delta
     *
     * @param diff Value to increase the successors by.
     */
    public void addSuccessors(int diff) {
        this.successors += diff;
    }

    /**
     * Add to the accumulated dependency value by a delta
     *
     * @param diff Value to accumulate by.
     */
    public void addDep(double diff) {
        this.dependency += diff;
    }


    // I/O

    public void write(DataOutput out) throws IOException {
        out.writeInt(successors);
        out.writeDouble(dependency);

    }

    public void readFields(DataInput in) throws IOException {
        successors = in.readInt();
        dependency = in.readDouble();
    }

}
