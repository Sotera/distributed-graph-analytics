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
import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;


/**
 * Maintains a list of the top N items(an item is defined as an int id, and double value), ranked by a double value.
 * Designed for use with the giraph Aggregator pattern.
 */
public class HighBetweennessList implements Writable {

    /**
     * Container class to store an id and value
     */
    class BcTuple {
        /**
         * Vertex Id
         */
        String id;
        /**
         * Approx. Betweenness.
         */
        double value;

        /**
         * Constructor for a New BcTuple
         *
         * @param id    Vertex Id
         * @param value Betweenness Value
         */
        public BcTuple(String id, double value) {
            this.id = id;
            this.value = value;
        }
    }

    /**
     * BcTuple Comparator
     * Used to order based on value.
     */
    public static Comparator<BcTuple> comparator = new Comparator<BcTuple>() {
        public int compare(BcTuple arg0, BcTuple arg1) {
            return (arg0.value < arg1.value) ? -1 : (arg0.value > arg1.value) ? 1 : 0;
        }

    };


    /**
     * The maximum number of tuples to keep.
     */
    private int maxSize;

    /**
     * A PriorityQueue of BcTuple's.
     */
    private PriorityQueue<BcTuple> highBetweennessQueue;

    /**
     * Creates a new HighBetweennessList with Max size 1.
     */
    public HighBetweennessList() {
        this(1);
    }

    /**
     * Creates a new HighBetweennessList with a Custom MaxSize
     *
     * @param maxSize The maximum size of the priority queue.
     */
    public HighBetweennessList(int maxSize) {
        this.maxSize = maxSize;
        highBetweennessQueue = new PriorityQueue<BcTuple>(maxSize, comparator);
    }

    /**
     * Creates a new HighBetweennessList with maxSize 1 and Adds a Value.
     *
     * @param id    Vertex Id
     * @param value Betweenness Value
     */
    public HighBetweennessList(String id, double value) {
        this();
        highBetweennessQueue.add(new BcTuple(id, value));
    }

    /**
     * Creates a new HighBetweennessList with a custom size and initial value.
     *
     * @param maxSize The maximum size of the priority queue.
     * @param id      Vertex Id
     * @param value   Betweenness Value
     */
    public HighBetweennessList(int maxSize, String id, double value) {
        this(maxSize);
        highBetweennessQueue.add(new BcTuple(id, value));

    }


    /**
     * Add items from other to this.  Keeping only the top N (maxSize) items.
     *
     * @param other Another Highbetweenness set.
     */
    public void aggregate(HighBetweennessList other) {
        if (other.maxSize > maxSize) {
            maxSize = other.maxSize;
        }

        for (BcTuple t : other.getQueue()) {
            if (highBetweennessQueue.size() < maxSize) {
                highBetweennessQueue.add(t);
            } else {
                BcTuple first = highBetweennessQueue.peek();
                if (first.value < t.value) {
                    highBetweennessQueue.poll();
                    highBetweennessQueue.add(t);
                }
            }
        }
    }


    /**
     * @return the ids of the stored items, as a set.
     */
    public Set<String> getHighBetweennessSet() {
        Set<String> set = new HashSet<String>();
        for (BcTuple t : highBetweennessQueue) {
            set.add(t.id);
        }
        return set;
    }


    /**
     * Write fields
     */
    public void write(DataOutput out) throws IOException {
        out.writeInt(maxSize);
        int size = (highBetweennessQueue == null) ? 0 : highBetweennessQueue.size();
        out.writeInt(size);
        if (highBetweennessQueue != null) {
            for (BcTuple t : highBetweennessQueue) {
                Text.writeString(out, t.id);
                out.writeDouble(t.value);
            }
        }
    }

    /**
     * Read fields
     */
    public void readFields(DataInput in) throws IOException {
        maxSize = in.readInt();
        highBetweennessQueue = new PriorityQueue<BcTuple>(maxSize, comparator);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            highBetweennessQueue.add(new BcTuple(Text.readString(in), in.readDouble()));
        }
    }


    /**
     * @return the priority queue backing this list.
     */
    public PriorityQueue<BcTuple> getQueue() {
        return highBetweennessQueue;
    }


    /**
     * Return a string representation of the list.
     */
    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("{maxSize: ").append(maxSize).append(", high betweenness set: ");
        b.append("[");
        if (this.highBetweennessQueue != null) {
            for (BcTuple t : highBetweennessQueue) {
                b.append("(").append(t.id).append(",").append(t.value).append(")");
            }
        }
        b.append("] }");
        return b.toString();
    }


    /**
     * @return the maxSize of this list.
     */
    public int getMaxSize() {
        return maxSize;
    }


    /**
     * Set the maxSize of this list
     *
     * @param maxSize The maximum size of the priority queue.
     */
    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

}
