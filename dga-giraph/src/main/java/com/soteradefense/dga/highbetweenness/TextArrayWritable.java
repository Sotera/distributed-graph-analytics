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

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

/**
 * A Simple IntArrayWritable.
 */
public class TextArrayWritable extends ArrayWritable {
    /**
     * The Default Constructor for creating a new IntArrayWritable.
     */
    public TextArrayWritable() {
        super(Text.class);
    }

    /**
     * Constructor to create a new IntArrayWritable with an array of IntWritable.
     *
     * @param values An array of IntWritable
     */
    public TextArrayWritable(Text[] values) {
        super(Text.class, values);
    }

}