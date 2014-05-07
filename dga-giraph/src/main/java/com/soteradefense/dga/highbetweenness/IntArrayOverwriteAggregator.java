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

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.IntWritable;

/**
 * Stores a single IntArrayWritable for global broadcast / communication.
 * 
 * @author Eric Kimbrel - Sotera Defense, eric.kimbrel@soteradefense.com
 */
public class IntArrayOverwriteAggregator extends BasicAggregator<IntArrayWritable>{
	
	/**
	 * Set the aggregated value of this aggregator to 
	 * the specified value.
	 */
	public void aggregate(IntArrayWritable value) {
		this.setAggregatedValue(value);
	}

	/**
	 * Returns an empty IntArrayWritable
	 */
	public IntArrayWritable createInitialValue() {
		return new IntArrayWritable(new IntWritable[0]);
	}

}
