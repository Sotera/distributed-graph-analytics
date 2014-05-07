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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Represents a source vertex's dependency on a specific target vertex.
 * 
 * For more information about pair dependencies and betweenness centrality see
 *   "U. Brandes, A Faster Algorithm for Betweenness Centrality"
 * 
 * @author Eric Kimbrel - Sotera Defense, eric.kimbrel@soteradefense.com
 *
 */
public class PartialDependency implements Writable{

	// the dependency of a node on a source node 
	private double dependency;
	
	// the number of successors that still need to be accumulated before the dependency value
	// is correct
	private int successors;
	
	
	// CONSTRUCTORS
	
	public PartialDependency(){
		dependency = 0;
		successors = 0;
	}
	
	public PartialDependency(int successors, double dependency){
		this.successors = successors;
		this.dependency = dependency;
	}
	
	
	// GETTERS / SETTERS
	
	public double getDependency() {
		return dependency;
	}
	public void setDependency(double dependency) {
		this.dependency = dependency;
	}
	public int getSuccessors() {
		return successors;
	}
	public void setSuccessors(int successors) {
		this.successors = successors;
	}
	
	
	/**
	 * Update the successor count by a delta
	 * @param diff
	 */
	public void addSuccessors(int diff){
		this.successors += diff;
	}
	
	/**
	 * Add to the accumulated dependency value by a delta
	 * @param diff
	 */
	public void addDep(double diff){
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
