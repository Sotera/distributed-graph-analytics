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
 * Message that can be passed between vertices.  Used for both shortest path computation and pair dependency
 * accumulation.  Some fields are only used in one of the two phases, or take on different meaning in the different phases.
 * 
 * For more information on this method of calculation betweenness centrality see
 * "U. Brandes, A Faster Algorithm for Betweenness Centrality"
 * 
 * @author Eric Kimbrel - Sotera Defense, eric.kimbrel@soteradefense.com
 *
 */
public class PathData implements Writable{

	// distance of this path
	private long distance;
	
	// source node originating this path
	private int source;
	
	// the predecessor OR successor node (for shortest path OR pair dependency accumulation)
	private int from;
	
	// the sources dependency on the successor
	private double dependency;
	
	// number of shortest paths from source to the predecessor
	private long numPaths;
	
	
	/* Constructors */
	
	public PathData(){
		distance = Long.MAX_VALUE;
		source = -1;
		from = -1;
		dependency = -1;
		numPaths = -1;
	}
	
	
	/**
	 * Get a new PathData message for shortest path computation.
	 * @param source
	 * @param from
	 * @param distance
	 * @param numPaths
	 * @return
	 */
	public static PathData getShortestPathMessage(int source, int from, long distance, long numPaths){
		PathData data = new PathData();
		data.setSource(source);
		data.setFrom(from);
		data.setDistance(distance);
		data.setNumPaths(numPaths);
		return data;
	}
	
	
	/**
	 * Get a new PathData message for sending successor / predecessor information to neighbors
	 * @param source
	 * @return
	 */
	public static PathData getPingMessage(int source){
		PathData data = new PathData();
		data.setSource(source);
		return data;
	}
	
	/**
	 * Get a new PathData message for accumulating pair dependency values
	 * @param source
	 * @param dependency
	 * @param numPaths
	 * @return
	 */
	public static PathData getDependencyMessage(int source,double dependency,long numPaths){
		PathData data = new PathData();
		data.setSource(source);
		data.setDependency(dependency);
		data.setNumPaths(numPaths);
		return data;
	}
	
	
	
	// I/O
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(source);
		out.writeInt(from);
		out.writeLong(distance);
		out.writeLong(numPaths);
		out.writeDouble(dependency);
	}
	
	
	public void readFields(DataInput in) throws IOException {
		source = in.readInt();
		from = in.readInt();
		distance = in.readLong();
		numPaths = in.readLong();
		dependency = in.readDouble();
	}
	
	
	
	/* Getters and Setters */
	
	public long getDistance() {
		return distance;
	}
	public void setDistance(long distance) {
		this.distance = distance;
	}
	public int getSource() {
		return source;
	}
	public void setSource(int source) {
		this.source = source;
	}
	public int getFrom() {
		return from;
	}
	public void setFrom(int from) {
		this.from = from;
	}
	public double getDependency() {
		return dependency;
	}
	public void setDependency(double dependency) {
		this.dependency = dependency;
	}
	public long getNumPaths() {
		return numPaths;
	}
	public void setNumPaths(long numPaths) {
		this.numPaths = numPaths;
	}
	
	
	


	
	
	
}
