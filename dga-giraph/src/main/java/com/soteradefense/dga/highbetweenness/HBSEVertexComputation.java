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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import com.soteradefense.dga.highbetweenness.HBSEMasterCompute.State;


/**
 * Calculates Shortest paths, and accumulates pair dependency information.
 * 
 * Use with SBMasterCompute to find an approximated set of vertices with highest
 * betweenness centrality.
 * 
 * For background information on the approximation method see:
 * "W. Chong, Efficient Extraction of High-Betweenness Vertices"
 * 
 * For background information on the method of accumulation of pair dependencies and shortest path data see:
 *  "U. Brandes, A Faster Algorithm for Betweenness Centrality"
 * 
 * @author Eric Kimbrel - Sotera Defense, eric.kimbrel@soteradefense.com
 *
 */
public class HBSEVertexComputation extends AbstractComputation<IntWritable,VertexData,IntWritable,PathData,PathData> {

	private static final Log LOG = LogFactory.getLog(HBSEVertexComputation.class);
	
	/**
	 * Works in two major stages which are repeated and coordinated
	 * by the setting of a global state (set by the master compute class)
	 * 
	 * Stage 1:  Discover shortest paths, and shortest path counts for each
	 *   source vertex in a globally set pivot batch to each node in the graph.
	 * Stage 2:  Accumulate pair dependencies 
	 * 
	 */
	@Override
	public void compute(Vertex<IntWritable, VertexData, IntWritable> vertex,
			Iterable<PathData> messages) throws IOException {
		
		long step = this.getSuperstep();
		int id = vertex.getId().get();
		int updateCount = 0;
		State state = getCurrentGlobalState();
		
		if (step == 0 ){
			vertex.setValue(new VertexData());
		}
		
		VertexData vertexValue = vertex.getValue();
		
		// Start a shortest path phase
		// if this vertex is a source (pivot) send shortest path messages to neighbors
		
		if (State.SHORTEST_PATH_START == state /*|| State.SHORTEST_PATH_RUN == state*/){
			if (getPivotBatch().contains(id)){
				LOG.info("Superstep: "+step+" Start new shortest path computation. Source = "+id);
				for(Edge<IntWritable,IntWritable> edge : vertex.getEdges()){
					sendMessage(edge.getTargetVertexId(), PathData.getShortestPathMessage(id,id,edge.getValue().get(),1));
				}
				vertexValue.addPathData(PathData.getShortestPathMessage(id, id, 0, 1L));
				this.aggregate(HBSEMasterCompute.UPDATE_COUNT_AGG, new IntWritable(1));
			}
			
		}
		
		
		// continue a shortest path phase, continues until no shortest paths are updated.
		if (State.SHORTEST_PATH_RUN == state){
			
			Map<Integer,ShortestPathList> updatedPathMap = new HashMap<Integer,ShortestPathList>();
			
			// process incoming messages
			for (PathData message: messages){
				ShortestPathList updatedPath = vertexValue.addPathData(message);
				if (updatedPath != null){
					updatedPathMap.put(message.getSource(), updatedPath);
				}	
			}
			
			// send outgoing messages for each updated shortest path
			for (Entry<Integer,ShortestPathList> entry : updatedPathMap.entrySet()){
				ShortestPathList spl = entry.getValue();
				int src = entry.getKey();
				long numPaths = spl.getNumShortestPaths();
				updateCount++;
				for(Edge<IntWritable,IntWritable> edge : vertex.getEdges()){
					long newDistance = spl.getDistance() + edge.getValue().get();
					this.sendMessage(edge.getTargetVertexId(), PathData.getShortestPathMessage(src,id,newDistance,numPaths));
				}
			}
			
			this.aggregate(HBSEMasterCompute.UPDATE_COUNT_AGG, new IntWritable(updateCount));
		}
		
		
		// Start of the dependency accumulation phase, each vertex must determine if it has any successors
		if (State.PAIR_DEPENDENCY_PING_PREDECESSOR == state){
			StringBuilder builder = new StringBuilder();
			// for each shortest path send a message with that source.
			for (Entry<Integer,ShortestPathList> entry: vertexValue.getPathDataMap().entrySet()){
				int source = entry.getKey();
				long distance = entry.getValue().getDistance();
				if (distance > 0){ // exclude this vertex
					for (int pred : entry.getValue().getPredPathCountMap().keySet()){
						this.sendMessage(new IntWritable(pred), PathData.getPingMessage(source));
						builder.append("(").append(pred).append(",").append(source).append("),");
					}	
				}
				
			}
			if (builder.length() > 1) builder.deleteCharAt(builder.length()-1);
			LOG.trace("ID: "+id+" Step: "+step+" State: "+state+" sent messages (pred,srouce):  "+builder.toString());
		}
		
		
		// process ping messages form PAIR_DEPENDENCY_PING_PREDECESSOR to determine if 
		// this vertex has any successors
		// vertices with no successors will begin the pair dependency accumulation process.
		if (State.PAIR_DEPENDENCY_FIND_SUCCESSORS == state){
			Set<Integer> successorExists = new HashSet<Integer>();
			
			for (PathData message: messages){
				int src = message.getSource();
				successorExists.add(src);
				vertexValue.addPartialDep(src, 0.0, 1);  // for every successor message ad one to the partial dep count
			}
			
			StringBuilder builder = new StringBuilder();
			Set<Integer> allPaths = vertexValue.getPathDataMap().keySet();
			allPaths.remove(vertex.getId());
			Set<Integer> noSuccessor = new HashSet<Integer>();
			for (int src : allPaths){
				if (!successorExists.contains(src)){
					noSuccessor.add(src);
					builder.append(src).append(",");
				}
			}
			
			// for any sources that this vertex has no successors, start the dependency accumulation chain
			if (noSuccessor.size() > 0){
				for (int src : noSuccessor){
					ShortestPathList spl = vertexValue.getPathDataMap().get(src);
					long numPaths = spl.getNumShortestPaths();
					double dep = 0;
					for (int pred : spl.getPredPathCountMap().keySet()){
						this.sendMessage(new IntWritable(pred), PathData.getDependencyMessage(src,dep,numPaths));
					}
				}
				noSuccessor.clear();
			}
			
			if (builder.length() > 1) builder.deleteCharAt(builder.length()-1);
			LOG.trace("ID: "+id+" Step: "+step+" State: "+state+" set noSuccessor "+builder.toString());
			
		}
		
		
		// continue the pair dependency phase until no updates are done / all dependencies are accumulated.
		if (State.PAIR_DEPENDENCY_RUN == state){
			
			for (PathData message: messages){
				int src = message.getSource();
				
				if (src == id){
					continue; // don't process paths for which you are the source
				}
				
				double successorDep = message.getDependency();
				long successorNumPaths = message.getNumPaths();
				long numPaths = vertexValue.getPathDataMap().get(src).getNumShortestPaths();
				double partialDep = ((double)numPaths/successorNumPaths) * ( 1 + successorDep );
				LOG.debug("ID: "+id+" Step: "+step+" message {src:"+src+" successorPaths:"+successorNumPaths+" successorDep:"+successorDep+"} calculated {paths:"+numPaths+", dep:"+partialDep+"}");
				
				// accumulate the dependency and subtract one successor
				PartialDependency partialSum = vertexValue.addPartialDep(src, partialDep,-1);
				
				// if the successor count reaches 0 pass this vertex dependency on to predecessors
				if (partialSum.getSuccessors() == 0){
					ShortestPathList spl = vertexValue.getPathDataMap().get(src);
					StringBuilder builder = new StringBuilder();
					this.aggregate(HBSEMasterCompute.UPDATE_COUNT_AGG, new IntWritable(1));
					for (int pred : spl.getPredPathCountMap().keySet()){
						this.sendMessage(new IntWritable(pred), PathData.getDependencyMessage(src,partialSum.getDependency(),numPaths));
						builder.append(pred).append(",");
					}
					if (builder.length() > 1) builder.deleteCharAt(builder.length()-1);
					LOG.debug("ID: "+id+" Step: "+step+" forwarding partial dep to predecessors ("+builder.toString()+") {src:"+src+", paths:"+numPaths+", dep:"+partialSum.getDependency()+"}");
				}
			}
		}
		
		
		// completed dependency accumulation. calculate current betweenness value and clear all other vertex data
		// to prepare for next stage (which will be SHORTEST_PATH_START or the computation will halt, based on high betweeness
		// set stability, as determined in the master compute class.
		if (State.PAIR_DEPENDENCY_COMPLETE == state){
			double approxBetweenness = vertexValue.getApproxBetweenness();
			for (PartialDependency partialDep : vertexValue.getPartialDepMap().values()){
				approxBetweenness += partialDep.getDependency();
			}
			vertexValue.setApproxBetweenness(approxBetweenness);
			vertexValue.getPartialDepMap().clear();
			vertexValue.getPathDataMap().clear();
			this.aggregate(HBSEMasterCompute.HIGH_BC_SET_AGG, getNewHighBetweennessList(id,approxBetweenness));
		}
		
		
	}
	
	
	
	/**
	* Return the current global state 
	* @return State
	*/
	private State getCurrentGlobalState(){
		  IntWritable stateInt = this.getAggregatedValue(HBSEMasterCompute.STATE_AGG);
		  return State.values()[stateInt.get()];
	}
	
	
	/**
	 * Read the global pivot batch:  the set of vertices that are to be used
	 * as sources in this phase.
	 * @return
	 */
	
	private Set<Integer> getPivotBatch(){
		IntArrayWritable iwa = this.getAggregatedValue(HBSEMasterCompute.PIVOT_AGG);
		Writable[] wa = iwa.get();
		Set<Integer> batch = new HashSet<Integer>();
		for (Writable iw : wa){
			int pivot = ((IntWritable) iw).get();
			batch.add(pivot);
		}
		return batch;
	}
	
	
	/**
	 * Get a new HighBetweennessList object, configured with the betweenness.set.maxSize option
	 * from the job conf. If not set size will default to 1.
	 * @param id
	 * @param value
	 * @return an empty HighBetweenessList object.
	 */
	public HighBetweennessList getNewHighBetweennessList(int id, double value){
		  int size = 1;
		  try{
				size = Integer.parseInt(getConf().get("betweenness.set.maxSize"));
			} catch (NumberFormatException e){
				LOG.error("betweenness.set.maxSize must be set to a valid int.");
				throw e;
			}
			return new HighBetweennessList(size,id,value);
	}



}
