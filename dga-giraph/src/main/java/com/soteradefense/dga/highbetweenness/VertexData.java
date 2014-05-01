package com.soteradefense.dga.highbetweenness;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;


/**
 * Stores all required data for a vertex
 *   - Map of Sources to ShortestPathLists (for shortest path phase)
 *   - Map of Sources to PartialDependency (for pair dependency phase)
 *   - approximated betweenness value
 *   
 * @author Eric Kimbrel - Sotera Defense, eric.kimbrel@soteradefense.com
 *
 */
public class VertexData implements Writable{

	// map of source vertex to shortest path data
	private Map<Integer,ShortestPathList> pathDataMap = new HashMap<Integer,ShortestPathList>();
	
	// map of source vertex to pair dependency data
	private Map<Integer,PartialDependency> partialDepMap = new HashMap<Integer,PartialDependency>();
	
	// current approximated betweenness value 
	// ( equals actual betweenness when / if all vertices are used as sources) 
	private double approxBetweenness = 0.0;
	
	

	/**
	 * Add the new path to the list of shortest paths, if it is indeed a shortest path
	 * @param data - a shortest path message
	 * @return The updated ShortestPathList, or null if no update was made.
	 */
	public ShortestPathList addPathData(PathData data){
		int source = data.getSource();
		ShortestPathList list = getPathDataMap().get(source);
		
		// if the list was empty ad the first item and return
		if (null == list){
			list = new ShortestPathList(data);
			getPathDataMap().put(source,list);
			return list;
		}
		else{
			return (list.update(data)) ? list : null ;
		}	
		
	}
	
	/**
	 * Updates pair dependency data
	 * @param src the source vertex
	 * @param dep the partial dependency value of this vertex on the source
	 * @param successorDelta change in number of successors remaining to accumulate. -1 if accumulating a dependency, 1 if adding a successor.
	 * @return the PartialDependency object that was updated.
	 */
	public PartialDependency addPartialDep(int src, double dep, int successorDelta){
		PartialDependency current;
		if (partialDepMap.containsKey(src)){
			current = partialDepMap.get(src);
			current.addSuccessors(successorDelta);
			current.addDep(dep);
		}
		else{
			current = new PartialDependency(successorDelta,dep);
			partialDepMap.put(src, current);
		}
		return current;
	}
	
	
	
	// I/O
	
	public void write(DataOutput out) throws IOException {
		out.writeDouble(approxBetweenness);
		out.writeInt(pathDataMap.size());
		for (Entry<Integer,ShortestPathList> entry : pathDataMap.entrySet()){
			out.writeInt(entry.getKey());
			entry.getValue().write(out);
		}
		
		out.writeLong(this.partialDepMap.size());
		for (Entry<Integer,PartialDependency> entry : partialDepMap.entrySet()){
			out.writeInt(entry.getKey());
			entry.getValue().write(out);
		}
		
	}

	public void readFields(DataInput in) throws IOException {
		approxBetweenness = in.readDouble();
		
		// read the path data map
		pathDataMap.clear();
		int size = in.readInt();
		for (int i = 0; i < size; i++){
			int key = in.readInt();
			ShortestPathList list = new ShortestPathList();
			list.readFields(in);
			pathDataMap.put(key, list);
		}
	
		
		partialDepMap.clear();
		size = in.readInt();
		for (int i = 0; i < size; i ++){
			int src = in.readInt();
			PartialDependency dep = new PartialDependency();
			dep.readFields(in);
			partialDepMap.put(src, dep);
		}
		
	}

	
	// GETTERS / SETTERS
	
	public Map<Integer,PartialDependency> getPartialDepMap() {
		return partialDepMap;
	}

	public void setPartialDepMap(Map<Integer,PartialDependency> partialDepMap) {
		this.partialDepMap = partialDepMap;
	}

	public double getApproxBetweenness() {
		return approxBetweenness;
	}

	public void setApproxBetweenness(double approxBetweenness) {
		this.approxBetweenness = approxBetweenness;
	}
	
	public Map<Integer, ShortestPathList> getPathDataMap() {
		return pathDataMap;
	}

	public void setPathDataMap(Map<Integer, ShortestPathList> pathDataMap) {
		this.pathDataMap = pathDataMap;
	}
	

}
