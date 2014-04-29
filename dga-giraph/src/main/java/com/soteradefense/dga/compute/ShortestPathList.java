package com.soteradefense.dga.compute;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

/**
 * Keeps Shortest path data for a single source vertex to a single target vertex.
 * 
 * Maintains Shortest path, predecessors, and number of shortest paths from the source
 * to each predecessor.
 * 
 * 
 * @author Eric Kimbrel - Sotera Defense, eric.kimbrel@soteradefense.com
 *
 */
public class ShortestPathList implements Writable{
	
	// distance from source to this vertex on the shortest path
	private long distance;
	
	// map of predecessor to number of shortest paths from source to that predecessor
	private Map<Integer,Long> predPathCountMap;
	
	
	
	/**
	 * Create a new shortest empty Path List
	 */
	public ShortestPathList(){
		distance = Long.MAX_VALUE;
		setPredPathCountMap(new HashMap<Integer,Long>());
	}
	
	
	/**
	 * Create a new shortest path list based on a 
	 * shortest path message.
	 * @param data
	 */
	public ShortestPathList(PathData data){
		this();
		this.distance = data.getDistance();
		predPathCountMap.put(data.getFrom(), data.getNumPaths());
	}
	
	
	/**
	 * @return The number of shortest paths from source to this vertex
	 */
	public long getNumShortestPaths(){
		long paths = 0L;
		for (long pred : predPathCountMap.values()){
			paths += pred;
		}
		return paths;
	}
	
	
	/**
	 * Update This shortest path list based on a new shortest path message
	 * @param data
	 * @return true if the ShortestPathList is modified in anyway, otherwise false.
	 */
	public boolean update(PathData data){
		if (data.getDistance() == this.distance){
			if (!this.predPathCountMap.containsKey(data.getFrom())){
				predPathCountMap.put(data.getFrom(),data.getNumPaths());
				return true;
			}
			else{
				long oldValue = predPathCountMap.get(data.getFrom());
				boolean update = oldValue != data.getNumPaths();
				if (update){
					predPathCountMap.put(data.getFrom(), data.getNumPaths());
				}
				return update;
			}
		}
		else if(data.getDistance() < this.distance){
			this.distance = data.getDistance();
			this.predPathCountMap.clear();
			predPathCountMap.put(data.getFrom(),data.getNumPaths());
			return true;
		}
		else{
			return false;
		}
	}

	
	
	// I/ O
	
	public void write(DataOutput out) throws IOException {
		out.writeLong(distance);
		out.writeInt(this.predPathCountMap.size());
		for (Entry<Integer,Long> entry: predPathCountMap.entrySet()){
			out.writeInt(entry.getKey());
			out.writeLong(entry.getValue());
		}

	}

	
	public void readFields(DataInput in) throws IOException {
		distance = in.readLong();
		int size = in.readInt();
		this.predPathCountMap.clear();
		for (int i = 0; i < size; i++){
			predPathCountMap.put(in.readInt(), in.readLong());
		}
	}

	
	
	// GETTERS / SETTERS
	
	public long getDistance() {
		return distance;
	}

	public void setDistance(long distance) {
		this.distance = distance;
	}

	public Map<Integer,Long> getPredPathCountMap() {
		return predPathCountMap;
	}

	public void setPredPathCountMap(Map<Integer,Long> predPathCountMap) {
		this.predPathCountMap = predPathCountMap;
	}
	
}