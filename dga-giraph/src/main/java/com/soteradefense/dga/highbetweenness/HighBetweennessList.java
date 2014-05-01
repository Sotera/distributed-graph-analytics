package com.soteradefense.dga.highbetweenness;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.hadoop.io.Writable;


/**
 * Maintains a list of the top N items(an item is defined as an int id, and double value), ranked by a double value.
 * Designed for use with the giraph Aggregator pattern.
 * 
 * @author Eric Kimbrel - Sotera Defense, eric.kimbrel@soteradefense.com
 */
public class HighBetweennessList implements Writable{

	/**
	 *  Container class to store an id and value 
	 */
	class BcTuple{
		int id;
		double value;
		
		public BcTuple(int id, double value){
			this.id = id;
			this.value = value;
		}
	}
	
	/**
	 * BcTuple Comparator
	 * Used to order based on value.
	 */
	public static Comparator<BcTuple> comparator = new Comparator<BcTuple>(){
		public int compare(BcTuple arg0, BcTuple arg1) {
			return (arg0.value < arg1.value) ? -1 : (arg0.value > arg1.value) ? 1 : 0;
		}
		
	};
	
	
	// Max number of BcTuples to keep
	private int maxSize;
	
	private PriorityQueue<BcTuple> highBetweennessQueue;
	
	
	// CONSTRUCTORS
	
	public HighBetweennessList(){
		maxSize = 1;
		highBetweennessQueue = new PriorityQueue<BcTuple>(1,comparator);
	}
	
	
	public HighBetweennessList(int maxSize){
		this();
		this.maxSize = maxSize;
		highBetweennessQueue = new PriorityQueue<BcTuple>(maxSize,comparator);
	}
	
	
	public HighBetweennessList(int id, double value){
		this();
		maxSize = 1;
		highBetweennessQueue = new PriorityQueue<BcTuple>(1,comparator);
		highBetweennessQueue.add(new BcTuple(id,value));
		
	}
	
	public HighBetweennessList(int maxSize, int id, double value){
		this();
		this.maxSize = maxSize;
		highBetweennessQueue = new PriorityQueue<BcTuple>(maxSize,comparator);
		highBetweennessQueue.add(new BcTuple(id,value));
		
	}
	
	
	/**
	 * Add items from other to this.  Keeping only the top N (maxSize) items.
	 * @param other
	 */
	public void aggregate(HighBetweennessList other){
		if (other.maxSize > maxSize){
			maxSize = other.maxSize;
		}
		
		for (BcTuple t : other.getQueue()){
			if (highBetweennessQueue.size() < maxSize){
				highBetweennessQueue.add(t);
			}
			else{
				BcTuple first = highBetweennessQueue.peek();
				if (first.value < t.value){
					highBetweennessQueue.poll();
					highBetweennessQueue.add(t);
				}
			}
		}
	}
	
	
	/**
	 * @return the ids of the stored items, as a set.
	 */
	public Set<Integer> getHighBetweennessSet(){
		Set<Integer> set = new HashSet<Integer>();
		for (BcTuple t: highBetweennessQueue){
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
		if (highBetweennessQueue != null){
			for (BcTuple t: highBetweennessQueue){
				out.writeInt(t.id);
				out.writeDouble(t.value);
			}
		}
	}

	
	/**
	 * Read fields
	 */
	public void readFields(DataInput in) throws IOException {
		maxSize = in.readInt();
		highBetweennessQueue = new PriorityQueue<BcTuple>(maxSize,comparator);
		int size = in.readInt();
		for (int i = 0; i < size; i++){
			highBetweennessQueue.add(new BcTuple(in.readInt(),in.readDouble()));
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
	public String toString(){
		StringBuilder b = new StringBuilder();
		b.append("{maxSize: ").append(maxSize).append(", high betweenness set: ");
		b.append("[");
		if (this.highBetweennessQueue != null){
			for (BcTuple t : highBetweennessQueue){
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
	 * @param maxSize
	 */
	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}

}
