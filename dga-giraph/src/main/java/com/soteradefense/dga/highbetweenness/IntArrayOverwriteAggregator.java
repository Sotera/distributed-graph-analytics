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
