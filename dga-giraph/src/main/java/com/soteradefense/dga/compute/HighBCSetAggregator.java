package com.soteradefense.dga.compute;

import org.apache.giraph.aggregators.BasicAggregator;

/**
 *  Aggregator to accumulate a list of the top N vertices, ranked by approximated betweenness value.
 *  
 * @author Eric Kimbrel - Sotera Defense, eric.kimbrel@soteradefense.com
 *
 */
public class HighBCSetAggregator extends BasicAggregator<HighBetweennessList>{

	
	public void aggregate(HighBetweennessList value) {
		if (value == null || value.getMaxSize() == -1) return;
		HighBetweennessList old = getAggregatedValue();
		old.aggregate(value);	
	}

	public HighBetweennessList createInitialValue() {
		return new HighBetweennessList();
	}
	
	

}
