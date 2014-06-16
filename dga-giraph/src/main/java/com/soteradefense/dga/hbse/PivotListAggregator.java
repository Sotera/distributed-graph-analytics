package com.soteradefense.dga.hbse;

import org.apache.giraph.aggregators.BasicAggregator;


public class PivotListAggregator extends BasicAggregator<PivotList> {
    @Override
    public void aggregate(PivotList value) {
        PivotList current = getAggregatedValue();
        current.aggregate(value);
    }

    @Override
    public PivotList createInitialValue() {
        return new PivotList();
    }
}
