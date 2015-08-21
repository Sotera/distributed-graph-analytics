package com.soteradefense.dga.scan1;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.ArrayPrimitiveWritable;

/**
 * Created by ekimbrel on 8/21/15.
 */
public class MaxScanAggregator extends BasicAggregator<ArrayPrimitiveWritable>{
    @Override
    public void aggregate(ArrayPrimitiveWritable value) {
        ArrayPrimitiveWritable aggregatedValue = getAggregatedValue();
        int[] scanIdValue = (int[]) value.get();
        int[] oldIdValue = (int[])  aggregatedValue.get();

        if (scanIdValue[1] > oldIdValue[1]){
            aggregatedValue.set(value.get());
        }
    }

    @Override
    public ArrayPrimitiveWritable createInitialValue() {
        return new ArrayPrimitiveWritable(new int[]{0,0});
    }

}
