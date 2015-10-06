package com.soteradefense.dga.triangles;

import org.apache.giraph.combiner.MessageCombiner;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * Created by ekimbrel on 9/23/15.
 */
public class TriangleCountCombiner implements MessageCombiner<IntWritable,ArrayPrimitiveWritable>{

    public void combine(IntWritable vertexId, ArrayPrimitiveWritable originalMessage, ArrayPrimitiveWritable messageToCombine){
        int [] original = (int []) originalMessage.get();
        int[] other = (int []) messageToCombine.get();
        int[] combined = new int[ original.length + other.length];
        int i = 0;
        for (int source: original) combined[i++] = source;
        for (int source: other) combined[i++] = source;
        originalMessage.set( combined );
    }

    public ArrayPrimitiveWritable createInitialMessage(){
        return new ArrayPrimitiveWritable(new int[0]);
    }
}
