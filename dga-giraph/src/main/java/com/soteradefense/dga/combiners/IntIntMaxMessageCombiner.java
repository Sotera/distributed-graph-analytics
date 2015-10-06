package com.soteradefense.dga.combiners;


import org.apache.hadoop.io.IntWritable;
import org.apache.giraph.combiner.MessageCombiner;

/**
 * For IntWriteable Vertex Ids sending IntWritable messages where only a single message per vertex
 * is desired.  Simply use the message with the highest value, and throw away others.
 *
 * Created by ekimbrel on 9/24/15.
 *
 */
public class IntIntMaxMessageCombiner implements MessageCombiner<IntWritable,IntWritable> {

    public void combine(IntWritable vertexId, IntWritable originalMessage, IntWritable messageToCombine) {
        int original = originalMessage.get();
        int other = messageToCombine.get();
        if (other > original){
            originalMessage.set(other);
        }
    }


    public IntWritable createInitialMessage() {
        return new IntWritable(Integer.MIN_VALUE);
    }
}
