package com.soteradefense.dga.pagerank;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;


public class PageRankCompute extends BasicComputation<Text, DoubleWritable, NullWritable, DoubleWritable> {

    public static final String MAX_EPSILON = "com.soteradefense.dga.MAX_EPSILON";
    public static final String DAMPING_FACTOR = "damping.factor";
    public static final float DAMPING_FACTOR_DEFAULT_VALUE = 0.85f;


    @Override
    public void compute(Vertex<Text, DoubleWritable, NullWritable> vertex, Iterable<DoubleWritable> messages) throws IOException {

        float dampingFactor = this.getConf().getFloat(DAMPING_FACTOR, DAMPING_FACTOR_DEFAULT_VALUE);

        long step = getSuperstep();

        // count the total number of nodes.
        if (step == 0) {
            //set initial value
            vertex.setValue(new DoubleWritable(1.0 / getTotalNumVertices()));
        } else { // go until no one votes to continue

            double rank = 0;
            for (DoubleWritable partial : messages) {
                rank += partial.get();
            }
            rank = ((1 - dampingFactor) / getTotalNumVertices()) + (dampingFactor * rank);
            double delta = Math.abs(rank - vertex.getValue().get()) / vertex.getValue().get();
            aggregate(MAX_EPSILON, new DoubleWritable(delta));
            vertex.setValue(new DoubleWritable(rank));
        }
        distributeRank(vertex);
    }

    private void distributeRank(Vertex<Text, DoubleWritable, NullWritable> vertex) {
        double rank = vertex.getValue().get() / vertex.getNumEdges();
        sendMessageToAllEdges(vertex, new DoubleWritable(rank));
    }

}
