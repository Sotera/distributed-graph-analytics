package com.soteradefense.dga.BfsTree;

import com.soteradefense.dga.DGALoggingUtil;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 *
 * Starting from a provided search key (vertex ID) we create a BFS tree from the graph.
 *
 * All vertex ids must be > 0 for valid results due to default vertex values being set to 0 and
 * vertices with no output edges aren't loaded in step 0, so their default vertex (parent id) value can
 * not be reliable set to -1.  Instead we must check for parent values < 1 and set them explicitly to -1 at each step.
 *
 * input:  int value csv.
 *
 * Created by ekimbrel on 9/24/15.
 */
public class BfsTreeComputation extends BasicComputation<IntWritable, IntWritable, NullWritable, IntWritable> {

    private static final Logger logger = LoggerFactory.getLogger(BfsTreeComputation.class);

    @Override
    public void initialize(GraphState graphState, WorkerClientRequestProcessor<IntWritable, IntWritable, NullWritable> workerClientRequestProcessor, GraphTaskManager<IntWritable, IntWritable, NullWritable> graphTaskManager, WorkerGlobalCommUsage workerGlobalCommUsage, WorkerContext workerContext) {
        super.initialize(graphState, workerClientRequestProcessor, graphTaskManager, workerGlobalCommUsage, workerContext);
        DGALoggingUtil.setDGALogLevel(this.getConf());
    }

    @Override
    public void compute(Vertex<IntWritable, IntWritable, NullWritable> vertex, Iterable<IntWritable> messages) throws IOException {

        long step = getSuperstep();
        int thisVertexId = vertex.getId().get();
        if (thisVertexId < 1){
            throw new IllegalStateException("Invalid vertex id:  all ids must be > 0 for bfs-tree-computation");
        }


        // on step 0 send original messages from root node to its adjacent nodes.
        if (0 == step){

            // default all parent values to -1
            vertex.setValue(new IntWritable(-1));

            // get the search key from the global aggregator (set by BfsTreeMasterCompute)
            int searchKey = ( (IntWritable) getAggregatedValue(BfsTreeMasterCompute.SEARCH_KEY_AGG)).get();

            // if the search key matches this vertex set the partent to itself
            // and send out thisVertexId to all adjacent nodes.
            if (searchKey == thisVertexId){
                vertex.setValue(new IntWritable(thisVertexId));
                this.sendMessageToAllEdges(vertex,vertex.getId());
            }

        }

        /*
            on each step after step 0:
                if this node gets any messages:
                    if this nodes parent is -1
                        set its parent to the first message value
                        send out this nodes id to all adjacent nodes
        */

        else {

            // 0 can be used as a default value for verticies that aren't loaded until later in the computation due to have no out edges
            // as a result we must check and replace any 0's with -1, and all graph vertices must have id >= 1
            if (vertex.getValue().get() < 1){
                vertex.setValue(new IntWritable(-1));
            }

            int thisParentId = vertex.getValue().get();
            if (-1 == thisParentId){
                // to ensure consistent results on multiple runs take the max value
                int maxValue = -1;
                for (IntWritable message: messages) maxValue = Math.max(maxValue,message.get());

                if (maxValue > -1){
                    vertex.setValue(new IntWritable(maxValue));
                    sendMessageToAllEdges(vertex,vertex.getId());
                }
            }
        }

        // all nodes vote to halt after every super step.
        vertex.voteToHalt();

    }
}
