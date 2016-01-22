package com.soteradefense.dga.BfsTree;

import com.soteradefense.dga.DGALoggingUtil;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.IntWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.giraph.aggregators.IntOverwriteAggregator;

/**
 * Created by ekimbrel on 9/24/15.
 *
 * For algorithim description see BfsTreeComputation.
 *
 * The job of the master compute class in the BFS tree computation is to set the initial search key
 * in a global aggregator before super step 0.
 *
 *
 */
public class BfsTreeMasterCompute extends DefaultMasterCompute {


    private static final Logger logger = LoggerFactory.getLogger(BfsTreeMasterCompute.class);
    public static final String SEARCH_KEY_AGG = "com.soteradefense.dga.BfsTree.searchKeyAggregator";
    public static final String SEARCH_KEY_CONF = "dga.bfstree.searchkey";
    private int searchKey;


    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        DGALoggingUtil.setDGALogLevel(this.getConf());
        registerAggregator(SEARCH_KEY_AGG,IntOverwriteAggregator.class);
        searchKey = Integer.parseInt(getConf().get(SEARCH_KEY_CONF,"-1"));
        if (searchKey == -1){
            throw new IllegalArgumentException("Search Key value must be set to a postive integer.  set -ca "+SEARCH_KEY_CONF+"=<value> when running BfsTree");
        }
    }

    @Override
    public void compute() {
        if (0 == this.getSuperstep()){
            setAggregatedValue(SEARCH_KEY_AGG,new IntWritable(searchKey));
        }
    }


}
