package com.soteradefense.dga.hbse;

public class HBSEConfigurationConstants {
    /**
     * Configuration Identifier for the directory to output the highbetweenness set.
     */
    public static final String BETWEENNESS_OUTPUT_DIR = "betweenness.output.dir";

    /**
     * Configuration Identifier for the number of shortest path phases to run through.
     */
    public static final String BETWEENNESS_SHORTEST_PATH_PHASES = "betweenness.shortest.path.phases";

    /**
     * Configuration Identifier for the set stability cut off point (margin of error).
     */
    public static final String BETWEENNESS_SET_STABILITY = "betweenness.set.stability";

    /**
     * Configuration Identifier for the set stability counter cut off point (margin of error).
     */
    public static final String BETWEENNESS_SET_STABILITY_COUNTER = BETWEENNESS_SET_STABILITY + ".counter";


    /**
     * Configuration Identifier for the maximum number of nodes in the betweenness set.
     */
    public static final String BETWEENNESS_SET_MAX_SIZE = "betweenness.set.maxSize";

    /**
     * Configuration Identifier for the pivot point batch size as a percent integer.
     */
    public static final String PIVOT_BATCH_SIZE = "pivot.batch.size";

    /**
     * Configuration Identifier for the initial pivot point batch size as a percent integer.
     */
    public static final String PIVOT_BATCH_SIZE_INITIAL = PIVOT_BATCH_SIZE + ".initial";

    /**
     * Configuration Identifier for the random seed value when choosing new pivot points.
     */
    public static final String PIVOT_BATCH_RANDOM_SEED = "pivot.batch.random.seed";

    /**
     * Configuration Identifier for the number of vertices to perform the operation on.
     */
    public static final String VERTEX_COUNT = "vertex.count";

    /**
     * Configuration Identifier for the default file system.
     */
    public static final String FS_DEFAULT_FS = "fs.defaultFS";

    /**
     * Configuration Identifier for the default name.
     */
    public static final String FS_DEFAULT_NAME = "fs.default.name";

    /**
     * This is the filename for the final highbetweenness set
     */
    public static final String FINAL_SET_CSV = "final_set.csv";

    /**
     * The filename where the stats are written
     */
    public static final String STATS_CSV = "stats.csv";
}
