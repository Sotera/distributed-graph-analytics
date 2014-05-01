package com.soteradefense.dga.highbetweenness;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.aggregators.IntOverwriteAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;


/**
 * Coordinates Global state for the SBVertex to calculate shortest paths, accumulates pair dependency information,
 * and monitor high betweenness set stability.
 * 
 * Required configuration settings
 * 
 * fs.defaultFS OR fs.default.name:  If not set in the environment you can set them as a custom arguments. typically this will not need to be set
 * betweenness.output.dir: Directory in HDFS used to write the high betweenness set.
 * betweenness.set.stability: Integer value, algorithm completes with the high betweenness set changes by less than this value, checked after each cycle.
 * betweenness.set.stability.counter: Integer value, number of times the stability threshold must be reached.
 * betweenness.set.maxSize: Size the result set desired.
 * pivot.batch.size: Number of pivots to use in each batch
 * pivot.batch.size.initial:  Number of pivots to use in the first bacth (defaults to pivot.batch.size)
 * vertex.count: The number of vertices to be loaded 
 * betweenness.shortestpath.phases: Number of shortest path phases to run for every 1 dependency accumulation phase.
 *
 * WARNING:  id values for all vertices must be in 0,1,2,...N where N=vertex.count-1
 * 
 * 
 * For background information on the approximation method see:
 * "W. Chong, Efficent Extraction of High-Betweenness Vertices"
 * 
 * For background information on the method of accumulation of pair dependencies and shortest path data see:
 *  "U. Brandes, A Faster Algorithm for Betweenness Centrality"
 * 
 * @author Eric Kimbrel - Sotera Defense, eric.kimbrel@soteradefense.com
 *
 */
public class HBSEMasterCompute extends DefaultMasterCompute{

	private static final Log LOG = LogFactory.getLog(HBSEMasterCompute.class);
	
	// aggregator identifiers
	public static final String STATE_AGG = "com.sotera.graph.singbetweenness.STATE_AGG";
	public  static final String PIVOT_AGG = "com.sotera.graph.singbetweenness.PIVOT_AGG";
	public  static final String UPDATE_COUNT_AGG = "com.sotera.graph.singbetweenness.UPDATE_COUNT_AGG";
	public static final String HIGH_BC_SET_AGG = "com.sotera.graph.singbetweenness.HIGH_BC_SET_AGG";
	
	// number of pivots to use per batch
	private int batchSize;
	private int initialBatchSize;
	
	// number of shortest path phases per dependency accumulation phase.
	private int shortestPathPhases;
	private int shortestPathPhasesCompleted;
	
	// total number of vertices / highest vertex id value
	private int maxId;
	
	// pivots being used as sources in the current phase
	Queue<Integer> currentPivots = new LinkedList<Integer>();
	
	// pivots that have been used in past phases
	Set<Integer> previousPivots = new HashSet<Integer>();
	
	// current set of highest betweenness nodes
	Set<Integer> highBetweennessSet = new HashSet<Integer>();
	
	// limit on the high betweenness set size
	int maxHighBCSetSize;
	
	// current cycle: a cyle is defined here as a shortest path phase + a pair dependency phase
	int cycle = 1;
	
	
	// algorithm is complete when high betweenness set changes by less than this amount after a full cycle
	// cutoff must be met stabilityCounter times prior to exit.
	int stabilityCutoff;
	int stabilityCounter;
	int counter = 0;
	
	// where to store the output (a location in hdfs)
	String outputDir;
	
	
	// Global states to coordinate computation
	public enum State {
		START,
		SHORTEST_PATH_START,
		SHORTEST_PATH_RUN,
		PAIR_DEPENDENCY_PING_PREDECESSOR,
		PAIR_DEPENDENCY_FIND_SUCCESSORS,
		PAIR_DEPENDENCY_RUN,
		PAIR_DEPENDENCY_COMPLETE,
		FINISHED;
	}
	private State state = State.START;
	
	private Random random;
	
	private Date start;
	private Date end;
	
	/**
	 * Read options from configuration file and set up aggregators (global communication)
	 */
	@Override
    public void initialize() throws InstantiationException, IllegalAccessException {
		start = new Date();
		state = State.START;
		this.registerPersistentAggregator(STATE_AGG, IntOverwriteAggregator.class);
		this.registerPersistentAggregator(PIVOT_AGG, IntArrayOverwriteAggregator.class);
		this.registerAggregator(UPDATE_COUNT_AGG, IntSumAggregator.class);
		this.registerAggregator(HIGH_BC_SET_AGG, HighBCSetAggregator.class);
		
		
		String defaultFS = this.getDefaultFS(this.getConf());
		if (defaultFS == null){
			throw new IllegalArgumentException("fs.defaultFS OR fs.default.name must be set.  If not set in the environemtn you can set them as a custom argument to the giraph job via -ca fs.default.name=<your default fs>");
		}
		
		outputDir = getConf().get("betweenness.output.dir");
		if (outputDir == null || outputDir.length() < 1){
			throw new IllegalArgumentException("betweenness.output.dir must be set to a valid directory in HDFS");
		}
		
		this.shortestPathPhases = 1;
		this.shortestPathPhasesCompleted = 0;
		String shortestPathPhasesStr = getConf().get("betweenness.shortestpath.phases");
		try{
			if (shortestPathPhasesStr != null) shortestPathPhases = Integer.parseInt(shortestPathPhasesStr);
		} catch(NumberFormatException e){
			LOG.error("betweenness.shortestpath.phases not set to valid int. default=1");
		}
		
		
		stabilityCutoff = 0;
		String stabilityCutoffStr = getConf().get("betweenness.set.stability");
		if (null != stabilityCutoffStr){
			try{
				stabilityCutoff = Integer.parseInt(stabilityCutoffStr);
			} catch (NumberFormatException e){
				LOG.error("betweenness.set.stability must be set to a valid int. default="+stabilityCutoff);
				throw e;
			}
		}
		LOG.info("betweenness.set.stability="+stabilityCutoff);
		
		stabilityCounter = 3;
		String stabilityCounterStr = getConf().get("betweenness.set.stability.counter");
		if (null != stabilityCutoffStr){
			try{
				stabilityCounter = Integer.parseInt(stabilityCounterStr);
			} catch (NumberFormatException e){
				LOG.error("betweenness.set.stability.counter must be set to a valid int. default="+stabilityCounter);
				throw e;
			}
		}
		LOG.info("betweenness.set.stability.counter="+stabilityCounter);
		
		maxHighBCSetSize = 1;
		try{
			maxHighBCSetSize = Integer.parseInt(getConf().get("betweenness.set.maxSize"));
		} catch (NumberFormatException e){
			LOG.error("betweenness.set.maxSize must be set to a valid int.");
			throw e;
		}
		LOG.info("betweenness.set.maxSize="+maxHighBCSetSize);
	
		
		
		// manually set first pivot batch if argument is present
		try{
			String pivotBatchStr = this.getConf().get("pivot.batch.string");
			if (null != pivotBatchStr && pivotBatchStr.length() > 0){
				String [] pivotBatchArray = pivotBatchStr.split(",");
				for (String pivotStr : pivotBatchArray){
					int pivot = Integer.parseInt(pivotStr);
					currentPivots.add(pivot);
					LOG.info("Manually added pivot: "+pivot);
				}
			}
			
		} catch (NumberFormatException e){
			LOG.error("Optional argument pivot.batch.string invalid. Must be a comma seperated list of ints.");
			throw e;
		}
		if (!currentPivots.isEmpty()) setGlobalPivots(currentPivots);
		
		
		String batchSizeStr = this.getConf().get("pivot.batch.size");
		try{
			batchSize = Integer.parseInt(batchSizeStr);
		}
		catch (NumberFormatException e){
			LOG.error("Required option not set or invalid. \"pivot.batch.size\" must be set to a valid int, was set to: "+batchSizeStr);
			throw e;
		}
		LOG.info("pivot.batch.size="+batchSize);
		
		initialBatchSize = batchSize;
		try{
			String initialBatchSizeStr = getConf().get("pivot.batch.size.initial");
			if (initialBatchSizeStr != null) initialBatchSize = Integer.parseInt(initialBatchSizeStr);
		}
		catch(NumberFormatException e){
			LOG.error("Optional setting pivot.batch.size.initial set to invalid value, using default");
		}
		
		
		String randomSeedStr = getConf().get("pivot.batch.random.seed");
		if (null == randomSeedStr){
			random = new Random();
		}
		else{
			long seed = Long.parseLong(randomSeedStr);
			random = new Random(seed);
			LOG.info("Set random seed: "+seed);
		}
		
		
		String maxIdStr = this.getConf().get("vertex.count");
		try{
			maxId = Integer.parseInt(maxIdStr);
		}
		catch (NumberFormatException e){
			LOG.error("Required option not set or invalid. \"vertex.count\" must be set to a valid int, was set to: "+maxIdStr);
			throw e;
		}
		LOG.info("vertex.count="+maxId);
		
	}

	  
	  
	  
	  
	  /**
	   * Coordinates the computation phases of SBVertex by monitoring for the completion of each state
	   * and moving to the next state.
	   *   -- selects pivots
	   *   -- monitors for completion of shortest paths
	   *   -- starts pair dependency phase
	   *   -- monitors for completion of pair dependency
	   *   -- checks high betweenness set stability
	   *   -- if set is stable
	   *   --     save set and exit
	   *   -- else 
	   *   --     select new pivots and start new shortest path phase
	   *   
	   */
	  @Override
	  public void compute(){
		  long step = this.getSuperstep();
		  LOG.info("Superstep: "+step+" starting in State: "+state);
		  
		  if (State.START == state){
			  if (currentPivots.isEmpty()){
				  int currentBatchSize = (step == 0) ? this.initialBatchSize : this.batchSize;
				  choosePivots(currentBatchSize);
			  }
			  state = State.SHORTEST_PATH_START;
			  setGlobalState(state);
			  LOG.info("Superstep: "+step+" Switched to State: "+state);
			  return;		  
		  }
		  
		  
		  else if (State.SHORTEST_PATH_START == state){
			  int updateCount = ( (IntWritable) this.getAggregatedValue(UPDATE_COUNT_AGG)).get();
			  LOG.info("Superstep: "+step+" Paths updated: "+updateCount);
			  state = State.SHORTEST_PATH_RUN;
			  setGlobalState(state);
			  LOG.info("Superstep: "+step+" Switched to State: "+state);
		  }
		  
		  else if (State.SHORTEST_PATH_RUN == state){
			  int updateCount = ( (IntWritable) this.getAggregatedValue(UPDATE_COUNT_AGG)).get();
			  LOG.info("Superstep: "+step+" Paths updated: "+updateCount);
			  if (updateCount == 0){
				  shortestPathPhasesCompleted++;
				  if (shortestPathPhasesCompleted == shortestPathPhases){
					  state = State.PAIR_DEPENDENCY_PING_PREDECESSOR;
				  }
				  else{
					  choosePivots(this.batchSize);
					  state = State.SHORTEST_PATH_START;
				  }
				  setGlobalState(state);
				  LOG.info("Superstep: "+step+" UPDATE COUNT 0, shortest path phase "+shortestPathPhasesCompleted+" of "+shortestPathPhases+" Switched to State: "+state);
			  }
		  }
		  
		  else if (State.PAIR_DEPENDENCY_PING_PREDECESSOR == state){
			  shortestPathPhasesCompleted = 0;
			  state = State.PAIR_DEPENDENCY_FIND_SUCCESSORS;
			  setGlobalState(state);
			  LOG.info("Superstep: "+step+" Switched to State: "+state);
		  }
		  
		  else if (State.PAIR_DEPENDENCY_FIND_SUCCESSORS == state){
			  state = State.PAIR_DEPENDENCY_RUN;
			  setGlobalState(state);
			  LOG.info("Superstep: "+step+" Switched to State: "+state);
		  }
		  
		  
		  else if (State.PAIR_DEPENDENCY_RUN == state){
			  int updateCount = ( (IntWritable) this.getAggregatedValue(UPDATE_COUNT_AGG)).get();
			  if (updateCount == 0){
				  state = State.PAIR_DEPENDENCY_COMPLETE;
				  setGlobalState(state);
			  }
			  LOG.info("Superstep: "+step+" UPDATE COUNT "+updateCount+", State: "+state);	 
		  }
		  
		  else if (State.PAIR_DEPENDENCY_COMPLETE == state){
			  HighBetweennessList hbl= getAggregatedValue(HIGH_BC_SET_AGG);
			  Set<Integer> incomingSet = hbl.getHighBetweennessSet();
			  int delta = this.compareHighBetweennessSet(incomingSet);
			  highBetweennessSet = incomingSet;
			  
			  LOG.info("High Betweenness Set Delta: "+delta);
			  String logprefix = "Finished Cycle: "+cycle;
			  cycle++;
			  
			  if (delta <= stabilityCutoff){
				  counter++;
				  if (counter >= stabilityCounter){
					  LOG.info(logprefix+" Set Delta < cutoff value; counter="+counter+" approximation complete.");
					  state = State.FINISHED;
				  }
				  else{
					  LOG.info(logprefix+" Set Delta < cutoff value; counter="+counter);
					  choosePivots(this.batchSize);
					  state = State.SHORTEST_PATH_START;
				  }
				  
			  }
			  else if (currentPivots.size() + previousPivots.size() == this.maxId){
				  LOG.info(logprefix+" All possible pivots selected, exiting");
				  state = State.FINISHED;
			  }
			  else{
				  counter = 0; // reset counter
				  LOG.info(logprefix+" Delta did not meet cutoff, starting next cycle.");
				  choosePivots(this.batchSize);
				  state = State.SHORTEST_PATH_START;
			  }
			  setGlobalState(state);
			  LOG.info("Superstep: "+step+", going to State: "+state);	
		  }
		  
		  
		  else if(State.FINISHED == state){
			  this.haltComputation();
			  end = new Date();
			  this.writeHighBetweennessSet(highBetweennessSet);
			  this.writeStats();
		  }
		  
		  else{
			  LOG.error("INVALID STATE: "+state);
			  throw new IllegalStateException("Invalid State"+state);
		  } 
		  
	  }
	  
	  
	  
	  /**
	   * Populate currentPivots with a new batch of pivots. Set the value globally with an aggregator
	   */
	  private void choosePivots(int currentBatchSize){
		  LOG.info("Selecting new pivots.");
		  previousPivots.addAll(currentPivots);
		  currentPivots.clear();
		  
		  StringBuilder b = new StringBuilder();
		  b.append("[");
		  
		  int attempt = 0;
		  while (currentPivots.size() < currentBatchSize && (previousPivots.size() + currentPivots.size()) < maxId){
			  attempt++;
			  int pivot = random.nextInt(maxId);
			  if (!previousPivots.contains(pivot)){
				  currentPivots.add(pivot);
				  b.append(pivot).append(",");
			  }
		  }
		  b.deleteCharAt(b.length()-1);
		  b.append("]");
		  LOG.info("Pivot selection complete. Took "+attempt+" attempts.");
		  LOG.info("Pivot set: "+b.toString());
		  setGlobalPivots(currentPivots);
	  }
	  
	 
	  
	  private void writeStats(){
		  
		  int pivotsSelected = this.currentPivots.size()+this.previousPivots.size();
          double percentSelected = (double) pivotsSelected / this.maxId;
		  int time = (int) ( (end.getTime() - start.getTime()) / 1000);
          
		  String defaultFS = getDefaultFS(getConf());
		  String filename = defaultFS+"/"+outputDir+"/stats.csv";
          Path pt = new Path(filename);
          try {
                  FileSystem fs = FileSystem.get(new Configuration());
                  BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
                  br.write("k: "+this.highBetweennessSet.size()+"\n");
                  br.write("delta p: "+this.batchSize+"\n");
                  br.write("cutoff: "+this.stabilityCutoff+"\n");
                  br.write("counter: "+this.stabilityCounter+"\n");
                  br.write("pivots selected: "+pivotsSelected+"\n");
                  br.write("percent of graph selected: "+percentSelected+"\n");
                  br.write("supsersteps: "+this.getSuperstep()+"\n");
                  br.write("cycles: "+this.cycle+"\n");
                  br.write("run time: "+time+"\n");
                  
                  
                  br.close();
          } catch (IOException e) {
                  e.printStackTrace();
                  throw new IllegalStateException("Could not write to file: "+filename);
          }
	  }
	  
	  /**
	   * Set pivots globally in the pivot aggregator
	   * @param pivots
	   */
	  private void setGlobalPivots(Collection<Integer> pivots){
		  IntWritable[] batch = new IntWritable[pivots.size()];
		  int i = 0;
		  for (int pivot : pivots){
			  batch[i++] = new IntWritable(pivot);
		  }
		  this.setAggregatedValue(PIVOT_AGG, new IntArrayWritable(batch));	  
	  }
	  
	  
	  /**
	   * Write the high betweenness set to a file in hdfs
	   * @param set
	   */
	  private void writeHighBetweennessSet(Set<Integer> set){
          String defaultFS = getDefaultFS(getConf());
		  String filename = defaultFS+"/"+outputDir+"/final_set.csv";
          Path pt = new Path(filename);
          try {
                  FileSystem fs = FileSystem.get(new Configuration());
                  BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
                  for (int id : set){
                	  br.write(id+"\n");
                  }
                  br.close();
          } catch (IOException e) {
                  e.printStackTrace();
                  throw new IllegalStateException("Could not write to file: "+filename);
          }

  }
	  
	  
	  /**
	   * Set the value of the state aggregator
	   * @param state
	   */
	  private void setGlobalState(State state){
		  this.setAggregatedValue(STATE_AGG, new IntWritable(state.ordinal()));
	  }
	  
	  
	  /**
	   * Find the number of changes in the high betweenness set.
	   * @param incomingSet
	   * @return the number of changes in the high betweenness set.
	   */
	  private int compareHighBetweennessSet(Set<Integer> incomingSet){
		  int diff = 0;
		  for (int id : incomingSet){
			  if (!this.highBetweennessSet.contains(id)){
				  diff++;
			  }
		  }
		  return diff;
	  }
	  
	  
	  /**
	   * Get the default file system. used to create a valid hdfs path
	   * @param conf
	   * @return
	   */
	  private String getDefaultFS(Configuration conf) {
          return (conf.get("fs.defaultFS") != null ? conf.get("fs.defaultFS") : conf.get("fs.default.name"));
      }
	  
	 
	
	
	
	
}
