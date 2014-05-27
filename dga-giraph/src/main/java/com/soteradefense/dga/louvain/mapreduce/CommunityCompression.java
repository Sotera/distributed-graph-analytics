package com.soteradefense.dga.louvain.mapreduce;

import com.soteradefense.dga.louvain.giraph.LouvainVertexWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;


/**
 * Map reduce job to compresse a graph in such a way that each community is represetned by a single node.
 * 
 * input format:  see LouvainVertexOutputFormat
 * output format  see LouvainVertexInputFormat
 * *** the input to this job is output of the BSP computation, the output of this job is the input to the next stage of BSP.
 *
 */
public class CommunityCompression {



	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, LouvainVertexWritable> {

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, LouvainVertexWritable> output, Reporter reporter) throws IOException {

			String[] tokens = value.toString().trim().split("\t");
			if (3 > tokens.length){
				throw new IllegalArgumentException("Expected 4 cols: got "+tokens.length+"  from line: "+tokens.toString());
			}

			Text outKey = new Text(tokens[1]); // group by community
			String edgeListStr = (tokens.length == 3) ? "" : tokens[3];
			LouvainVertexWritable outValue = LouvainVertexWritable.fromTokens(tokens[2], edgeListStr);
			output.collect(outKey, outValue);
		}
	}



	public static class Reduce extends MapReduceBase implements Reducer<Text, LouvainVertexWritable, Text, Text> {

		public void reduce(Text key, Iterator<LouvainVertexWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String communityId = key.toString();
			long weight = 0;
			HashMap<String,Long> edgeMap = new HashMap<String,Long>();
			while (values.hasNext()){
				LouvainVertexWritable vertex = values.next();
				weight += vertex.getWeight();
				for (Entry<String, Long> entry : vertex.getEdges().entrySet()){
					String entryKey = entry.getKey();

					if (entryKey.equals(communityId)){
						weight += entry.getValue();
					}
					else if (edgeMap.containsKey(entryKey)){
						long w = edgeMap.get(entryKey) + entry.getValue();
						edgeMap.put(entryKey, w);
					}
					else{
						edgeMap.put(entry.getKey(), entry.getValue());
					}
				}
			}

			StringBuilder b = new StringBuilder();
			b.append(weight).append("\t");
			for (Entry<String, Long> entry : edgeMap.entrySet()){
				b.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
			}
			b.setLength(b.length() - 1);

			output.collect(new Text(key.toString()), new Text(b.toString()));

		}
	}



	public static void main(String [] args) throws Exception {
		JobConf conf = new JobConf(CommunityCompression.class);
		conf.setJobName("Louvain graph compression");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LouvainVertexWritable.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
	
}
