/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.soteradefense.dga.io.formats;

import com.soteradefense.dga.louvain.giraph.LouvainNodeState;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Reads in a graph from text file in hdfs. Required format in is a tab delimited
 * file with 3 columns: <code>id<tab>internal weight<tab>edge list
 * 
 * the edge list is a comma separated list of edges of the form id:weight
 * 
 * The graph must be bi-directional i.e. if vertex 1 has edge 2:9, the vertex 2
 * must have id 1:9 This condition is not verified as the input is read, but
 * results of the algorithm will not be correct, and the run may fail with
 * exceptions.
 * 
 * 
 */
public class LouvainVertexInputFormat extends TextVertexInputFormat<Text, LouvainNodeState, LongWritable> {

	@Override
	public TextVertexReader createVertexReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
		return new LouvainVertexReader();
	}

	protected class LouvainVertexReader extends TextVertexReader {

		@Override
		public Vertex<Text, LouvainNodeState, LongWritable> getCurrentVertex() throws IOException, InterruptedException {

			String line = getRecordReader().getCurrentValue().toString();
			String[] tokens = line.trim().split("\t");
			if (tokens.length < 2) {
				throw new IllegalArgumentException("Invalid line: (" + line + ")");
			}
			LouvainNodeState state = new LouvainNodeState();
			Text id = new Text(tokens[0]);
			state.setCommunity(id.toString());
			state.setInternalWeight(Long.parseLong(tokens[1]));

			long sigmaTotal = 0;
			Map<Text, LongWritable> edgeMap = new HashMap<Text, LongWritable>();
			ArrayList<Edge<Text, LongWritable>> edgesList = new ArrayList<Edge<Text, LongWritable>>();
			String[] edges = (tokens.length > 2) ? tokens[2].split(",") : new String[0];
			for (int i = 0; i < edges.length; i++) {
				if (edges[i].indexOf(':') != -1) {
					String[] edgeTokens = edges[i].split(":");
					if (edgeTokens.length != 2) {
						throw new IllegalArgumentException("invalid edge (" + edgeTokens[i] + ") in line (" + line + ")");
					}
					long weight = Long.parseLong(edgeTokens[1]);
                    sigmaTotal += weight;
					Text edgeKey = new Text(edgeTokens[0]);
					edgeMap.put(edgeKey, new LongWritable(weight));
					// edgesList.add(EdgeFactory.create(new
					// LongWritable(edgeKey),new LongWritable(weight)));
				} else {
					Text edgeKey = new Text(tokens[i]);
					Long weight = 1L;
                    sigmaTotal += weight;
					edgeMap.put(edgeKey, new LongWritable(weight));
					// edgesList.add(EdgeFactory.create(new
					// LongWritable(edgeKey),new LongWritable(weight)));
				}

			}
			state.setCommunitySigmaTotal(sigmaTotal + state.getInternalWeight());
			state.setNodeWeight(sigmaTotal);

			for (Map.Entry<Text, LongWritable> entry : edgeMap.entrySet()) {
				edgesList.add(EdgeFactory.create(entry.getKey(), entry.getValue()));
			}

			Vertex<Text, LouvainNodeState, LongWritable> vertex = this.getConf().createVertex();
			vertex.initialize(id, state, edgesList);

			return vertex;

		}

		@Override
		public boolean nextVertex() throws IOException, InterruptedException {
			return getRecordReader().nextKeyValue();
		}

	}

}
