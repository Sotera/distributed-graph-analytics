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

import java.io.IOException;

import com.soteradefense.dga.highbetweenness.VertexData;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


/**
 * Writes the approximated betweenness value for each vertex
 * 
 * @author Eric Kimbrel - Sotera Defense, eric.kimbrel@soteradefense.com
 *
 */
public class HBSEOutputFormat extends TextVertexOutputFormat<IntWritable,VertexData,IntWritable>{

	@Override
	public TextVertexWriter createVertexWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new SBVertexWriter();
	}

	public class SBVertexWriter extends TextVertexWriter{

		public void writeVertex(
				Vertex<IntWritable, VertexData, IntWritable> vertex)
				throws IOException, InterruptedException {
			
			double approxBC = vertex.getValue().getApproxBetweenness();
			getRecordWriter().write(new Text(Integer.toString(vertex.getId().get())), new Text(Double.toString(approxBC)));
		}
		
	}

}
