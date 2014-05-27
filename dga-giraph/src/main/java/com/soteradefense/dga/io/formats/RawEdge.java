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

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.StringTokenizer;

public class RawEdge {

    private final String delimiter;
    private final String defaultEdgeValue;

    private String sourceId;
    private String targetId;
    private String edgeValue;

    public RawEdge(final String delimiter, final String defaultEdgeValue) {
        this.delimiter = delimiter;
        this.defaultEdgeValue = defaultEdgeValue;
    }

    public void fromText(final Text line) throws IOException {
        StringTokenizer tokenizer = new StringTokenizer(line.toString(), this.delimiter);
        if (tokenizer.countTokens() < 2) {
            throw new IOException("Line of text contained only " + tokenizer.countTokens() + " tokens.  This input format requires at least a sourceId and a targetId, with an optional edgeValue");
        }
        this.sourceId = tokenizer.nextToken();
        this.targetId = tokenizer.nextToken();
        this.edgeValue = tokenizer.hasMoreTokens() ? tokenizer.nextToken() : defaultEdgeValue;
    }

    public String getSourceId() {
        return sourceId;
    }

    public String getTargetId() {
        return targetId;
    }

    public String getEdgeValue() {
        return edgeValue;
    }
}
