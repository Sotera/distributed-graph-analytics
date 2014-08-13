/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *       http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.soteradefense.dga.louvain.giraph;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * messages sent between vertices.
 */
public class LouvainMessage implements Writable {

    private String communityId;
    private long communitySigmaTotal;
    private long edgeWeight;
    private String sourceId;

    public LouvainMessage(String communityId, long sigmaTotal, long weight, String sourceId) {
        this.communityId = communityId;
        this.communitySigmaTotal = sigmaTotal;
        this.edgeWeight = weight;
        this.sourceId = sourceId;
    }

    public LouvainMessage() {
        this("", 0, 0, "");
    }

    public LouvainMessage(LouvainMessage other) {
        this(other.communityId, other.communitySigmaTotal, other.edgeWeight, other.sourceId);
    }

    public String getCommunityId() {
        return communityId;
    }

    public void setCommunityId(String l) {
        this.communityId = l;
    }

    public long getCommunitySigmaTotal() {
        return communitySigmaTotal;
    }

    public void setCommunitySigmaTotal(long communitySigmaTotal) {
        this.communitySigmaTotal = communitySigmaTotal;
    }

    public void addToSigmaTotal(long partial) {
        this.communitySigmaTotal += partial;
    }

    public long getEdgeWeight() {
        return edgeWeight;
    }

    public void setEdgeWeight(long edgeWeight) {
        this.edgeWeight = edgeWeight;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        communityId = WritableUtils.readString(in);
        communitySigmaTotal = in.readLong();
        edgeWeight = in.readLong();
        sourceId = WritableUtils.readString(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, communityId);
        out.writeLong(communitySigmaTotal);
        out.writeLong(edgeWeight);
        WritableUtils.writeString(out, sourceId);
    }

}
