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
import java.util.ArrayList;
import java.util.List;


/**
 * The state of a vertex.
 */
public class LouvainNodeState implements Writable {

    private String community = "";
    private long communitySigmaTotal;

    // the internal edge weight of a node
    // i.e. edges from the node to itself.
    private long internalWeight;

    // outgoing degree of the node
    private long nodeWeight;

    // 1 if the node has changed communities this cycle, otherwise 0
    private long changed = 0;

    // history of total change numbers, used to determine when to halt
    private List<Long> changeHistory;

    private boolean fromLouvainVertexReader = false;

    public LouvainNodeState() {
        this.changeHistory = new ArrayList<Long>();
        this.changed = 0L;
    }

    public String getCommunity() {
        return community;
    }

    public void setCommunity(String community) {
        this.community = community;
    }

    public long getCommunitySigmaTotal() {
        return communitySigmaTotal;
    }

    public void setCommunitySigmaTotal(long communitySigmaTotal) {
        this.communitySigmaTotal = communitySigmaTotal;
    }

    public long getInternalWeight() {
        return internalWeight;
    }

    public void setInternalWeight(long internalWeight) {
        this.internalWeight = internalWeight;
    }

    public long getChanged() {
        return changed;
    }

    public void setChanged(long changed) {
        this.changed = changed;
    }

    public List<Long> getChangeHistory() {
        return this.changeHistory;
    }

    public long getNodeWeight() {
        return nodeWeight;
    }

    public void setNodeWeight(long nodeWeight) {
        this.nodeWeight = nodeWeight;
    }

    public boolean isFromLouvainVertexReader() {
        return fromLouvainVertexReader;
    }

    public void setFromLouvainVertexReader(boolean fromLouvainVertexReader) {
        this.fromLouvainVertexReader = fromLouvainVertexReader;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        community = WritableUtils.readString(in);
        communitySigmaTotal = in.readLong();
        internalWeight = in.readLong();
        changed = in.readLong();
        nodeWeight = in.readLong();
        int historyLength = in.readInt();
        changeHistory = new ArrayList<Long>(2 * (historyLength + 1));
        for (int i = 0; i < historyLength; i++) {
            changeHistory.add(in.readLong());
        }

    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, community);
        out.writeLong(communitySigmaTotal);
        out.writeLong(internalWeight);
        out.writeLong(changed);
        out.writeLong(nodeWeight);
        out.writeInt(changeHistory.size());
        for (Long i : changeHistory) {
            out.writeLong(i);
        }

    }

}
