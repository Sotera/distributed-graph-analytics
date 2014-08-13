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

package com.soteradefense.dga.hbse;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;


public class PivotList implements Writable {

    private List<String> pivots;

    public PivotList() {
        super();
        pivots = new LinkedList<String>();
    }

    public PivotList(String s) {
        this();
        pivots.add(s);
    }

    public void aggregate(PivotList other) {
        if(!pivots.containsAll(other.pivots))
            pivots.addAll(other.pivots);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(pivots.size());
        for (String s : pivots) {
            Text.writeString(dataOutput, s);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        pivots.clear();

        int size = dataInput.readInt();
        for (int i = 0; i < size; ++i) {
            pivots.add(Text.readString(dataInput));
        }
    }

    public List<String> getPivots() {
        return pivots;
    }

    public void trim(int totalLength) {
        pivots = pivots.subList(0, totalLength);
    }
}
