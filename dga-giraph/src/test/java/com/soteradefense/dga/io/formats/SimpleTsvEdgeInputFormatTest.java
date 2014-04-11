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

import com.google.common.collect.Maps;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SimpleTsvEdgeInputFormatTest {

    @Test
    public void testEdgesWithoutWeight() throws Exception {
        String[] edges = new String[]{
                "1\t2",
                "1\t3",
                "1\t4",
                "1\t5",
                "1\t6",
                "2\t3",
                "2\t4",
                "2\t5",
                "2\t1",
                "3\t4",
                "3\t5",
                "3\t20",
                "3\t1",
                "3\t2",
                "4\t5",
                "4\t1",
                "4\t2",
                "4\t3",
                "5\t1",
                "5\t2",
                "5\t3",
                "5\t4",
                "6\t1",
                "6\t8",
                "8\t6",
                "8\t10",
                "10\t8",
                "10\t12",
                "12\t10",
                "15\t17",
                "15\t19",
                "17\t15",
                "19\t15",
                "19\t20",
                "20\t3",
                "20\t19"
        };

    }

    public void testEdgesWithWeight() {

    }

    @Test(expected = IOException.class)
    public void testEdgesWithInvalidWeight() {

    }

    public void testEdgesWithWeightInSomeRowsAndNoWeightInOthers() {

    }

    @Test(expected = IOException.class)
    public void testInvalidEdges() {

    }

    public void testEdgesWithConfiguredDelimiter() {

    }

    public void testEdgesWithConfiguredDefaultWeight() {

    }

    @Test(expected = IOException.class)
    public void testEdgesWithInvalidConfiguredDefaultWeight() throws IOException {

    }

    private static Map<String, String> parseResults(Iterable<String> results) {
        Map<String, String> values = Maps.newHashMap();
        for (String line : results) {
            String[] tokens = line.split("\\s+");
            values.put(tokens[0], tokens[1]);
        }
        return values;
    }

}
