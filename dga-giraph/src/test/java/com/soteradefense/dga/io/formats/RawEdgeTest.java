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
package com.soteradefense.dga.io.formats;

import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class RawEdgeTest {

    private RawEdge rawEdge;

    @Test
    public void testGoodData() throws IOException {
        rawEdge = new RawEdge(",", "");
        String goodRow = "fancy.email@gmail.com,commonemail@comcast.net,4";
        rawEdge.fromText(new Text(goodRow));
        assertEquals("fancy.email@gmail.com", rawEdge.getSourceId());
        assertEquals("commonemail@comcast.net", rawEdge.getTargetId());
        assertEquals("4", rawEdge.getEdgeValue());
    }

    @Test
    public void testManyFields() throws IOException {
        rawEdge = new RawEdge(",", "");
        String goodRow = "fancy.email@gmail.com,commonemail@comcast.net,4,5,6,7";
        rawEdge.fromText(new Text(goodRow));
        assertEquals("fancy.email@gmail.com", rawEdge.getSourceId());
        assertEquals("commonemail@comcast.net", rawEdge.getTargetId());
        assertEquals("4", rawEdge.getEdgeValue());
    }

    @Test(expected=IOException.class)
    public void testTooFewFields() throws IOException {
        rawEdge = new RawEdge(",", "");
        String badRow = "fancy.email@gmail.com";
        rawEdge.fromText(new Text(badRow));
    }

    @Test(expected=IOException.class)
    public void testBadData() throws IOException{
        rawEdge = new RawEdge(",","");
        String badData = "asdfhwuhufnwsk";
        rawEdge.fromText(new Text(badData));
    }

    @Test(expected=IOException.class)
    public void testEmptyFields() throws IOException {
        rawEdge = new RawEdge(",", "");
        String badRow = "fancy.email@gmail.com,,";
        rawEdge.fromText(new Text(badRow));
    }

    @Test
    public void testOtherDelimiter() throws IOException {
        rawEdge = new RawEdge("\t", "");
        String goodRow = "fancy.email@gmail.com\tcommonemail@comcast.net\t4";
        rawEdge.fromText(new Text(goodRow));
        assertEquals("fancy.email@gmail.com", rawEdge.getSourceId());
        assertEquals("commonemail@comcast.net", rawEdge.getTargetId());
        assertEquals("4", rawEdge.getEdgeValue());
    }

    @Test
    public void testForDefault() throws IOException {
        rawEdge = new RawEdge(",", "");
        String goodRow = "fancy.email@gmail.com,commonemail@comcast.net";
        rawEdge.fromText(new Text(goodRow));
        assertEquals("fancy.email@gmail.com", rawEdge.getSourceId());
        assertEquals("commonemail@comcast.net", rawEdge.getTargetId());
        assertEquals("", rawEdge.getEdgeValue());
    }

    @Test
    public void testMultipleDelimiters() throws IOException {
        rawEdge = new RawEdge("asdf", "");
        String goodData = "AaSsDdFf10";
        rawEdge.fromText(new Text(goodData));
        assertEquals("A", rawEdge.getSourceId());
        assertEquals("S", rawEdge.getTargetId());
        assertEquals("D", rawEdge.getEdgeValue());
    }
}
