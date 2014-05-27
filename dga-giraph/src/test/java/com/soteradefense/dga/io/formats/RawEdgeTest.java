package com.soteradefense.dga.io.formats;

import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class RawEdgeTest {

    private RawEdge rawEdge;

    @Before
    public void setup() {
        rawEdge = new RawEdge(",", "");
    }

    @Test
    public void testGoodData() throws IOException {
        String goodRow = "fancy.email@gmail.com,commonemail@comcast.net,4";
        rawEdge.fromText(new Text(goodRow));
        assertEquals("fancy.email@gmail.com", rawEdge.getSourceId());
        assertEquals("commonemail@comcast.net", rawEdge.getTargetId());
        assertEquals("4", rawEdge.getEdgeValue());
    }

    @Test
    public void testManyFields() throws IOException {
        String goodRow = "fancy.email@gmail.com,commonemail@comcast.net,4,5,6,7";
        rawEdge.fromText(new Text(goodRow));
        assertEquals("fancy.email@gmail.com", rawEdge.getSourceId());
        assertEquals("commonemail@comcast.net", rawEdge.getTargetId());
        assertEquals("4", rawEdge.getEdgeValue());
    }

    @Test(expected=IOException.class)
    public void testTooFewFields() throws IOException {
        String goodRow = "fancy.email@gmail.com";
        rawEdge.fromText(new Text(goodRow));
    }

    @Test
    public void testOtherDelimiter() throws IOException {
        String goodRow = "fancy.email@gmail.com\tcommonemail@comcast.net\t4";
        RawEdge tabEdge = new RawEdge("\t","");
        tabEdge.fromText(new Text(goodRow));
        assertEquals("fancy.email@gmail.com", tabEdge.getSourceId());
        assertEquals("commonemail@comcast.net", tabEdge.getTargetId());
        assertEquals("4", tabEdge.getEdgeValue());
    }

    @Test
    public void testForDefault() throws IOException {
        String goodRow = "fancy.email@gmail.com,commonemail@comcast.net";
        rawEdge.fromText(new Text(goodRow));
        assertEquals("fancy.email@gmail.com", rawEdge.getSourceId());
        assertEquals("commonemail@comcast.net", rawEdge.getTargetId());
        assertEquals("", rawEdge.getEdgeValue());
    }

}
