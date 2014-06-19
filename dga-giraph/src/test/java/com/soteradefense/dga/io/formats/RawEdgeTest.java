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
