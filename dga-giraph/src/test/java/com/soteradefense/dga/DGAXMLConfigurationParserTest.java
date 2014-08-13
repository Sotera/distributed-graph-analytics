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
package com.soteradefense.dga;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.junit.Test;
import org.xml.sax.SAXParseException;

import static org.junit.Assert.assertEquals;

public class DGAXMLConfigurationParserTest {

    @Test
    public void testGoodXML() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "\n" +
                "<configuration>\n" +
                "    <system>\n" +
                "        <property>\n" +
                "            <name>giraph.zkList</name>\n" +
                "            <value>localhost:2181</value>\n" +
                "        </property>\n" +
                "    </system>\n" +
                "    <custom>\n" +
                "        <property>\n" +
                "            <name>mapred.task.timeout</name>\n" +
                "            <value>600000</value>\n" +
                "        </property>\n" +
                "    </custom>\n" +
                "    <giraph>\n" +
                "        <property>\n" +
                "            <name>w</name>\n" +
                "            <value>21</value>\n" +
                "        </property>\n" +
                "    </giraph>\n" +
                "</configuration>";
        byte [] xmlBytes = xml.getBytes();
        InputStream bais = new ByteArrayInputStream(xmlBytes);

        DGAConfiguration conf = DGAXMLConfigurationParser.parse(bais);
        assertEquals("localhost:2181", conf.getSystemProperties().get("giraph.zkList"));

        assertEquals(1, conf.getGiraphProperties().size());
        assertEquals(1, conf.getCustomArgumentProperties().size());

        assertEquals("21", conf.getGiraphProperties().get("-w"));
        assertEquals("600000", conf.getCustomArgumentProperties().get("mapred.task.timeout"));
    }

    @Test(expected=SAXParseException.class)
    public void testBadXML() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "\n" +
                "<configuration>\n" +
                "    <system>\n" +
                "        <property>\n" +
                "            <name>giraph.zkList</name>\n" +
                "            <value>localhost:2181</value>\n" +
                "        </property\n" +
                "    </system>\n" +
                "    <custom>\n" +
                "        <property>\n" +
                "            <name>mapred.task.timeout</name>\n" +
                "            <value>600000</value>\n" +
                "        </property>\n" +
                "    </custom>\n" +
                "    <giraph>\n" +
                "        <property>\n" +
                "            <name>w</name>\n" +
                "            <value>21</value>\n" +
                "        </property>\n" +
                "    </giraph>";
        byte [] xmlBytes = xml.getBytes();
        InputStream bais = new ByteArrayInputStream(xmlBytes);

        DGAConfiguration conf = DGAXMLConfigurationParser.parse(bais);
    }

    @Test
    public void testNoCustomProperties() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "\n" +
                "<configuration>\n" +
                "    <system>\n" +
                "        <property>\n" +
                "            <name>giraph.zkList</name>\n" +
                "            <value>localhost:2181</value>\n" +
                "        </property>\n" +
                "    </system>\n" +
                "    <giraph>\n" +
                "        <property>\n" +
                "            <name>w</name>\n" +
                "            <value>21</value>\n" +
                "        </property>\n" +
                "    </giraph>\n" +
                "</configuration>";
        byte [] xmlBytes = xml.getBytes();
        InputStream bais = new ByteArrayInputStream(xmlBytes);

        DGAConfiguration conf = DGAXMLConfigurationParser.parse(bais);
        assertEquals("localhost:2181", conf.getSystemProperties().get("giraph.zkList"));

        assertEquals(1, conf.getGiraphProperties().size());
        assertEquals(0, conf.getCustomArgumentProperties().size());

        assertEquals("21", conf.getGiraphProperties().get("-w"));
    }

    @Test
    public void testNoSystemProperties() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "\n" +
                "<configuration>\n" +
                "    <custom>\n" +
                "        <property>\n" +
                "            <name>mapred.task.timeout</name>\n" +
                "            <value>600000</value>\n" +
                "        </property>\n" +
                "    </custom>\n" +
                "    <giraph>\n" +
                "        <property>\n" +
                "            <name>w</name>\n" +
                "            <value>21</value>\n" +
                "        </property>\n" +
                "    </giraph>\n" +
                "</configuration>";
        byte [] xmlBytes = xml.getBytes();
        InputStream bais = new ByteArrayInputStream(xmlBytes);

        DGAConfiguration conf = DGAXMLConfigurationParser.parse(bais);

        assertEquals(1, conf.getGiraphProperties().size());
        assertEquals(1, conf.getCustomArgumentProperties().size());

        assertEquals("21", conf.getGiraphProperties().get("-w"));
        assertEquals("600000", conf.getCustomArgumentProperties().get("mapred.task.timeout"));
    }

    @Test
    public void testNoOverrideOfSystemProperties() throws Exception {
        System.setProperty("giraph.zkList", "notlocalhostatall:2181");
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "\n" +
                "<configuration>\n" +
                "    <system>\n" +
                "        <property>\n" +
                "            <name>giraph.zkList</name>\n" +
                "            <value>localhost:2181</value>\n" +
                "        </property>\n" +
                "    </system>\n" +
                "    <custom>\n" +
                "        <property>\n" +
                "            <name>mapred.task.timeout</name>\n" +
                "            <value>600000</value>\n" +
                "        </property>\n" +
                "    </custom>\n" +
                "    <giraph>\n" +
                "        <property>\n" +
                "            <name>w</name>\n" +
                "            <value>21</value>\n" +
                "        </property>\n" +
                "    </giraph>\n" +
                "</configuration>";
        byte [] xmlBytes = xml.getBytes();
        InputStream bais = new ByteArrayInputStream(xmlBytes);

        DGAConfiguration conf = DGAXMLConfigurationParser.parse(bais);
        assertEquals("localhost:2181", conf.getSystemProperties().get("giraph.zkList"));

        assertEquals(1, conf.getGiraphProperties().size());
        assertEquals(1, conf.getCustomArgumentProperties().size());

        assertEquals("21", conf.getGiraphProperties().get("-w"));
        assertEquals("600000", conf.getCustomArgumentProperties().get("mapred.task.timeout"));
    }

    @Test
    public void testNoGiraphProperties() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "\n" +
                "<configuration>\n" +
                "    <system>\n" +
                "        <property>\n" +
                "            <name>giraph.zkList</name>\n" +
                "            <value>localhost:2181</value>\n" +
                "        </property>\n" +
                "    </system>\n" +
                "    <custom>\n" +
                "        <property>\n" +
                "            <name>mapred.task.timeout</name>\n" +
                "            <value>600000</value>\n" +
                "        </property>\n" +
                "    </custom>\n" +
                "</configuration>";
        byte [] xmlBytes = xml.getBytes();
        InputStream bais = new ByteArrayInputStream(xmlBytes);

        DGAConfiguration conf = DGAXMLConfigurationParser.parse(bais);
        assertEquals("localhost:2181", conf.getSystemProperties().get("giraph.zkList"));

        assertEquals(0, conf.getGiraphProperties().size());
        assertEquals(1, conf.getCustomArgumentProperties().size());
        assertEquals("600000", conf.getCustomArgumentProperties().get("mapred.task.timeout"));
    }

    @Test
    public void testDisallowedGiraphProperties() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "\n" +
                "<configuration>\n" +
                "    <system>\n" +
                "        <property>\n" +
                "            <name>giraph.zkList</name>\n" +
                "            <value>localhost:2181</value>\n" +
                "        </property>\n" +
                "    </system>\n" +
                "    <custom>\n" +
                "        <property>\n" +
                "            <name>mapred.task.timeout</name>\n" +
                "            <value>600000</value>\n" +
                "        </property>\n" +
                "    </custom>\n" +
                "    <giraph>\n" +
                "        <property>\n" +
                "            <name>w</name>\n" +
                "            <value>21</value>\n" +
                "        </property>\n" +
                "        <property>\n" +
                "            <name>eip</name>\n" +
                "            <value>/path/to/disallowed/things</value>\n" +
                "        </property>\n" +
                "    </giraph>\n" +
                "</configuration>";
        byte [] xmlBytes = xml.getBytes();
        InputStream bais = new ByteArrayInputStream(xmlBytes);

        DGAConfiguration conf = DGAXMLConfigurationParser.parse(bais);
        assertEquals("localhost:2181", conf.getSystemProperties().get("giraph.zkList"));

        assertEquals(1, conf.getGiraphProperties().size());
        assertEquals(1, conf.getCustomArgumentProperties().size());

        assertEquals("21", conf.getGiraphProperties().get("-w"));
        assertEquals("600000", conf.getCustomArgumentProperties().get("mapred.task.timeout"));
    }

    @Test
    public void testAllGiraphPropertiesThatCanBePresent() throws Exception{
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "\n" +
                "<configuration>\n" +
                "    <system>\n" +
                "        <property>\n" +
                "            <name>giraph.zkList</name>\n" +
                "            <value>localhost:2181</value>\n" +
                "        </property>\n" +
                "    </system>\n" +
                "    <custom>\n" +
                "        <property>\n" +
                "            <name>mapred.task.timeout</name>\n" +
                "            <value>600000</value>\n" +
                "        </property>\n" +
                "    </custom>\n" +
                "    <giraph>\n" +
                "        <property>\n" +
                "            <name>w</name>\n" +
                "            <value>21</value>\n" +
                "        </property>\n" +
                "        <property>\n" +
                "            <name>q</name>\n" +
                "            <value></value>\n" +
                "        </property>\n" +
                "        <property>\n" +
                "            <name>yj</name>\n" +
                "            <value>/path/to/yarn/things</value>\n" +
                "        </property>\n" +
                "        <property>\n" +
                "            <name>yh</name>\n" +
                "            <value>1600</value>\n" +
                "        </property>\n" +
                "    </giraph>\n" +
                "</configuration>";
        byte [] xmlBytes = xml.getBytes();
        InputStream bais = new ByteArrayInputStream(xmlBytes);

        DGAConfiguration conf = DGAXMLConfigurationParser.parse(bais);
        assertEquals("localhost:2181", conf.getSystemProperties().get("giraph.zkList"));

        assertEquals(4, conf.getGiraphProperties().size());
        assertEquals(1, conf.getCustomArgumentProperties().size());

        assertEquals("21", conf.getGiraphProperties().get("-w"));
        assertEquals("1600", conf.getGiraphProperties().get("-yh"));
        assertEquals("/path/to/yarn/things", conf.getGiraphProperties().get("-yj"));
        assertEquals("", conf.getGiraphProperties().get("-q"));
        assertEquals("600000", conf.getCustomArgumentProperties().get("mapred.task.timeout"));
    }

}
