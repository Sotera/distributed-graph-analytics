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


import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.PrintStream;
import java.security.Permission;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DGACommandLineUtilTest {

    public SecurityManager defaultManager;
    protected static class ExitException extends SecurityException
    {
        public final int status;
        public ExitException(int status)
        {
            super("There is no escape!");
            this.status = status;
        }
    }

    private static class NoExitSecurityManager extends SecurityManager
    {
        @Override
        public void checkPermission(Permission perm)
        {
            // allow anything.
        }
        @Override
        public void checkPermission(Permission perm, Object context)
        {
            // allow anything.
        }
        @Override
        public void checkExit(int status)
        {
            super.checkExit(status);
            throw new ExitException(status);
        }
    }


    @Before
    public void setUp() throws Exception{
        defaultManager = System.getSecurityManager();
        System.setSecurityManager(new NoExitSecurityManager());
    }

    @Test(expected = ParseException.class)
    public void testBadCommandLineArguments() throws ParseException {
        String[] args = {"-ca", "highbetweenness.set.stability", "10", "-ca"};
        Options options = DGACommandLineUtil.generateOptions();
        DGAConfiguration conf = DGACommandLineUtil.parseCommandLine(args, options);
    }

    @Test
    public void testBadCommandLineArguments1() throws ParseException {
        String[] args = {"ca-", "highbetweenness.set.stability", "10"};
        Options options = DGACommandLineUtil.generateOptions();
        DGAConfiguration conf = DGACommandLineUtil.parseCommandLine(args, options);
        assertEquals(conf.getCustomArgumentProperties().size(), 0);
        assertEquals(conf.getGiraphProperties().size(), 0);
    }

    @Test(expected = ParseException.class)
    public void testBadCommandLineArguments2() throws ParseException {
        String[] args = {"-ca", "highbetweenness.set.stability", "10"};
        Options options = DGACommandLineUtil.generateOptions();
        DGAConfiguration conf = DGACommandLineUtil.parseCommandLine(args, options);
    }

    @Test
    public void testBadCommandLineArguments3() throws ParseException {
        String[] args = {"-ca", "highbetweenness.set.stability=10", "10", "-q", "-w", "10"};
        Options options = DGACommandLineUtil.generateOptions();
        DGAConfiguration conf = DGACommandLineUtil.parseCommandLine(args, options);
        assertEquals(conf.getCustomArgumentProperties().size(), 1);
        assertEquals(conf.getGiraphProperties().size(), 2);
    }

    @Test(expected = ParseException.class)
    public void testBadCommandLineArguments4() throws ParseException {
        String[] args = {"-ca", "highbetweenness.set.stability10", "10", "-q", "-w", "10"};
        Options options = DGACommandLineUtil.generateOptions();
        DGAConfiguration conf = DGACommandLineUtil.parseCommandLine(args, options);
        assertEquals(conf.getCustomArgumentProperties().size(), 1);
        assertEquals(conf.getGiraphProperties().size(), 2);
    }

    @Test
    public void testBadCommandLineArguments5() throws ParseException {
        String[] args = {"badargument"};
        Options options = DGACommandLineUtil.generateOptions();
        DGAConfiguration conf = DGACommandLineUtil.parseCommandLine(args, options);
        assertEquals(conf.getCustomArgumentProperties().size(), 0);
        assertEquals(conf.getGiraphProperties().size(), 0);
    }

    @Test
    public void testGoodCommandLineArguments() throws ParseException {
        String[] args = {"-ca", "highbetweenness.set.stability=10", "-ca", "highbetweenness.pivot.percent=50", "-q", "-w", "10"};
        Options options = DGACommandLineUtil.generateOptions();
        DGAConfiguration conf = DGACommandLineUtil.parseCommandLine(args, options);
        assertEquals(conf.getCustomArgumentProperties().size(), 2);
        assertEquals(conf.getGiraphProperties().size(), 2);
    }

    @Test
    public void testCustomDelimiter() throws ParseException {
        String[] args = {"-ca", "simple.edge.delimiter=\\t"};
        Options options = DGACommandLineUtil.generateOptions();
        DGAConfiguration conf = DGACommandLineUtil.parseCommandLine(args, options);
        assertEquals(conf.getCustomArgumentProperties().size(), 1);
        assertEquals(conf.getCustomArgumentProperties().get("simple.edge.delimiter"), "\t");
        assertEquals(conf.getGiraphProperties().size(), 0);
    }

    @Test
    public void testDCommandNoSpace() throws ParseException {
        String[] args = {"-ca", "simple.edge.delimiter=\\t", "-Dgiraph.zkList=localhost:2181"};
        Options options = DGACommandLineUtil.generateOptions();
        DGAConfiguration conf = DGACommandLineUtil.parseCommandLine(args, options);
        assertEquals(conf.getCustomArgumentProperties().size(), 1);
        assertEquals(conf.getSystemProperties().size(), 1);
        assertEquals(conf.getSystemProperties().get("giraph.zkList"), "localhost:2181");
        assertEquals(conf.getCustomArgumentProperties().get("simple.edge.delimiter"), "\t");
        assertEquals(conf.getGiraphProperties().size(), 0);
    }

    @Test(expected = ParseException.class)
    public void testCACommandNoSpace() throws ParseException {
        String[] args = {"-casimple.edge.delimiter=\\t", "-Dgiraph.zkList=localhost:2181"};
        Options options = DGACommandLineUtil.generateOptions();
        DGAConfiguration conf = DGACommandLineUtil.parseCommandLine(args, options);
        assertEquals(conf.getCustomArgumentProperties().size(), 1);
        assertEquals(conf.getSystemProperties().size(), 1);
        assertEquals(conf.getSystemProperties().get("giraph.zkList"), "localhost:2181");
        assertEquals(conf.getCustomArgumentProperties().get("simple.edge.delimiter"), "\t");
        assertEquals(conf.getGiraphProperties().size(), 0);
    }

    @Test(expected = ExitException.class)
    public void testHelpCommand() throws ParseException {
        String[] args = {"-h"};
        Options options = DGACommandLineUtil.generateOptions();
        DGAConfiguration conf = DGACommandLineUtil.parseCommandLine(args, options);

    }

    @Test
    public void testTooLittleArgs() throws ParseException {
        String[] args = {"wcc"};
        Options options = DGACommandLineUtil.generateOptions();
        DGAConfiguration conf = DGACommandLineUtil.parseCommandLine(args, options);
        assertEquals(conf.getGiraphProperties().size(), 0);
        assertEquals(conf.getSystemProperties().size(), 0);
        assertEquals(conf.getCustomArgumentProperties().size(), 0);
    }

    @After
    public void cleanUp(){
        System.setSecurityManager(defaultManager);
    }

}
