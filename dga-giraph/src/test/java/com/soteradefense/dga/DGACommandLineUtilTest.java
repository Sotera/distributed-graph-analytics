package com.soteradefense.dga;


import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DGACommandLineUtilTest {

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

}
