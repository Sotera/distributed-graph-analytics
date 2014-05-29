package com.soteradefense.dga;

import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DGAConfigurationUtilTest {

    private InputStream inputStream;
    private String[] arguments;
    private Map<String, String> minimalDefaultGiraphParameters;
    private Map<String, String> minimalDefaultCustomArgumentParameters;
    private Map<String, String> requiredGiraphAnalyticParameters;
    private Map<String, String> requiredCustomArgumentAnalyticParameters;

    @Before
    public void setup() throws UnsupportedEncodingException {
        this.minimalDefaultGiraphParameters = new HashMap<String, String>();
        this.minimalDefaultCustomArgumentParameters = new HashMap<String, String>();
        this.requiredGiraphAnalyticParameters = new HashMap<String, String>();
        this.requiredCustomArgumentAnalyticParameters = new HashMap<String, String>();
        this.minimalDefaultGiraphParameters.put("w", "1");
        this.minimalDefaultCustomArgumentParameters.put("betweenness.set.stability.counter", "4");

        this.requiredGiraphAnalyticParameters.put("eif", "com.soteradefense.dga.DummyInputFormat");
        this.requiredGiraphAnalyticParameters.put("eof", "com.soteradefense.dga.DummyOutputFormat");

        this.requiredCustomArgumentAnalyticParameters.put("betweenness.set.stability", "2");

        inputStream = ClassLoader.getSystemResourceAsStream("dga-properties-good.xml");

        arguments = new String[]{"-w", "24", "-q", "-ca", "pivot.batch.size.initial=10", "-ca", "vertex.count=4",
                "-ca", "betweenness.set.maxSize=100", "-ca", "betweenness.set.stability=12", "-ca", "betweenness.set.stability.counter=3",
                "-ca", "betweenness.output.dir=/path/to/things", "-ca", "pivot.batch.string=chanterellemushrooms"};

    }

    @Test
    public void testGoodXML() throws Exception {
        DGAConfigurationUtil.updateConfigurationFromFileByType("giraph", this.minimalDefaultGiraphParameters, inputStream);
        assertEquals(1, this.minimalDefaultGiraphParameters.size());
        assertEquals("21", this.minimalDefaultGiraphParameters.get("w"));

        inputStream = ClassLoader.getSystemResourceAsStream("dga-properties-good.xml");

        DGAConfigurationUtil.updateConfigurationFromFileByType("custom", this.minimalDefaultCustomArgumentParameters, inputStream);
        assertEquals(2, this.minimalDefaultCustomArgumentParameters.size());
        assertEquals("4", this.minimalDefaultCustomArgumentParameters.get("betweenness.set.stability.counter"));
        assertEquals("600000", this.minimalDefaultCustomArgumentParameters.get("mapred.task.timeout"));
    }

    @Test
    public void testBadXML() throws Exception {

    }

    @Test(expected = Exception.class)
    public void testMalformedCommandLine1() throws Exception {
        String[] malformedArgs = {"-q", "-ca", "-ca", "betweenness.set.stability=1"};
        DGAConfigurationUtil.updateCustomArgumentConfigurationFromCommandLine(this.minimalDefaultCustomArgumentParameters, malformedArgs);
    }

    @Test(expected = Exception.class)
    public void testMalformedCommandLine2() throws Exception {
        String[] malformedArgs = {"-q", "-ca", "betweenness.set.stability1"};
        DGAConfigurationUtil.updateCustomArgumentConfigurationFromCommandLine(this.minimalDefaultCustomArgumentParameters, malformedArgs);
    }

    @Test(expected = Exception.class)
    public void testMalformedCommandLine3() throws Exception {
        String[] malformedArgs = {"-q", "-ca-", "------betweenness.set.stability=1"};
        DGAConfigurationUtil.updateCustomArgumentConfigurationFromCommandLine(this.minimalDefaultCustomArgumentParameters, malformedArgs);
    }

    @Test(expected = Exception.class)
    public void testMalformedCommandLine4() throws Exception {
        String[] malformedArgs = {"-q", "ca-", "betweenness.set.stability=1"};
        DGAConfigurationUtil.updateCustomArgumentConfigurationFromCommandLine(this.minimalDefaultCustomArgumentParameters, malformedArgs);
    }

    @Test(expected = Exception.class)
    public void testCommandLineWithAllMapsException() throws Exception {
        //TODO: May have broke it?
        String[] args = {"-ca", "betweenness.stability.counter=10", "-eif", "com.soteradefense.dga.format.io.EdgeInputFormatCustom", "-ca", "betweennes.pivot.percent=25", "-w", "25"};
        DGAConfigurationUtil.updateGiraphConfigurationFromCommandLine(this.minimalDefaultGiraphParameters, args);
        assertEquals(this.minimalDefaultGiraphParameters.size(), 1);
        DGAConfigurationUtil.updateCustomArgumentConfigurationFromCommandLine(this.minimalDefaultCustomArgumentParameters, args);
        assertEquals(this.minimalDefaultCustomArgumentParameters.size(), 2);
        DGAConfigurationUtil.updateConfigurationWithRequiredConfiguration(this.minimalDefaultGiraphParameters, this.requiredGiraphAnalyticParameters);
        DGAConfigurationUtil.updateConfigurationWithRequiredConfiguration(this.minimalDefaultCustomArgumentParameters, this.requiredCustomArgumentAnalyticParameters);
    }

    @Test
    public void testCommandLineWithAllMaps() throws Exception {
        String[] args = {"-ca", "betweenness.pivot.percent.initial=10",  "-ca", "betweennes.pivot.percent=25", "-ca", "betweenness.set.stability=40"};
        DGAConfigurationUtil.updateGiraphConfigurationFromCommandLine(this.minimalDefaultGiraphParameters, args);
        assertEquals(this.minimalDefaultGiraphParameters.size(), 1);
        DGAConfigurationUtil.updateCustomArgumentConfigurationFromCommandLine(this.minimalDefaultCustomArgumentParameters, args);
        assertEquals(this.minimalDefaultCustomArgumentParameters.size(), 4);
        DGAConfigurationUtil.updateConfigurationWithRequiredConfiguration(this.minimalDefaultGiraphParameters, this.requiredGiraphAnalyticParameters);
        DGAConfigurationUtil.updateConfigurationWithRequiredConfiguration(this.minimalDefaultCustomArgumentParameters, this.requiredCustomArgumentAnalyticParameters);
        assertEquals("2", this.minimalDefaultCustomArgumentParameters.get("betweenness.set.stability"));
    }
    @Test
    public void testCommandLineFromAllSources() throws Exception{
        String[] giraphCommand = DGAConfigurationUtil.generateDGAArgumentsFromAllSources("wcc",this.minimalDefaultGiraphParameters,this.minimalDefaultCustomArgumentParameters, this.requiredGiraphAnalyticParameters, this.requiredCustomArgumentAnalyticParameters, null, arguments);
        assertEquals(giraphCommand.length, );
    }

    @Test
    public void testReservedParameterXML() throws Exception {

    }

    @Test
    public void testReservedParameterCommandLine() throws Exception {

    }

    @Test
    public void testFlagParameterWorks() throws Exception {

    }

    @Test
    public void testSystemParameters() throws Exception {

    }

}
