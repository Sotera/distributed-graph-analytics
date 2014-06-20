package com.soteradefense.dga;

import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DGAConfigurationTest {

    private DGAConfiguration dgaConf1;
    private DGAConfiguration dgaConf2;
    private DGAConfiguration dgaConf3;

    @Before
    public void setup() {
        dgaConf1 = new DGAConfiguration();
        dgaConf2 = new DGAConfiguration();
        dgaConf3 = new DGAConfiguration();

        dgaConf1.setDGAGiraphProperty("-eip", "/sweet/path");
        dgaConf2.setDGAGiraphProperty("-vif", "class.name.VertexInputFormat");
        dgaConf3.setDGAGiraphProperty("-eip", "/sweeter/path");

        dgaConf1.setGiraphProperty("-w", "12");
        dgaConf2.setGiraphProperty("-w", "4");
        dgaConf3.setGiraphProperty("-w", "8");

        dgaConf1.setCustomProperty("simple.edge.delimiter", ",");
        dgaConf2.setCustomProperty("simple.edge.delimiter", "\t");

        dgaConf1.setCustomProperty("simple.edge.value.default", "");
        dgaConf3.setCustomProperty("simple.edge.value.default", "4");
    }

    @Test
    public void testCoalesce() {

        DGAConfiguration coalesced = DGAConfiguration.coalesce(dgaConf1, dgaConf2, dgaConf3);
        Map<String, String> giraphProps = coalesced.getGiraphProperties();
        Map<String, String> customArgumentProps = coalesced.getCustomArgumentProperties();

        assertEquals(3, giraphProps.size());
        assertEquals(2, customArgumentProps.size());
        assertEquals("/sweeter/path", giraphProps.get("-eip"));
        assertEquals("class.name.VertexInputFormat", giraphProps.get("-vif"));
        assertEquals("8", giraphProps.get("-w"));

        assertEquals("\t", customArgumentProps.get("simple.edge.delimiter"));
        assertEquals("4", customArgumentProps.get("simple.edge.value.default"));


        coalesced = DGAConfiguration.coalesce(dgaConf2, dgaConf3, dgaConf1);
        giraphProps = coalesced.getGiraphProperties();
        customArgumentProps = coalesced.getCustomArgumentProperties();

        assertEquals(3, giraphProps.size());
        assertEquals(2, customArgumentProps.size());
        assertEquals("/sweet/path", giraphProps.get("-eip"));
        assertEquals("class.name.VertexInputFormat", giraphProps.get("-vif"));
        assertEquals("12", giraphProps.get("-w"));

        assertEquals(",", customArgumentProps.get("simple.edge.delimiter"));
        assertEquals("", customArgumentProps.get("simple.edge.value.default"));

        coalesced = DGAConfiguration.coalesce(dgaConf3, dgaConf1, dgaConf2);
        giraphProps = coalesced.getGiraphProperties();
        customArgumentProps = coalesced.getCustomArgumentProperties();

        assertEquals(3, giraphProps.size());
        assertEquals(2, customArgumentProps.size());
        assertEquals("/sweet/path", giraphProps.get("-eip"));
        assertEquals("class.name.VertexInputFormat", giraphProps.get("-vif"));
        assertEquals("4", giraphProps.get("-w"));

        assertEquals("\t", customArgumentProps.get("simple.edge.delimiter"));
        assertEquals("", customArgumentProps.get("simple.edge.value.default"));

    }

    @Test(expected = IllegalArgumentException.class)
    public void testDisallowedGiraphProperties() {
        DGAConfiguration dgaConf = new DGAConfiguration();
        dgaConf.setGiraphProperty("-eip", "/path");
    }

    @Test
    public void testAllAllowedGiraphProperties() {
        DGAConfiguration dgaConf = new DGAConfiguration();
        dgaConf.setGiraphProperty("-w", "21");
        dgaConf.setGiraphProperty("-q", "");
        dgaConf.setGiraphProperty("-yj", "/path/to/yarn/jars");
        dgaConf.setGiraphProperty("-yh", "1600");
        assertEquals(4, dgaConf.getGiraphProperties().size());
    }

    @Test
    public void testGenerateArguments() {
        DGAConfiguration coalesced = DGAConfiguration.coalesce(dgaConf1, dgaConf2, dgaConf3);
        Map<String, String> giraphProps = coalesced.getGiraphProperties();
        Map<String, String> customArgumentProps = coalesced.getCustomArgumentProperties();
        String[] generatedArgs = coalesced.convertToCommandLineArguments("test.class.Name");

        for (String key : giraphProps.keySet()) {
            assertTrue("Checking for " + key + " : " + giraphProps.get(key), argsExistInArray(generatedArgs, key, giraphProps.get(key)));
        }

        for (String key : customArgumentProps.keySet()) {
            String value = customArgumentProps.get(key);
            value = key + "=" + value;
            assertTrue("Checking for -ca : " + value, argsExistInArray(generatedArgs, "-ca", value));
        }

        assertEquals("test.class.Name", generatedArgs[0]);
    }

    private boolean argsExistInArray(String[] args, String key, String value) {
        for (int i = 1; i < args.length; i = i + 2) {
            if (args[i].equals(key) && args[i + 1].equals(value)) {
                return true;
            }
        }
        return false;
    }

}
