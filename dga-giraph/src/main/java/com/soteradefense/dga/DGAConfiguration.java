package com.soteradefense.dga;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.soteradefense.dga.io.formats.DGATextEdgeValueInputFormat;
import com.soteradefense.dga.io.formats.SimpleEdgeOutputFormat;
public class DGAConfiguration {

    private Map<String, String> giraphProperties;
    private Map<String, String> customArgumentProperties;
    private Map<String, String> systemProperties;

    public DGAConfiguration() {
        this.giraphProperties = new HashMap<String, String>();
        this.customArgumentProperties = new HashMap<String, String>();
        this.systemProperties = new HashMap<String, String>();
    }

    public void setGiraphProperty(String key, String value) {
        if (key.equals("-q") || key.equals("-w") || key.equals("-yh") || key.equals("-yj")) {
            this.setDGAGiraphProperty(key, value);
        } else {
            throw new IllegalArgumentException("The key provided, " + key + ", is not allowed to be specified within DGA.");
        }
    }

    void setDGAGiraphProperty(String key, String value) {
        this.giraphProperties.put(key, value);
    }

    public void setCustomProperty(String key, String value) {
        this.customArgumentProperties.put(key, value);
    }

    public void setSystemProperty(String key, String value) {
        this.systemProperties.put(key, value);
    }

    public Map<String, String> getGiraphProperties() {
        return Collections.unmodifiableMap(this.giraphProperties);
    }

    public Map<String, String> getCustomArgumentProperties() {
        return Collections.unmodifiableMap(this.customArgumentProperties);
    }

    public Map<String, String> getSystemProperties() {
        return Collections.unmodifiableMap(this.systemProperties);
    }

    public String[] convertToCommandLineArguments(String computationClassName) {
        List<String> argList = new ArrayList<String>();
        for (String key : this.systemProperties.keySet()) {
            argList.add("-D");
            argList.add(key + "=" + this.systemProperties.get(key));
        }

        argList.add(computationClassName);
        for (String key : this.giraphProperties.keySet()) {
            if (key.equals("-q")) {
                argList.add(key);
            } else {
                argList.add(key);
                argList.add(this.giraphProperties.get(key));
            }
        }

        for (String key : this.customArgumentProperties.keySet()) {
            argList.add("-ca");
            argList.add(key + "=" + this.customArgumentProperties.get(key));
        }

        return argList.toArray(new String[argList.size()]);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("dgaConfiguration=(giraphProperties: {");
        for (Map.Entry<String, String> entry : this.giraphProperties.entrySet()) {
            builder.append(entry.getKey());
            builder.append(":");
            builder.append(entry.getValue());
            builder.append(",");
        }
        builder.replace(builder.length()-1, builder.length(), "}");
        builder.append("customArgumentProperties: {");
        for (Map.Entry<String, String> entry : this.customArgumentProperties.entrySet()) {
            builder.append(entry.getKey());
            builder.append(":");
            builder.append(entry.getValue());
            builder.append(",");
        }
        builder.replace(builder.length()-1, builder.length(), "}");
        builder.append("systemProperties: {");
        for (Map.Entry<String, String> entry : this.systemProperties.entrySet()) {
            builder.append(entry.getKey());
            builder.append(":");
            builder.append(entry.getValue());
            builder.append(",");
        }
        builder.replace(builder.length()-1, builder.length(), "})");
        return builder.toString();
    }

    /**
     * This method returns a new DGAConfiguration object containing the resulting coalescing activity of all provided DGAConfigurations
     * The order of the configurations is important -- the configuration with the lowest priority will be added first, then the second, and so on.
     * This means the last configuration passed in will trump all previous configurations if a collision in property name will occur.
     * @param configurationsInOrder The order to apply configurations in the resulting
     * @return
     */
    public static DGAConfiguration coalesce(DGAConfiguration ... configurationsInOrder) {
        DGAConfiguration conf = new DGAConfiguration();
        for (DGAConfiguration dgaConf : configurationsInOrder) {
            conf.giraphProperties.putAll(dgaConf.giraphProperties);
            conf.customArgumentProperties.putAll(dgaConf.customArgumentProperties);
            conf.systemProperties.putAll(dgaConf.systemProperties);
        }
        return conf;
    }

}

