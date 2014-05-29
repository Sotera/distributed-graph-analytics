package com.soteradefense.dga;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

/**
 * Generates configuration settings for DGA out of defaults, analytic required settings, command line arguments, and configuration files
 */
public class DGAConfigurationUtil {

    private static final Logger logger = LoggerFactory.getLogger(DGAConfigurationUtil.class);

    private static final Set<String> reservedGiraphSettings = new TreeSet<String>();

    static {
        reservedGiraphSettings.add("aw");
        reservedGiraphSettings.add("c");
        reservedGiraphSettings.add("cf");
        reservedGiraphSettings.add("eif");
        reservedGiraphSettings.add("eip");
        reservedGiraphSettings.add("eof");
        reservedGiraphSettings.add("esd");
        reservedGiraphSettings.add("h");
        reservedGiraphSettings.add("jyc");
        reservedGiraphSettings.add("la");
        reservedGiraphSettings.add("mc");
        reservedGiraphSettings.add("op");
        reservedGiraphSettings.add("pc");
        reservedGiraphSettings.add("th");
        reservedGiraphSettings.add("ve");
        reservedGiraphSettings.add("vif");
        reservedGiraphSettings.add("vip");
        reservedGiraphSettings.add("vof");
        reservedGiraphSettings.add("vsd");
        reservedGiraphSettings.add("vvf");
        reservedGiraphSettings.add("wc");
        reservedGiraphSettings.add("yh");
        reservedGiraphSettings.add("yj");
    }

    public static String[] generateDGAArgumentsFromAllSources(String analyticComputation, Map<String, String> giraphConfiguration, Map<String, String> customArgumentConfiguration, Map<String, String> requiredAnalyticGiraphConfiguration, Map<String, String> requiredAnalyticCustomArgumentConfiguration, InputStream is, String[] commandLineArguments) throws Exception {
//        updateSystemConfigurationFromFile(is);
//        updateConfigurationFromFileByType("giraph", giraphConfiguration, is);
//        updateConfigurationFromFileByType("custom", customArgumentConfiguration, is);
        updateGiraphConfigurationFromCommandLine(giraphConfiguration, commandLineArguments);
        updateCustomArgumentConfigurationFromCommandLine(customArgumentConfiguration, commandLineArguments);
        updateConfigurationWithRequiredConfiguration(giraphConfiguration, requiredAnalyticGiraphConfiguration);
        updateConfigurationWithRequiredConfiguration(customArgumentConfiguration, requiredAnalyticCustomArgumentConfiguration);

        String[] giraphRunnerArgs = new String[giraphConfiguration.size() + customArgumentConfiguration.size() + 1];
        giraphRunnerArgs[0] = analyticComputation;
        int index = 1;
        for (String key : giraphConfiguration.keySet()) {
            giraphRunnerArgs[index] = "-" + key;
            index++;
            giraphRunnerArgs[index] = giraphConfiguration.get(key);
            index++;
        }

        for (String key : customArgumentConfiguration.keySet()) {
            giraphRunnerArgs[index] = "-ca";
            index++;
            String value = customArgumentConfiguration.get(key);
            if (!value.startsWith("\"") || !value.endsWith("\"")) {
                value = "\"" + value + "\"";
            }
            giraphRunnerArgs[index] = key + "=" + value;
            index++;
        }
        return giraphRunnerArgs;
    }

    public static void updateConfigurationWithRequiredConfiguration(Map<String, String> configuration, Map<String, String> requiredAnalyticConfiguration) {
        for (String key : requiredAnalyticConfiguration.keySet()) {
            if (configuration.containsKey(key)) {
                logger.warn("Configuration setting " + key + " : " + configuration.get(key) + " has been overriden by the analytic required setting " + key + " : " + requiredAnalyticConfiguration.get(key));
            }
            configuration.put(key, requiredAnalyticConfiguration.get(key));
        }
    }

    public static void updateGiraphConfigurationFromCommandLine(Map<String, String> configuration, String[] args) throws Exception {
        if (args != null && args.length > 0) {
            String previousValue = null;
            for (int i = 0; i < args.length; i++) {
                String currentValue = args[i];
                if (currentValue.equals("-q")) {
                    if (previousValue == null) configuration.put("q", "");
                    else
                        throw new Exception("Malformed argument list: Parameter " + previousValue + " requires an argument");
                } else {
                    if (previousValue != null && currentValue.startsWith("-")) {
                        throw new Exception("Malformed argument list: Parameter " + previousValue + " requires an argument");
                    } else if (previousValue != null) {
                        if (!previousValue.equals("-ca")) {
                            if (!reservedGiraphSettings.contains(previousValue.substring(1))) {
                                configuration.put(previousValue.substring(1), currentValue);
                            } else {
                                logger.info("DGA does not support overriding this configuration value, argument " + previousValue + " and value " + currentValue);
                            }
                            previousValue = null;
                        } else {
                            previousValue = null;
                        }
                    } else {
                        previousValue = currentValue;
                    }
                }
            }
            if (previousValue != null) {
                throw new Exception("Malformed argument list: Parameter " + previousValue + " requires an argument");
            }
        }
    }

    public static void updateCustomArgumentConfigurationFromCommandLine(Map<String, String> configuration, String[] args) throws Exception {
        if (args != null && args.length > 0) {
            String previousValue = null;
            for (int i = 0; i < args.length; i++) {
                String currentValue = args[i];
                if (currentValue.equals("-q")) {
                    if (previousValue != null)
                        throw new Exception("Malformed argument list: Parameter " + previousValue + " requires an argument");
                } else {
                    if (previousValue != null && currentValue.startsWith("-")) {
                        throw new Exception("Malformed argument list: Parameter " + previousValue + " requires an argument");
                    } else if (previousValue != null) {
                        if (previousValue.equals("-ca")) {
                            int equalsPosition = currentValue.indexOf("=");
                            if (equalsPosition == -1) {
                                throw new Exception("Malformed custom argument: -ca " + currentValue + " must be in the form of -ca key=value or -ca key=\"longer value list\"");
                            }
                            String customArgumentKey = currentValue.substring(0, equalsPosition);
                            String customArgumentValue = currentValue.substring(equalsPosition + 1);
                            configuration.put(customArgumentKey, customArgumentValue);
                            previousValue = null;
                        }
                    } else {
                        previousValue = currentValue;
                    }
                }
            }
            if (previousValue != null) {
                throw new Exception("Malformed argument list: Parameter " + previousValue + " requires an argument");
            }
        }
    }

    public static void updateSystemConfigurationFromFile(InputStream is) throws Exception {
        DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
        domFactory.setNamespaceAware(true);
        DocumentBuilder builder = domFactory.newDocumentBuilder();
        Document doc = builder.parse(is);
        XPath xpath = XPathFactory.newInstance().newXPath();
        String expression = "//configuration/property[@type='system']";
        try {
            XPathExpression expr = xpath.compile(expression);
            NodeList nodes = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
            for (int i = 0; i < nodes.getLength(); i++) {
                NodeList children = nodes.item(i).getChildNodes();
                String name = null;
                String value = null;
                for (int j = 0; j < children.getLength(); j++) {
                    if ("name".equals(children.item(j).getLocalName())) {
                        name = children.item(j).getTextContent();
                    } else if ("value".equals(children.item(j).getLocalName())) {
                        value = children.item(j).getTextContent();
                    }
                }

                if (name == null || value == null)
                    throw new IOException("Malformed XML for the this DGA configuration file");
                if (System.getProperty(name) == null) System.setProperty(name, value);
            }
        } catch (XPathExpressionException e) {
            logger.error("There was an error in the XPathExpression " + expression, e);
            throw e;

        }
    }

    private static void updateMapFromFile(Document doc, XPathExpression expr, Map<String, String> configuration) throws Exception {
        NodeList nodes = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
        for (int i = 0; i < nodes.getLength(); i++) {
            NodeList children = nodes.item(i).getChildNodes();
            String name = null;
            String value = null;
            for (int j = 0; j < children.getLength(); j++) {
                if ("name".equals(children.item(j).getLocalName())) {
                    name = children.item(j).getTextContent();
                } else if ("value".equals(children.item(j).getLocalName())) {
                    value = children.item(j).getTextContent();
                }
            }

            if (name == null || value == null)
                throw new IOException("Malformed XML for the this DGA configuration file");
            if (reservedGiraphSettings.contains(name)) {
                logger.warn("The configuration setting " + name + " cannot be specified through DGARunner");
            } else {
                configuration.put(name, value);
            }
        }
    }

    public static void updateConfigurationFromFileByType(String type, Map<String, String> configuration, InputStream is) throws Exception {
        DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
        domFactory.setNamespaceAware(true);
        DocumentBuilder builder = domFactory.newDocumentBuilder();
        Document doc = builder.parse(is);
        XPath xpath = XPathFactory.newInstance().newXPath();
        String expression = "//configuration/property[@type='" + type + "']";
        try {
            XPathExpression expr = xpath.compile(expression);
            updateMapFromFile(doc, expr, configuration);
        } catch (XPathExpressionException e) {
            logger.error("There was an error in the XPathExpression " + expression, e);
            throw e;
        }
    }

}
