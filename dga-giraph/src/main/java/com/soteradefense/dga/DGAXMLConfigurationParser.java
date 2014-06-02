package com.soteradefense.dga;

import java.io.IOException;
import java.io.InputStream;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class DGAXMLConfigurationParser {

    private static final Logger logger = LoggerFactory.getLogger(DGAXMLConfigurationParser.class);

    public static DGAConfiguration parse(InputStream is) throws IOException, ParserConfigurationException, SAXException, XPathExpressionException {
        DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
        domFactory.setNamespaceAware(true);
        DocumentBuilder builder = domFactory.newDocumentBuilder();
        Document doc = builder.parse(is);
        XPath xpath = XPathFactory.newInstance().newXPath();

        // Now we'll create our DGA Configuration object
        DGAConfiguration dgaConf = new DGAConfiguration();

        String expression = "//configuration/system/property";
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
                dgaConf.setSystemProperty(name, value);
            }
        } catch (XPathExpressionException e) {
            logger.error("There was an error in the XPathExpression " + expression, e);
            throw e;
        }

        expression = "//configuration/custom/property";
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
                dgaConf.setCustomProperty(name, value);
            }
        } catch (XPathExpressionException e) {
            logger.error("There was an error in the XPathExpression " + expression, e);
            throw e;
        }

        expression = "//configuration/giraph/property";
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
                try {
                    dgaConf.setGiraphProperty("-" + name, value);
                } catch (IllegalArgumentException e) {
                    logger.warn("An illegal argument was specified in the configuration file: " + name + " is not an allowable giraph configuration.");
                }
            }
        } catch (XPathExpressionException e) {
            logger.error("There was an error in the XPathExpression " + expression, e);
            throw e;
        }

        return dgaConf;
    }
}
