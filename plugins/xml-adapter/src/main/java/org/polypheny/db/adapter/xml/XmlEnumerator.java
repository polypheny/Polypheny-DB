package org.polypheny.db.adapter.xml;

import java.io.IOException;
import java.net.URL;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.type.entity.PolyValue;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class XmlEnumerator implements Enumerator<PolyValue[]> {

    private URL url;
    private DocumentBuilderFactory factory;
    private DocumentBuilder builder;
    private XmlToPolyConverter converter;
    private PolyValue[] current;
    private NodeList nodes;
    private int currentIndex;

    public XmlEnumerator(URL url) throws ParserConfigurationException {
        this.url = url;
        this.factory = DocumentBuilderFactory.newInstance();
        this.builder = factory.newDocumentBuilder();
        this.converter = new XmlToPolyConverter();
        this.currentIndex = 0;
    }

    private void initializeParser() throws SAXException, IOException {
        Document document = builder.parse(url.openStream());
        Element rootElement = document.getDocumentElement();
        // Assume the root element contains a collection of child elements
        this.nodes = rootElement.getChildNodes();
        this.currentIndex = 0;
    }

    private Node getNextNode() {
        if (nodes == null || currentIndex >= nodes.getLength()) {
            return null;
        }
        while (currentIndex < nodes.getLength()) {
            Node node = nodes.item(currentIndex++);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                return node;
            }
        }
        return null;
    }

    @Override
    public PolyValue[] current() {
        return current;
    }

    @Override
    public boolean moveNext() {
        try {
            if (nodes == null) {
                initializeParser();
            }
            Node node = getNextNode();
            current = node == null ? null : new PolyValue[]{converter.nodeToPolyDocument(node)};
            return node != null;
        } catch (IOException | SAXException e) {
            throw new RuntimeException("Error reading XML: " + e.getMessage(), e);
        }
    }

    @Override
    public void reset() {
        this.nodes = null;
        this.current = null;
        this.currentIndex = 0;
    }

    @Override
    public void close() {
        // Nothing to close in this implementation
    }
}
