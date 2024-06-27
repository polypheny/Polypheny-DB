/*
 * Copyright 2019-2024 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.adapter.xml;

import java.io.IOException;
import java.net.URL;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.type.entity.PolyValue;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class XmlEnumerator implements Enumerator<PolyValue[]> {

    private URL url;
    private XMLStreamReader reader;
    private DocumentBuilderFactory factory;
    private DocumentBuilder builder;
    private XmlToPolyConverter converter;
    private PolyValue[] current;
    private boolean isCollection;
    private NodeList nodes;
    private int currentIndex;


    public XmlEnumerator( URL url ) throws ParserConfigurationException {
        this.url = url;
        this.factory = DocumentBuilderFactory.newInstance();
        this.builder = factory.newDocumentBuilder();
        this.converter = new XmlToPolyConverter();
        this.currentIndex = -1;
    }


    private void initializeParser() throws SAXException, IOException {
        if ( this.reader == null ) {
            Document document = builder.parse( url.openStream() );
            Element rootElement = document.getDocumentElement();
            isCollection = rootElement.getNodeType() == Node.ELEMENT_NODE && rootElement.getChildNodes().getLength() > 0;
            if ( !isCollection ) {
                throw new IllegalArgumentException( "Invalid XML file format. Expected a collection of elements at the top level." );
            }
            this.nodes = rootElement.getChildNodes();
            this.currentIndex = 0;
        }
    }


    private Node getNextNode() {
        if ( nodes == null || currentIndex >= nodes.getLength() ) {
            return null;
        }

        while ( currentIndex < nodes.getLength() ) {
            Node node = nodes.item( currentIndex++ );
            if ( node.getNodeType() == Node.ELEMENT_NODE ) {
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
            if ( reader == null ) {
                initializeParser();
            }
            Node node = getNextNode();
            current = node == null ? null : new PolyValue[]{ converter.nodeToPolyDocument( node ) };
            return node != null;
        } catch ( IOException | SAXException e ) {
            throw new RuntimeException( "Error reading XML: " + e.getMessage(), e );
        }
    }


    @Override
    public void reset() {
        close();
        this.reader = null;
        this.current = null;
        this.currentIndex = -1;
    }


    @Override
    public void close() {
        if ( reader != null ) {
            try {
                reader.close();
            } catch ( XMLStreamException e ) {
                throw new RuntimeException( "Failed to close XML reader: " + e.getMessage(), e );
            }
        }
    }

}
