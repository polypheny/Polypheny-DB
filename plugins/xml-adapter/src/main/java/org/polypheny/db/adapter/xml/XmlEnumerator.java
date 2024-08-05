package org.polypheny.db.adapter.xml;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.type.entity.PolyValue;

public class XmlEnumerator implements Enumerator<PolyValue[]> {

    private final URL url;
    private XMLStreamReader reader;
    private final XmlToPolyConverter converter;
    private PolyValue[] current;
    private boolean inTopLevelElement = false;

    public XmlEnumerator(URL url) {
        this.url = url;
        this.converter = new XmlToPolyConverter();
        this.current = null;
        initializeReader();
    }

    private void initializeReader() {
        try {
            XMLInputFactory factory = XMLInputFactory.newInstance();
            InputStream inputStream = url.openStream();
            reader = factory.createXMLStreamReader(inputStream);
        } catch (XMLStreamException | IOException e) {
            throw new RuntimeException("Error initializing XML reader: " + e.getMessage(), e);
        }
    }

    private PolyValue[] parseNext() throws XMLStreamException {
        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamReader.START_ELEMENT) {
                if (inTopLevelElement) {
                    // Process each child element of the top-level collection as a document
                    return new PolyValue[]{converter.nodeToPolyDocument(reader)};
                } else {
                    // Mark that we are inside the top-level collection element
                    inTopLevelElement = true;
                }
            } else if (event == XMLStreamReader.END_ELEMENT && inTopLevelElement) {
                inTopLevelElement = false; // End of top-level collection element
            }
        }
        return null; // No more elements
    }

    @Override
    public PolyValue[] current() {
        return current;
    }

    @Override
    public boolean moveNext() {
        try {
            current = parseNext();
            return current != null;
        } catch (XMLStreamException e) {
            throw new RuntimeException("Error reading XML: " + e.getMessage(), e);
        }
    }

    @Override
    public void reset() {
        close();
        initializeReader();
    }

    @Override
    public void close() {
        if (reader != null) {
            try {
                reader.close();
            } catch (XMLStreamException e) {
                throw new RuntimeException("Error closing XML reader: " + e.getMessage(), e);
            }
        }
        reader = null;
        current = null;
    }
}
