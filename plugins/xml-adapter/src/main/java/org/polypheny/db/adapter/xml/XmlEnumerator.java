package org.polypheny.db.adapter.xml;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyValue;

public class XmlEnumerator implements Enumerator<PolyValue[]> {

    private final URL url;
    private XMLStreamReader reader;
    private XmlToPolyConverter converter;
    private String rootElementName;
    private PolyValue[] current;
    private boolean inTopLevelElement = false;


    public XmlEnumerator( URL url ) {
        this.url = url;
        this.converter = new XmlToPolyConverter();
        this.current = null;
        this.rootElementName = null;
        initializeReader();
    }


    private void initializeReader() {
        try {
            XMLInputFactory factory = XMLInputFactory.newInstance();
            InputStream inputStream = url.openStream();
            reader = factory.createXMLStreamReader( inputStream );

            while (reader.hasNext() && reader.next() != XMLStreamConstants.START_ELEMENT);
            rootElementName = reader.getLocalName();
            if(!reader.hasNext()) {
                throw new GenericRuntimeException( "Unexpected end of stream" );
            }
            do {
                if (!reader.hasNext()) {
                    return;
                }
                reader.next();
            } while (reader.getEventType() != XMLStreamConstants.START_ELEMENT );
        } catch ( XMLStreamException | IOException e ) {
            throw new GenericRuntimeException( "Error initializing XML reader: " + e.getMessage(), e );
        }
    }


    @Override
    public PolyValue[] current() {
        return current;
    }


    @Override
    public boolean moveNext() {
        try {
            String documentOuterName = reader.getLocalName();
            reader.next();
            current = new PolyValue[]{ converter.toPolyDocument( reader, documentOuterName ) };
            if (!reader.hasNext() ) {
                return false;
            }
            if (reader.next() == XMLStreamConstants.END_ELEMENT && rootElementName.equals( reader.getLocalName())) {
                return false;
            }
            while (reader.getEventType() != XMLStreamConstants.START_ELEMENT ) {
                if (!reader.hasNext()) {
                    return false;
                }
                reader.next();
            }
            return true;
        } catch ( XMLStreamException e ) {
            throw new RuntimeException( "Could nod find start of XML content.", e );
        }

    }


    @Override
    public void reset() {
        close();
        initializeReader();
    }


    @Override
    public void close() {
        if ( reader != null ) {
            try {
                reader.close();
            } catch ( XMLStreamException e ) {
                throw new GenericRuntimeException( "Error closing XML reader: " + e.getMessage(), e );
            }
        }
        reader = null;
        current = null;
    }

}
