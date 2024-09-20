/*
 * Copyright 2019-2024 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.StringReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.apache.commons.codec.DecoderException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.adapter.xml.XmlToPolyConverter;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;

public class XmlToPolyConverterTest {

    private static XmlToPolyConverter converter;


    @BeforeAll
    public static void setup() {
        converter = new XmlToPolyConverter();
        TestHelper testHelper = TestHelper.getInstance();
    }


    @Test
    public void testString() throws XMLStreamException, DecoderException {
        String xml = "<name>Maxine</name>";
        XMLStreamReader reader = XMLInputFactory.newDefaultFactory().createXMLStreamReader( new StringReader( xml ) );
        reader.next();
        PolyValue value = converter.toPolyDocument( reader, "root" );
        assertTrue( value.isDocument() );
        PolyDocument polyDoc = value.asDocument();
        assertEquals( "Maxine", polyDoc.get( new PolyString( "name" ) ).asString().getValue() );
    }


    @Test
    public void testLong() throws XMLStreamException, DecoderException {
        String xml = "<integer type=\"integer\">492943</integer>";
        XMLStreamReader reader = XMLInputFactory.newDefaultFactory().createXMLStreamReader( new StringReader( xml ) );
        reader.next();
        PolyValue value = converter.toPolyDocument( reader, "root" );
        assertTrue( value.isDocument() );
        PolyDocument polyDoc = value.asDocument();
        assertEquals( 492943, polyDoc.get( new PolyString( "integer" ) ).asLong().getValue() );
    }


    @Test
    public void testDouble() throws XMLStreamException, DecoderException {
        String xml = "<double type=\"double\">-650825.13</double>";
        XMLStreamReader reader = XMLInputFactory.newDefaultFactory().createXMLStreamReader( new StringReader( xml ) );
        reader.next();
        PolyValue value = converter.toPolyDocument( reader, "root" );
        assertTrue( value.isDocument() );
        PolyDocument polyDoc = value.asDocument();
        assertEquals( -650825.13, polyDoc.get( new PolyString( "double" ) ).asDouble().getValue() );
    }


    @Test
    public void testBooleanTrue() throws XMLStreamException, DecoderException {
        String xml = "<boolean type=\"boolean\">true</boolean>";
        XMLStreamReader reader = XMLInputFactory.newDefaultFactory().createXMLStreamReader( new StringReader( xml ) );
        reader.next();
        PolyValue value = converter.toPolyDocument( reader, "root" );
        assertTrue( value.isDocument() );
        PolyDocument polyDoc = value.asDocument();
        assertEquals( true, polyDoc.get( new PolyString( "boolean" ) ).asBoolean().getValue() );
    }


    @Test
    public void testBooleanFalse() throws XMLStreamException, DecoderException {
        String xml = "<boolean type=\"boolean\">false</boolean>";
        XMLStreamReader reader = XMLInputFactory.newDefaultFactory().createXMLStreamReader( new StringReader( xml ) );
        reader.next();
        PolyValue value = converter.toPolyDocument( reader, "root" );
        assertTrue( value.isDocument() );
        PolyDocument polyDoc = value.asDocument();
        assertEquals( false, polyDoc.get( new PolyString( "boolean" ) ).asBoolean().getValue() );
    }


    @Test
    public void testArray() throws XMLStreamException, DecoderException {
        String xml = "<integers type=\"list\">\n"
                + "    <item>0</item>\n"
                + "    <item>1</item>\n"
                + "    <item>2</item>\n"
                + "    <item>3</item>\n"
                + "</integers>\n";
        XMLStreamReader reader = XMLInputFactory.newDefaultFactory().createXMLStreamReader( new StringReader( xml ) );
        reader.next();
        PolyValue value = converter.toPolyDocument( reader, "root" );
        assertTrue( value.isDocument() );
        PolyDocument polyDoc = value.asDocument();
        assertTrue( polyDoc.get( new PolyString( "integers" ) ).isList() );
        PolyList<PolyValue> list = polyDoc.get( new PolyString( "integers" ) ).asList();
        assertEquals( "0", list.get( 0 ).asString().getValue() );
        assertEquals( "1", list.get( 1 ).asString().getValue() );
        assertEquals( "2", list.get( 2 ).asString().getValue() );
        assertEquals( "3", list.get( 3 ).asString().getValue() );
    }


    @Test
    public void testNull() throws XMLStreamException, DecoderException {
        String xml = "<null></null>";
        XMLStreamReader reader = XMLInputFactory.newDefaultFactory().createXMLStreamReader( new StringReader( xml ) );
        reader.next();
        PolyValue value = converter.toPolyDocument( reader, "root" );
        assertTrue( value.isDocument() );
        PolyDocument polyDoc = value.asDocument();
        assertTrue( polyDoc.get( new PolyString( "null" ) ).isNull() );
    }


    @Test
    public void testDate() throws XMLStreamException, DecoderException {
        String xml = "<date type=\"date\">2024-08-07</date>";
        XMLStreamReader reader = XMLInputFactory.newDefaultFactory().createXMLStreamReader( new StringReader( xml ) );
        reader.next();
        PolyValue value = converter.toPolyDocument( reader, "root" );
        assertTrue( value.isDocument() );
        PolyDocument polyDoc = value.asDocument();
        assertEquals( 1722988800000L, polyDoc.get( new PolyString( "date" ) ).asDate().millisSinceEpoch );
    }


    @Test
    public void testTime() throws XMLStreamException, DecoderException {
        String xml = "<time type=\"time\">13:45:30</time>";
        XMLStreamReader reader = XMLInputFactory.newDefaultFactory().createXMLStreamReader( new StringReader( xml ) );
        reader.next();
        PolyValue value = converter.toPolyDocument( reader, "root" );
        assertTrue( value.isDocument() );
        PolyDocument polyDoc = value.asDocument();
        assertEquals( 49530000, polyDoc.get( new PolyString( "time" ) ).asTime().ofDay );
    }


    @Test
    public void testDateTime() throws XMLStreamException, DecoderException {
        String xml = "<dateTime type=\"dateTime\">2024-08-07T13:45:30</dateTime>";
        XMLStreamReader reader = XMLInputFactory.newDefaultFactory().createXMLStreamReader( new StringReader( xml ) );
        reader.next();
        PolyValue value = converter.toPolyDocument( reader, "root" );
        assertTrue( value.isDocument() );
        PolyDocument polyDoc = value.asDocument();
        assertEquals( 1723038330000L, polyDoc.get( new PolyString( "dateTime" ) ).asTimestamp().millisSinceEpoch );
    }


    @Test
    public void testBase64Binary() throws XMLStreamException, DecoderException {
        String xml = "<binary type=\"base64Binary\">SGVsbG8sIFdvcmxkIQ==</binary>";
        XMLStreamReader reader = XMLInputFactory.newDefaultFactory().createXMLStreamReader( new StringReader( xml ) );
        reader.next();
        PolyValue value = converter.toPolyDocument( reader, "root" );
        assertTrue( value.isDocument() );
        PolyDocument polyDoc = value.asDocument();
        assertEquals( "Hello, World!", new String( polyDoc.get( new PolyString( "binary" ) ).asBinary().getValue() ) );
    }


    @Test
    public void testHexBinary() throws XMLStreamException, DecoderException {
        String xml = "<binary type=\"hexBinary\">48656c6c6f2c20576f726c6421</binary>";
        XMLStreamReader reader = XMLInputFactory.newDefaultFactory().createXMLStreamReader( new StringReader( xml ) );
        reader.next();
        PolyValue value = converter.toPolyDocument( reader, "root" );
        assertTrue( value.isDocument() );
        PolyDocument polyDoc = value.asDocument();
        assertEquals( "Hello, World!", new String( polyDoc.get( new PolyString( "binary" ) ).asBinary().getValue() ) );
    }

}
