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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.adapter.xml.XmlToPolyConverter;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.relational.PolyMap;

public class XmlToPolyConverterTest {

    private static XmlToPolyConverter converter;


    @BeforeAll
    public static void setup() {
        converter = new XmlToPolyConverter();
        TestHelper testHelper = TestHelper.getInstance();
    }


    @Test
    public void testString() throws XMLStreamException {
        String xml = "<root><name>Maxine</name></root>";
        XMLStreamReader reader = XMLInputFactory.newDefaultFactory().createXMLStreamReader( new StringReader( xml ) );
        reader.next();
        PolyValue value = converter.nodeToPolyDocument( reader );
        assertTrue( value.isDocument() );
        PolyDocument polyDoc = value.asDocument();
        assertEquals( "Maxine", polyDoc.get( new PolyString( "name" ) ).asString().getValue() );
    }


    @Test
    public void testLong() throws XMLStreamException {
        String xml = "<root><integer type=\"integer\">492943</integer></root>";
        XMLStreamReader reader = XMLInputFactory.newDefaultFactory().createXMLStreamReader( new StringReader( xml ) );
        reader.next();
        PolyValue value = converter.nodeToPolyDocument( reader );
        assertTrue( value.isDocument() );
        PolyDocument polyDoc = value.asDocument();
        assertEquals( 492943, polyDoc.get( new PolyString( "integer" ) ).asLong().getValue() );
    }


    @Test
    public void testDouble() throws XMLStreamException {
        String xml = "<root><double type=\"double\">-650825.13</double></root>";
        XMLStreamReader reader = XMLInputFactory.newDefaultFactory().createXMLStreamReader( new StringReader( xml ) );
        reader.next();
        PolyValue value = converter.nodeToPolyDocument( reader );
        assertTrue( value.isDocument() );
        PolyDocument polyDoc = value.asDocument();
        assertEquals( -650825.13, polyDoc.get( new PolyString( "double" ) ).asDouble().getValue() );
    }


    @Test
    public void testBooleanTrue() throws XMLStreamException {
        String xml = "<root><boolean type=\"boolean\">true</boolean></root>";
        XMLStreamReader reader = XMLInputFactory.newDefaultFactory().createXMLStreamReader( new StringReader( xml ) );
        reader.next();
        PolyValue value = converter.nodeToPolyDocument( reader );
        assertTrue( value.isDocument() );
        PolyDocument polyDoc = value.asDocument();
        assertEquals( true, polyDoc.get( new PolyString( "boolean" ) ).asBoolean().getValue() );
    }


    @Test
    public void testBooleanFalse() throws XMLStreamException {
        String xml = "<root><boolean type=\"boolean\">false</boolean></root>";
        XMLStreamReader reader = XMLInputFactory.newDefaultFactory().createXMLStreamReader( new StringReader( xml ) );
        reader.next();
        PolyValue value = converter.nodeToPolyDocument( reader );
        assertTrue( value.isDocument() );
        PolyDocument polyDoc = value.asDocument();
        assertEquals( false, polyDoc.get( new PolyString( "boolean" ) ).asBoolean().getValue() );
    }


    @Test
    public void testArray() throws XMLStreamException {
        String xml = "<root><integers type=\"list\"><item>0</item><item>1</item><item>2</item><item>3</item></integers></root>";
        XMLStreamReader reader = XMLInputFactory.newDefaultFactory().createXMLStreamReader( new StringReader( xml ) );
        reader.next();
        PolyValue value = converter.nodeToPolyDocument( reader );
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
    public void testNull() throws XMLStreamException {
        String xml = "<root><null></null></root>";
        XMLStreamReader reader = XMLInputFactory.newDefaultFactory().createXMLStreamReader( new StringReader( xml ) );
        reader.next();
        PolyValue value = converter.nodeToPolyDocument( reader );
        assertTrue( value.isDocument() );
        PolyDocument polyDoc = value.asDocument();
        assertTrue( polyDoc.get( new PolyString( "null" ) ).isNull() );
    }


    @Test
    public void testDocument() throws XMLStreamException {
        String xml = "<root>"
                + "<string>Hello, XML!</string>"
                + "<number type=\"double\">12345.678</number>"
                + "<boolean type=\"boolean\">true</boolean>"
                + "<null>null</null>"
                + "<object>"
                + "    <nestedString>Inside XML</nestedString>"
                + "    <nestedNumber type=\"integer\">9876</nestedNumber>"
                + "</object>"
                + "<array type=\"list\">"
                + "    <item>item1</item>"
                + "    <item type=\"integer\">234</item>"
                + "    <item type=\"boolean\">false</item>"
                + "    <item>null</item>"
                + "</array>"
                + "</root>";

        XMLStreamReader reader = XMLInputFactory.newDefaultFactory().createXMLStreamReader( new StringReader( xml ) );
        reader.next();
        PolyValue value = converter.nodeToPolyDocument( reader );
        assertTrue( value.isDocument() );
        PolyDocument polyDoc = value.asDocument();

        assertEquals( "Hello, XML!", polyDoc.get( new PolyString( "string" ) ).asString().getValue() );
        assertEquals( 12345.678, polyDoc.get( new PolyString( "number" ) ).asDouble().getValue() );
        assertEquals( true, polyDoc.get( new PolyString( "boolean" ) ).asBoolean().getValue() );
        assertTrue( polyDoc.get( new PolyString( "null" ) ).isNull() );

        // Check nested object
        assertTrue( polyDoc.get( new PolyString( "object" ) ).isMap() );
        PolyMap<PolyValue, PolyValue> nestedObject = polyDoc.get( new PolyString( "object" ) ).asMap();
        assertEquals( "Inside XML", nestedObject.get( new PolyString( "nestedString" ) ).asString().getValue() );
        assertEquals( 9876, nestedObject.get( new PolyString( "nestedNumber" ) ).asLong().getValue() );

        // Check array
        assertTrue( polyDoc.get( new PolyString( "array" ) ).isList() );
        PolyList<PolyValue> list = polyDoc.get( new PolyString( "array" ) ).asList();
        assertTrue( list.get( 0 ).isString() );
        assertTrue( list.get( 1 ).isLong() );
        assertTrue( list.get( 2 ).isBoolean() );
        assertTrue( list.get( 3 ).isNull() );

        assertEquals( "item1", list.get( 0 ).asString().getValue() );
        assertEquals( 234, list.get( 1 ).asLong().getValue() );
        assertEquals( false, list.get( 2 ).asBoolean().getValue() );
        assertTrue( list.get( 3 ).isNull() );
    }


    @Test
    public void testDate() throws XMLStreamException {
        String xml = "<root><date type=\"date\">2024-08-07</date></root>";
        XMLStreamReader reader = XMLInputFactory.newDefaultFactory().createXMLStreamReader( new StringReader( xml ) );
        reader.next();
        PolyValue value = converter.nodeToPolyDocument( reader );
        assertTrue( value.isDocument() );
        PolyDocument polyDoc = value.asDocument();
        assertEquals( 1722988800000L, polyDoc.get( new PolyString( "date" ) ).asDate().millisSinceEpoch );
    }


    @Test
    public void testTime() throws XMLStreamException {
        String xml = "<root><time type=\"time\">13:45:30</time></root>";
        XMLStreamReader reader = XMLInputFactory.newDefaultFactory().createXMLStreamReader( new StringReader( xml ) );
        reader.next();
        PolyValue value = converter.nodeToPolyDocument( reader );
        assertTrue( value.isDocument() );
        PolyDocument polyDoc = value.asDocument();
        assertEquals( 49530000, polyDoc.get( new PolyString( "time" ) ).asTime().ofDay );
    }


    @Test
    public void testDateTime() throws XMLStreamException {
        String xml = "<root><dateTime type=\"dateTime\">2024-08-07T13:45:30</dateTime></root>";
        XMLStreamReader reader = XMLInputFactory.newDefaultFactory().createXMLStreamReader( new StringReader( xml ) );
        reader.next();
        PolyValue value = converter.nodeToPolyDocument( reader );
        assertTrue( value.isDocument() );
        PolyDocument polyDoc = value.asDocument();
        assertEquals( 1723038330000L, polyDoc.get( new PolyString( "dateTime" ) ).asTimestamp().millisSinceEpoch );
    }


    @Test
    public void testBase64Binary() throws XMLStreamException {
        String xml = "<root><binary type=\"base64Binary\">SGVsbG8sIFdvcmxkIQ==</binary></root>";
        XMLStreamReader reader = XMLInputFactory.newDefaultFactory().createXMLStreamReader( new StringReader( xml ) );
        reader.next();
        PolyValue value = converter.nodeToPolyDocument( reader );
        assertTrue( value.isDocument() );
        PolyDocument polyDoc = value.asDocument();
        assertEquals( "Hello, World!", new String( polyDoc.get( new PolyString( "binary" ) ).asBinary().getValue() ) );
    }


    @Test
    public void testHexBinary() throws XMLStreamException {
        String xml = "<root><binary type=\"hexBinary\">48656c6c6f2c20576f726c6421</binary></root>";
        XMLStreamReader reader = XMLInputFactory.newDefaultFactory().createXMLStreamReader( new StringReader( xml ) );
        reader.next();
        PolyValue value = converter.nodeToPolyDocument( reader );
        assertTrue( value.isDocument() );
        PolyDocument polyDoc = value.asDocument();
        assertEquals( "Hello, World!", new String( polyDoc.get( new PolyString( "binary" ) ).asBinary().getValue() ) );
    }

}
