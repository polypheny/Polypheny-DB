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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.adapter.xml.XmlToPolyConverter;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class XmlToPolyConverterTest {

    private static XmlToPolyConverter converter;

    @BeforeAll
    public static void setup() {
        converter = new XmlToPolyConverter();
        TestHelper testHelper = TestHelper.getInstance();
    }

    private Document getDocumentFromXml(String xml) throws ParserConfigurationException, IOException, SAXException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        return builder.parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testString() throws ParserConfigurationException, IOException, SAXException {
        String xml = "<root><name>Maxine</name></root>";
        Document doc = getDocumentFromXml(xml);
        PolyValue value = converter.nodeToPolyDocument(doc.getDocumentElement());
        assertTrue(value.isDocument());
        PolyDocument polyDoc = value.asDocument();
        assertEquals("Maxine", polyDoc.get(new PolyString("name")).toString());
    }

    @Test
    public void testLong() throws ParserConfigurationException, IOException, SAXException {
        String xml = "<root><integer type=\"integer\">492943</integer></root>";
        Document doc = getDocumentFromXml(xml);
        PolyValue value = converter.nodeToPolyDocument(doc.getDocumentElement());
        assertTrue(value.isDocument());
        PolyDocument polyDoc = value.asDocument();
        assertEquals(492943, polyDoc.get(new PolyString("integer")).asLong().value);
    }

    @Test
    public void testDouble() throws ParserConfigurationException, IOException, SAXException {
        String xml = "<root><double type=\"double\">-650825.13</double></root>";
        Document doc = getDocumentFromXml(xml);
        PolyValue value = converter.nodeToPolyDocument(doc.getDocumentElement());
        assertTrue(value.isDocument());
        PolyDocument polyDoc = value.asDocument();
        assertEquals(-650825.13, polyDoc.get(new PolyString("double")).asDouble().value);
    }

    @Test
    public void testBooleanTrue() throws ParserConfigurationException, IOException, SAXException {
        String xml = "<root><boolean type=\"boolean\">true</boolean></root>";
        Document doc = getDocumentFromXml(xml);
        PolyValue value = converter.nodeToPolyDocument(doc.getDocumentElement());
        assertTrue(value.isDocument());
        PolyDocument polyDoc = value.asDocument();
        assertEquals(true, polyDoc.get(new PolyString("boolean")).asBoolean().value);
    }

    @Test
    public void testBooleanFalse() throws ParserConfigurationException, IOException, SAXException {
        String xml = "<root><boolean type=\"boolean\">false</boolean></root>";
        Document doc = getDocumentFromXml(xml);
        PolyValue value = converter.nodeToPolyDocument(doc.getDocumentElement());
        assertTrue(value.isDocument());
        PolyDocument polyDoc = value.asDocument();
        assertEquals(false, polyDoc.get(new PolyString("boolean")).asBoolean().value);
    }

    @Test
    public void testArray() throws ParserConfigurationException, IOException, SAXException {
        String xml = "<root><integers type=\"list\"><item type=\"integer\">0</item><item type=\"integer\">1</item><item type=\"integer\">2</item><item type=\"integer\">3</item></integers></root>";
        Document doc = getDocumentFromXml(xml);
        PolyValue value = converter.nodeToPolyDocument(doc.getDocumentElement());
        assertTrue(value.isDocument());
        PolyDocument polyDoc = value.asDocument();
        assertTrue(polyDoc.get(new PolyString("integers")).isList());
        PolyList<PolyValue> list = polyDoc.get(new PolyString("integers")).asList();
        assertEquals(0, list.get(0).asLong().value);
        assertEquals(1, list.get(1).asLong().value);
        assertEquals(2, list.get(2).asLong().value);
        assertEquals(3, list.get(3).asLong().value);
    }

    @Test
    public void testNull() throws ParserConfigurationException, IOException, SAXException {
        String xml = "<root><null></null></root>";
        Document doc = getDocumentFromXml(xml);
        PolyValue value = converter.nodeToPolyDocument(doc.getDocumentElement());
        assertTrue(value.isDocument());
        PolyDocument polyDoc = value.asDocument();
        assertTrue(polyDoc.get(new PolyString("null")).isNull());
    }

    @Test
    public void testDocument() throws ParserConfigurationException, IOException, SAXException {
        String xml = "<root>"
                + "<string>Hello, XML!</string>"
                + "<number type=\"double\">12345.678</number>"
                + "<boolean type=\"boolean\">true</boolean>"
                + "<null>null</null>"
                + "<object>"
                + "    <nestedString>Inside XML</nestedString>"
                + "    <nestedNumber type=\"integer\">9876</nestedNumber>"
                + "</object>"
                + "<array type=\"array\">"
                + "    <item>item1</item>"
                + "    <item type=\"integer\">234</item>"
                + "    <item type=\"boolean\">false</item>"
                + "    <item>null</item>"
                + "</array>"
                + "</root>";

        Document doc = getDocumentFromXml(xml);
        PolyValue value = converter.nodeToPolyDocument(doc.getDocumentElement());
        assertTrue(value.isDocument());
        PolyDocument polyDoc = value.asDocument();

        assertEquals("Hello, XML!", polyDoc.get(new PolyString("string")).asString().getValue());
        assertEquals(12345.678, polyDoc.get(new PolyString("number")).asDouble().getValue());
        assertEquals(true, polyDoc.get(new PolyString("boolean")).asBoolean().getValue());
        assertEquals(new PolyNull(), polyDoc.get(new PolyString("null")).asNull());

        // Check nested object
        assertTrue(polyDoc.get(new PolyString("object")).isMap());
        PolyMap<PolyValue, PolyValue> nestedObject = polyDoc.get(new PolyString("object")).asMap();
        assertEquals("Inside XML", nestedObject.get(new PolyString("nestedString")).asString().getValue());
        assertEquals(9876, nestedObject.get(new PolyString("nestedNumber")).asLong().getValue());

        // Check array
        assertTrue(polyDoc.get(new PolyString("array")).isList());
        PolyList<PolyValue> list = polyDoc.get(new PolyString("array")).asList();
        assertTrue(list.get(0).isString());
        assertTrue(list.get(1).isLong());
        assertTrue(list.get(2).isBoolean());
        assertTrue(list.get(3).isNull());

        assertEquals("item1", list.get(0).asString().getValue());
        assertEquals(234, list.get(1).asLong().getValue());
        assertEquals(false, list.get(2).asBoolean().getValue());
        assertEquals(new PolyNull(), list.get(3).asNull());
    }
}
