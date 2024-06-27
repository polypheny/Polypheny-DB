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

package org.polypheny.db.adapter.xml;


import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.codec.binary.Hex;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class XmlToPolyConverter {

    public PolyDocument nodeToPolyDocument( Node node ) {
        return new PolyDocument( nodeToPolyMap( node ) );
    }


    public PolyMap<PolyString, PolyValue> nodeToPolyMap( Node node ) {
        Map<PolyString, PolyValue> map = new HashMap<>();
        NodeList childNodes = node.getChildNodes();
        for ( int i = 0; i < childNodes.getLength(); i++ ) {
            Node child = childNodes.item( i );
            if ( child.getNodeType() == Node.ELEMENT_NODE ) {
                PolyString key = new PolyString( child.getNodeName() );
                PolyValue value = nodeToPolyValue( child );
                map.put( key, value );
            }
        }
        return PolyMap.of( map );
    }


    public PolyValue nodeToPolyValue( Node node ) {
        return switch ( node.getNodeType() ) {
            case Node.ELEMENT_NODE -> {
                String type = node.getAttributes().getNamedItem( "type" ) != null
                        ? node.getAttributes().getNamedItem( "type" ).getNodeValue()
                        : "string";
                yield convertByType( node, type );
            }
            case Node.TEXT_NODE -> new PolyString( node.getNodeValue().trim() );
            case Node.ATTRIBUTE_NODE -> new PolyString( node.getNodeValue() );
            default -> new PolyNull();
        };
    }


    private PolyValue convertByType( Node node, String type ) {
        String value = node.getTextContent().trim();
        return switch ( type ) {
            case "boolean" -> new PolyBoolean( Boolean.parseBoolean( value ) );
            case "integer" -> new PolyLong( Long.parseLong( value ) );
            case "decimal" -> new PolyBigDecimal( new java.math.BigDecimal( value ) );
            case "float" -> new PolyFloat( Float.parseFloat( value ) );
            case "double" -> new PolyDouble( Double.parseDouble( value ) );
            case "date" -> {
                LocalDate date = LocalDate.parse( value, DateTimeFormatter.ISO_DATE );
                yield new PolyDate( date.atStartOfDay().toInstant( java.time.ZoneOffset.UTC ).toEpochMilli() );
            }
            case "time" -> {
                LocalTime time = LocalTime.parse( value, DateTimeFormatter.ISO_TIME );
                yield new PolyTime( (int) (time.toNanoOfDay() / 1000000) );
            }
            case "dateTime" -> {
                LocalDateTime dateTime = LocalDateTime.parse( value, DateTimeFormatter.ISO_DATE_TIME );
                yield new PolyTimestamp( dateTime.toInstant( java.time.ZoneOffset.UTC ).toEpochMilli() );
            }
            case "base64Binary" -> {
                byte[] binaryData = Base64.getDecoder().decode( value );
                yield new PolyBinary( binaryData, binaryData.length );
            }
            case "hexBinary" -> {
                try {
                    byte[] hexBinaryData = Hex.decodeHex( value.toCharArray() );
                    yield new PolyBinary( hexBinaryData, hexBinaryData.length );
                } catch ( Exception e ) {
                    yield new PolyString( value );  // Handle error case appropriately
                }
            }
            default -> new PolyString( value );
        };
    }


    public PolyValue nodeToPolyList( Node node ) {
        NodeList childNodes = node.getChildNodes();
        return IntStream.range( 0, childNodes.getLength() )
                .mapToObj( childNodes::item )
                .map( this::nodeToPolyValue )
                .collect( Collectors.toCollection( PolyList::new ) );
    }


}
