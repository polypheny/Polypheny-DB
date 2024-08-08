package org.polypheny.db.adapter.xml;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.apache.commons.codec.DecoderException;
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

public class XmlToPolyConverter {

    public PolyDocument toPolyDocument( XMLStreamReader reader, String elementOuterName ) throws XMLStreamException {
        return new PolyDocument( toPolyMap( reader, elementOuterName ) );
    }


    private PolyMap<PolyString, PolyValue> toPolyMap( XMLStreamReader reader, String elementOuterName ) throws XMLStreamException {
        Map<PolyString, PolyValue> map = new HashMap<>();
        int event;
        while ( reader.hasNext() ) {
            if ( reader.getEventType() != XMLStreamConstants.START_ELEMENT ) {
                event = reader.next();
            } else {
                event = reader.getEventType();
            }
            if ( event == XMLStreamConstants.START_ELEMENT ) {
                PolyString key = new PolyString( reader.getLocalName() );
                PolyValue value = toPolyValue( reader );
                map.put( key, value );
                continue;
            }
            if ( event == XMLStreamConstants.END_ELEMENT && reader.getLocalName().equals( elementOuterName ) ) {
                break;
            }
        }
        return PolyMap.of( map );
    }


    public PolyValue toPolyValue( XMLStreamReader reader ) throws XMLStreamException {
        String currentElementName = reader.getLocalName();
        String typeName = reader.getAttributeValue( null, "type" );
        if ( typeName == null ) {
            typeName = "string";
        }

        if ( !reader.hasNext() ) {
            throw new XMLStreamException( "Unexpected end of stream." );
        }
        reader.next();
        if (reader.getEventType() == XMLStreamConstants.END_ELEMENT) {
            return new PolyNull();
        }
        String value = reader.getText().trim();
        if ( value.isEmpty() ) {
            if ( typeName.equals( "list" ) ) {
                reader.next(); // skip empty value between list body and first element
                return toPolyList(reader, currentElementName);
            }
            if ( typeName.equals( "string" ) ) { // This is "string" as nested documents or null values don't have a type specified and are auto typed as string ;)
                reader.next(); // skip empty value between list body and first element
                return toPolyMap( reader, currentElementName );
            }
        }

        return switch ( typeName ) {
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
                } catch ( DecoderException e ) {
                    throw new RuntimeException( "Failed to parse hexadecimal data.", e );
                }
            }
            case "string" -> new PolyString( value );
            default -> throw new RuntimeException( "Illegal type encountered: " + typeName );
        };
    }


    private PolyValue toPolyList( XMLStreamReader reader, String listOuterName ) throws XMLStreamException {
        List<PolyValue> values = new ArrayList<>();
        while(!reader.getLocalName().equals( listOuterName) ) {
            values.add( toPolyValue( reader ) );
            if (!reader.hasNext()) {
                throw new XMLStreamException( "Unexpected end of stream." );
            }
            reader.next(); // skip empty characters element
            reader.next(); // get to next start element
            reader.next(); // skip empty characters element
        }
        return new PolyList<>( values );
    }

    /*

    private PolyValue convertByType( XMLStreamReader reader, String type ) throws XMLStreamException {
        StringBuilder value = new StringBuilder();
        while ( reader.hasNext() ) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                if (!type.equals( "list" )) {
                    return toPolyMap( reader );
                }
                break;
            } else if ( event == XMLStreamConstants.CHARACTERS ) {
                value.append( reader.getText().trim() );
            } else if ( event == XMLStreamConstants.END_ELEMENT ) {
                break;
            }
        }

        if ( value.length() == 0 && !type.equals("list" )) {
            return new PolyNull();
        }

        String valueStr = value.toString();
        if (valueStr.equals( "null" )) {
            return new PolyNull();
        }

        return switch ( type ) {
            case "boolean" -> new PolyBoolean( Boolean.parseBoolean( valueStr ) );
            case "integer" -> new PolyLong( Long.parseLong( valueStr ) );
            case "decimal" -> new PolyBigDecimal( new java.math.BigDecimal( valueStr ) );
            case "float" -> new PolyFloat( Float.parseFloat( valueStr ) );
            case "double" -> new PolyDouble( Double.parseDouble( valueStr ) );
            case "date" -> {
                LocalDate date = LocalDate.parse( valueStr, DateTimeFormatter.ISO_DATE );
                yield new PolyDate( date.atStartOfDay().toInstant( java.time.ZoneOffset.UTC ).toEpochMilli() );
            }
            case "time" -> {
                LocalTime time = LocalTime.parse( valueStr, DateTimeFormatter.ISO_TIME );
                yield new PolyTime( (int) (time.toNanoOfDay() / 1000000) );
            }
            case "dateTime" -> {
                LocalDateTime dateTime = LocalDateTime.parse( valueStr, DateTimeFormatter.ISO_DATE_TIME );
                yield new PolyTimestamp( dateTime.toInstant( java.time.ZoneOffset.UTC ).toEpochMilli() );
            }
            case "base64Binary" -> {
                byte[] binaryData = Base64.getDecoder().decode( valueStr );
                yield new PolyBinary( binaryData, binaryData.length );
            }
            case "hexBinary" -> {
                try {
                    byte[] hexBinaryData = Hex.decodeHex( valueStr.toCharArray() );
                    yield new PolyBinary( hexBinaryData, hexBinaryData.length );
                } catch ( Exception e ) {
                    yield new PolyString( valueStr );
                }
            }
            case "list" -> nodeToPolyList( reader );
            default -> new PolyString( valueStr );
        };
    }


    public PolyValue nodeToPolyList(XMLStreamReader reader) throws XMLStreamException {
        PolyList<PolyValue> list = new PolyList<>();
        String currentElement = reader.getLocalName();

        while (reader.hasNext()) {
            int event = reader.getEventType();
            if (event == XMLStreamConstants.START_ELEMENT && reader.getLocalName().equals("item")) {
                list.add(nodeToPolyValue(reader));
            } else if (event == XMLStreamConstants.END_ELEMENT && reader.getLocalName().equals(currentElement)) {
                break;
            }
            reader.next();
        }
        return list;
    }



*/

}
