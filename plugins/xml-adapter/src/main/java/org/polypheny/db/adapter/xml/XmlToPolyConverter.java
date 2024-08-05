package org.polypheny.db.adapter.xml;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.codec.binary.Hex;
import org.polypheny.db.type.entity.*;
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

    public PolyDocument nodeToPolyDocument(XMLStreamReader reader) throws XMLStreamException {
        return new PolyDocument(nodeToPolyMap(reader));
    }

    public PolyMap<PolyString, PolyValue> nodeToPolyMap(XMLStreamReader reader) throws XMLStreamException {
        Map<PolyString, PolyValue> map = new HashMap<>();
        String currentElement = reader.getLocalName(); // Root element of the current document
        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                PolyString key = new PolyString(reader.getLocalName());
                PolyValue value = nodeToPolyValue(reader);
                map.put(key, value);
            } else if (event == XMLStreamConstants.END_ELEMENT && reader.getLocalName().equals(currentElement)) {
                break; // Stop processing at the end of the current document element
            }
        }
        return PolyMap.of(map);
    }

    public PolyValue nodeToPolyValue(XMLStreamReader reader) throws XMLStreamException {
        if (reader.getEventType() == XMLStreamConstants.START_ELEMENT) {
            String type = reader.getAttributeValue(null, "type");
            return convertByType(reader, type != null ? type : "string");
        }
        return new PolyNull(); // Default case for unsupported nodes
    }

    private PolyValue convertByType(XMLStreamReader reader, String type) throws XMLStreamException {
        StringBuilder value = new StringBuilder();
        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.CHARACTERS) {
                value.append(reader.getText().trim());
            } else if (event == XMLStreamConstants.END_ELEMENT) {
                break;
            }
        }

        if (value.length() == 0) {
            return new PolyNull();
        }

        String valueStr = value.toString();
        return switch (type) {
            case "boolean" -> new PolyBoolean(Boolean.parseBoolean(valueStr));
            case "integer" -> new PolyLong(Long.parseLong(valueStr));
            case "decimal" -> new PolyBigDecimal(new java.math.BigDecimal(valueStr));
            case "float" -> new PolyFloat(Float.parseFloat(valueStr));
            case "double" -> new PolyDouble(Double.parseDouble(valueStr));
            case "date" -> {
                LocalDate date = LocalDate.parse(valueStr, DateTimeFormatter.ISO_DATE);
                yield new PolyDate(date.atStartOfDay().toInstant(java.time.ZoneOffset.UTC).toEpochMilli());
            }
            case "time" -> {
                LocalTime time = LocalTime.parse(valueStr, DateTimeFormatter.ISO_TIME);
                yield new PolyTime((int) (time.toNanoOfDay() / 1000000));
            }
            case "dateTime" -> {
                LocalDateTime dateTime = LocalDateTime.parse(valueStr, DateTimeFormatter.ISO_DATE_TIME);
                yield new PolyTimestamp(dateTime.toInstant(java.time.ZoneOffset.UTC).toEpochMilli());
            }
            case "base64Binary" -> {
                byte[] binaryData = Base64.getDecoder().decode(valueStr);
                yield new PolyBinary(binaryData, binaryData.length);
            }
            case "hexBinary" -> {
                try {
                    byte[] hexBinaryData = Hex.decodeHex(valueStr.toCharArray());
                    yield new PolyBinary(hexBinaryData, hexBinaryData.length);
                } catch (Exception e) {
                    yield new PolyString(valueStr); // Fallback in case of error
                }
            }
            case "list" -> nodeToPolyList(reader);
            default -> new PolyString(valueStr); // Default to string if no type is specified
        };
    }

    public PolyValue nodeToPolyList(XMLStreamReader reader) {
        // Collect all attributes or child elements as list items
        return IntStream.range(0, reader.getAttributeCount())
                .mapToObj(i -> new PolyString(reader.getAttributeValue(i)))
                .collect( Collectors.toCollection(PolyList::new));
    }
}
