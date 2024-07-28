package org.polypheny.db.prisminterface.streaming;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyInterval;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyBlob;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;

public class SerializationHeuristic {

    private static final int STREAM_LIMIT = 256;

    public static int estimateSizeRows(List<PolyValue> rows) {
        return estimateSizeList( rows );
    }

    public static int estimateSizeDocument(PolyDocument document) {
        return estimateSizeProtoEntryList( document.asMap() );
    }

    public static int estimateSizeProtoNode( PolyNode polyNode) {
        int size = 0;
        size += 1 + computeStringSize(polyNode.getId().getValue()); // id
        size += polyNode.getLabels().stream().mapToInt(label -> 1 + computeStringSize(label.getValue())).sum(); // labels
        size += estimateSizeProtoEntryList(polyNode.properties.asMap()); // properties
        if (polyNode.variableName != null) {
            size += 1 + computeStringSize(polyNode.variableName.getValue()); // variable name
        }
        return size;
    }

    public static int estimateSizeProtoEdge( PolyEdge polyEdge) {
        int size = 0;
        size += 1 + computeStringSize(polyEdge.getId().getValue()); // id
        size += polyEdge.getLabels().stream().mapToInt(label -> 1 + computeStringSize(label.getValue())).sum(); // labels
        size += estimateSizeProtoEntryList(polyEdge.properties.asMap()); // properties
        size += 1 + computeStringSize(polyEdge.getSource().getValue()); // source
        size += 1 + computeStringSize(polyEdge.getTarget().getValue()); // target
        size += 1; // direction enum size
        if (polyEdge.variableName != null) {
            size += 1 + computeStringSize(polyEdge.variableName.getValue()); // variable name
        }
        return size;
    }

    public static int estimateSizeProtoPath( PolyPath polyPath) {
        throw new NotImplementedException("Paths can not yet be serialized.");
    }

    public static int estimateSizeList(List<PolyValue> valuesList) {
        return valuesList.stream().mapToInt(SerializationHeuristic::estimateSize).sum();
    }

    public static int estimateSizeProtoEntryList(PolyMap<PolyValue, PolyValue> polyMap) {
        return polyMap.entrySet().stream().mapToInt(SerializationHeuristic::estimateSizeProtoEntry).sum();
    }

    public static int estimateSizeProtoEntry(Map.Entry<PolyValue, PolyValue> polyMapEntry) {
        return estimateSize(polyMapEntry.getKey()) + estimateSize(polyMapEntry.getValue());
    }

    public static int estimateSize(PolyValue polyValue) {
        if (polyValue == null || polyValue.isNull()) {
            return estimateSizeProtoNull();
        }
        return switch (polyValue.getType()) {
            case BOOLEAN -> estimateSizeProtoBoolean(polyValue.asBoolean());
            case TINYINT, SMALLINT, INTEGER -> estimateSizeProtoInteger(polyValue.asInteger());
            case BIGINT -> estimateSizeProtoLong(polyValue.asLong());
            case DECIMAL -> estimateSizeProtoBigDecimal(polyValue.asBigDecimal());
            case REAL, FLOAT -> estimateSizeProtoFloat(polyValue.asFloat());
            case DOUBLE -> estimateSizeProtoDouble(polyValue.asDouble());
            case DATE -> estimateSizeProtoDate(polyValue.asDate());
            case TIME -> estimateSizeProtoTime(polyValue.asTime());
            case TIMESTAMP -> estimateSizeProtoTimestamp(polyValue.asTimestamp());
            case INTERVAL -> estimateSizeProtoInterval(polyValue.asInterval());
            case CHAR, VARCHAR -> estimateSizeProtoString(polyValue.asString());
            case BINARY, VARBINARY -> estimateSizeProtoBinary(polyValue.asBinary());
            case NULL -> estimateSizeProtoNull();
            case ARRAY -> estimateSizeProtoList(polyValue.asList());
            case DOCUMENT -> estimateSizeProtoDocument(polyValue.asDocument());
            case IMAGE, VIDEO, AUDIO, FILE -> estimateSizeProtoFile(polyValue.asBlob());
            default -> throw new NotImplementedException("Size estimation for " + polyValue.getType() + " to proto not implemented");
        };
    }

    private static int estimateSizeProtoDocument(PolyDocument polyDocument) {
        return estimateSizeProtoEntryList(polyDocument.asMap());
    }

    private static int estimateSizeProtoList(PolyList<PolyValue> polyList) {
        return estimateSizeList(polyList.getValue());
    }

    private static int estimateSizeProtoFile(PolyBlob polyBlob) {
        return 1 + 4 + polyBlob.getValue().length; // tag + length prefix + data size
    }

    private static int estimateSizeProtoInterval(PolyInterval polyInterval) {
        return 1 + 4 + 8; // tag + length prefix + data size
    }

    public static int estimateSizeProtoBoolean(PolyBoolean polyBoolean) {
        return 1 + 1; // tag + boolean value
    }

    public static int estimateSizeProtoInteger(PolyInteger polyInteger) {
        return 1 + computeInt32Size(polyInteger.getValue()); // tag + int32 value size
    }

    public static int estimateSizeProtoLong(PolyLong polyLong) {
        return 1 + computeInt64Size(polyLong.getValue()); // tag + int64 value size
    }

    public static int estimateSizeProtoBinary(PolyBinary polyBinary) {
            return 1 + computeInt64Size(0L); // tag + streamId size (assuming 0 for simplicity)
            //return 1 + 4 + polyBinary.getValue().length; // tag + length prefix + data size
    }

    public static int estimateSizeProtoDate(PolyDate polyDate) {
        return 1 + computeInt64Size(polyDate.getDaysSinceEpoch()); // tag + int32 value size
    }

    public static int estimateSizeProtoDouble(PolyDouble polyDouble) {
        return 1 + 8; // tag + double value
    }

    public static int estimateSizeProtoFloat(PolyFloat polyFloat) {
        return 1 + 4; // tag + float value
    }

    public static int estimateSizeProtoString(PolyString polyString) {
        return 1 + computeStringSize(polyString.getValue()); // tag + string value size
    }

    public static int estimateSizeProtoTime(PolyTime polyTime) {
        return 1 + computeInt32Size(polyTime.getOfDay()); // tag + int32 value size
    }

    public static int estimateSizeProtoTimestamp(PolyTimestamp polyTimestamp) {
        return 1 + computeInt64Size(polyTimestamp.getMillisSinceEpoch()); // tag + int64 value size
    }

    public static int estimateSizeProtoNull() {
        return 1; // tag
    }

    public static int estimateSizeProtoBigDecimal(PolyBigDecimal polyBigDecimal) {
        BigDecimal value = polyBigDecimal.getValue();
        int unscaledSize = value.unscaledValue().toByteArray().length;
        return 1 + computeInt32Size(unscaledSize) + unscaledSize + computeInt32Size(value.scale()); // tag + length + data size + scale
    }

    private static int computeInt32Size(int value) {
        if ((value & (0xffffffff << 7)) == 0) return 1;
        if ((value & (0xffffffff << 14)) == 0) return 2;
        if ((value & (0xffffffff << 21)) == 0) return 3;
        if ((value & (0xffffffff << 28)) == 0) return 4;
        return 5;
    }

    private static int computeInt64Size(long value) {
        if ((value & (0xffffffffffffffffL << 7)) == 0) return 1;
        if ((value & (0xffffffffffffffffL << 14)) == 0) return 2;
        if ((value & (0xffffffffffffffffL << 21)) == 0) return 3;
        if ((value & (0xffffffffffffffffL << 28)) == 0) return 4;
        if ((value & (0xffffffffffffffffL << 35)) == 0) return 5;
        if ((value & (0xffffffffffffffffL << 42)) == 0) return 6;
        if ((value & (0xffffffffffffffffL << 49)) == 0) return 7;
        if ((value & (0xffffffffffffffffL << 56)) == 0) return 8;
        if ((value & (0xffffffffffffffffL << 63)) == 0) return 9;
        return 10;
    }

    private static int computeStringSize(String value) {
        int length = value.getBytes().length;
        return computeInt32Size(length) + length;
    }
}
