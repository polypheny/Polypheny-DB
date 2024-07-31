package org.polypheny.db.prisminterface.streaming;

import static org.polypheny.db.prisminterface.streaming.StreamingFramework.STREAM_LIMIT;

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




    public static Estimate estimateSizeRows( List<PolyValue> rows ) {
        return estimateSizeList( rows );
    }


    public static Estimate estimateSizeDocument( PolyDocument document ) {
        return estimateSizeProtoEntryList( document.asMap() );
    }


    public static Estimate estimateSizeProtoNode( PolyNode polyNode ) {
        Estimate estimate = new Estimate();
        estimate.addToAll( 1 ).add( computeStringSize( polyNode.getId().getValue() ) ); // id
        estimate.add( polyNode.getLabels().stream().map( label -> new Estimate( 1 ).add( computeStringSize( label.getValue() ) ) ).reduce( new Estimate(), Estimate::add ) ); // labels
        estimate.add( estimateSizeProtoEntryList( polyNode.properties.asMap() ) ); // properties
        if ( polyNode.variableName != null ) {
            estimate.addToAll( 1 ).add( computeStringSize( polyNode.variableName.getValue() ) ); // variable name
        }
        return estimate;
    }


    public static Estimate estimateSizeProtoEdge( PolyEdge polyEdge ) {
        Estimate estimate = new Estimate();
        estimate.addToAll( 1 ).add( computeStringSize( polyEdge.getId().getValue() ) ); // id
        estimate.add( polyEdge.getLabels().stream().map( label -> new Estimate( 1 ).add( computeStringSize( label.getValue() ) ) ).reduce( new Estimate(), Estimate::add ) ); // labels
        estimate.add( estimateSizeProtoEntryList( polyEdge.properties.asMap() ) ); // properties
        estimate.addToAll( 1 ).add( computeStringSize( polyEdge.getSource().getValue() ) ); // source
        estimate.addToAll( 1 ).add( computeStringSize( polyEdge.getTarget().getValue() ) ); // target
        estimate.addToAll( 1 ); // direction enum size
        if ( polyEdge.variableName != null ) {
            estimate.addToAll( 1 ).add( computeStringSize( polyEdge.variableName.getValue() ) ); // variable name
        }
        return estimate;
    }


    public static Estimate estimateSizeProtoPath( PolyPath polyPath ) {
        throw new NotImplementedException( "Paths can not yet be serialized and thus their estimated size is undefined." );
    }


    public static Estimate estimateSizeList( List<PolyValue> valuesList ) {
        return valuesList.stream().map( SerializationHeuristic::estimateSize ).reduce( new Estimate(), Estimate::add );
    }


    public static Estimate estimateSizeProtoEntryList( PolyMap<PolyValue, PolyValue> polyMap ) {
        return polyMap.entrySet().stream().map( SerializationHeuristic::estimateSizeProtoEntry ).reduce( new Estimate(), Estimate::add );
    }


    public static Estimate estimateSizeProtoEntry( Map.Entry<PolyValue, PolyValue> polyMapEntry ) {
        return estimateSize( polyMapEntry.getKey() ).add( estimateSize( polyMapEntry.getValue() ) );
    }


    public static Estimate estimateSize( PolyValue polyValue ) {
        if ( polyValue == null || polyValue.isNull() ) {
            return estimateSizeProtoNull();
        }
        return switch ( polyValue.getType() ) {
            case BOOLEAN -> estimateSizeProtoBoolean( polyValue.asBoolean() );
            case TINYINT, SMALLINT, INTEGER -> estimateSizeProtoInteger( polyValue.asInteger() );
            case BIGINT -> estimateSizeProtoLong( polyValue.asLong() );
            case DECIMAL -> estimateSizeProtoBigDecimal( polyValue.asBigDecimal() );
            case REAL, FLOAT -> estimateSizeProtoFloat( polyValue.asFloat() );
            case DOUBLE -> estimateSizeProtoDouble( polyValue.asDouble() );
            case DATE -> estimateSizeProtoDate( polyValue.asDate() );
            case TIME -> estimateSizeProtoTime( polyValue.asTime() );
            case TIMESTAMP -> estimateSizeProtoTimestamp( polyValue.asTimestamp() );
            case INTERVAL -> estimateSizeProtoInterval( polyValue.asInterval() );
            case CHAR, VARCHAR -> estimateSizeProtoString( polyValue.asString() );
            case BINARY, VARBINARY -> estimateSizeProtoBinary( polyValue.asBinary() );
            case NULL -> estimateSizeProtoNull();
            case ARRAY -> estimateSizeProtoList( polyValue.asList() );
            case DOCUMENT -> estimateSizeProtoDocument( polyValue.asDocument() );
            case IMAGE, VIDEO, AUDIO, FILE -> estimateSizeProtoFile( polyValue.asBlob() );
            default -> throw new NotImplementedException( "Size estimation for " + polyValue.getType() + " to proto not implemented" );
        };
    }


    private static Estimate estimateSizeProtoDocument( PolyDocument polyDocument ) {
        return estimateSizeProtoEntryList( polyDocument.asMap() );
    }


    private static Estimate estimateSizeProtoList( PolyList<PolyValue> polyList ) {
        return estimateSizeList( polyList.getValue() );
    }


    private static Estimate estimateSizeProtoFile( PolyBlob polyBlob ) {
        // TODO: stream blobs as well
        return new Estimate( 1 + 4 + polyBlob.getValue().length ); // tag + length prefix + data size
    }


    private static Estimate estimateSizeProtoInterval( PolyInterval polyInterval ) {
        return new Estimate( 1 + 4 + 8 ); // tag + length prefix + data size
    }


    public static Estimate estimateSizeProtoBoolean( PolyBoolean polyBoolean ) {
        return new Estimate( 1 + 1 ); // tag + boolean value
    }


    public static Estimate estimateSizeProtoInteger( PolyInteger polyInteger ) {
        return new Estimate( 1 ).add( computeInt32Size( polyInteger.getValue() ) ); // tag + int32 value size
    }


    public static Estimate estimateSizeProtoLong( PolyLong polyLong ) {
        return new Estimate( 1 ).add( computeInt64Size( polyLong.getValue() ) ); // tag + int64 value size
    }


    public static Estimate estimateSizeProtoBinary( PolyBinary polyBinary ) {
        Estimate estimate = new Estimate();
        estimate.setAllStreamedLength( 1 + 10 ); // tag + streamId (64bit int)
        if ( polyBinary.getValue().length > STREAM_LIMIT ) {
            estimate.setDynamicLength( estimate.getAllStreamedLength() );
            return estimate;
        }
        estimate.setDynamicLength( 1 + 4 + polyBinary.getValue().length ); // tag + length prefix + data size
        return estimate;
    }


    public static Estimate estimateSizeProtoDate( PolyDate polyDate ) {
        return new Estimate( 1 ).add( computeInt64Size( polyDate.getDaysSinceEpoch() ) ); // tag + int32 value size
    }


    public static Estimate estimateSizeProtoDouble( PolyDouble polyDouble ) {
        return new Estimate( 1 + 8 ); // tag + 64bit double value
    }


    public static Estimate estimateSizeProtoFloat( PolyFloat polyFloat ) {
        return new Estimate( 1 + 4 ); // tag + 32 bit float value
    }


    public static Estimate estimateSizeProtoString( PolyString polyString ) {
        return new Estimate( 1 ).add( computeStringSize( polyString.getValue() ) ); // tag + string value size
    }


    public static Estimate estimateSizeProtoTime( PolyTime polyTime ) {
        return new Estimate( 1 ).add( computeInt32Size( polyTime.getOfDay() ) ); // tag + int32 value size
    }


    public static Estimate estimateSizeProtoTimestamp( PolyTimestamp polyTimestamp ) {
        return new Estimate( 1 ).add( computeInt64Size( polyTimestamp.getMillisSinceEpoch() ) ); // tag + int64 value size
    }


    public static Estimate estimateSizeProtoNull() {
        return new Estimate( 1 ); // tag
    }


    public static Estimate estimateSizeProtoBigDecimal( PolyBigDecimal polyBigDecimal ) {
        BigDecimal value = polyBigDecimal.getValue();
        int unscaledSize = value.unscaledValue().toByteArray().length;
        return new Estimate( 1 ).add( computeInt32Size( unscaledSize ) ).addToAll( unscaledSize ).add( computeInt32Size( value.scale() ) ); // tag + length + data size + scale
    }


    private static Estimate computeInt32Size( int value ) {
        if ( (value & (0xffffffff << 7)) == 0 ) {
            return new Estimate( 1 );
        }
        if ( (value & (0xffffffff << 14)) == 0 ) {
            return new Estimate( 2 );
        }
        if ( (value & (0xffffffff << 21)) == 0 ) {
            return new Estimate( 3 );
        }
        if ( (value & (0xffffffff << 28)) == 0 ) {
            return new Estimate( 4 );
        }
        return new Estimate( 5 );
    }


    private static Estimate computeInt64Size( long value ) {
        if ( (value & (0xffffffffffffffffL << 7)) == 0 ) {
            return new Estimate( 1 );
        }
        if ( (value & (0xffffffffffffffffL << 14)) == 0 ) {
            return new Estimate( 2 );
        }
        if ( (value & (0xffffffffffffffffL << 21)) == 0 ) {
            return new Estimate( 3 );
        }
        if ( (value & (0xffffffffffffffffL << 28)) == 0 ) {
            return new Estimate( 4 );
        }
        if ( (value & (0xffffffffffffffffL << 35)) == 0 ) {
            return new Estimate( 5 );
        }
        if ( (value & (0xffffffffffffffffL << 42)) == 0 ) {
            return new Estimate( 6 );
        }
        if ( (value & (0xffffffffffffffffL << 49)) == 0 ) {
            return new Estimate( 7 );
        }
        if ( (value & (0xffffffffffffffffL << 56)) == 0 ) {
            return new Estimate( 8 );
        }
        if ( (value & (0xffffffffffffffffL << 63)) == 0 ) {
            return new Estimate( 9 );
        }
        return new Estimate( 10 );
    }


    private static Estimate computeStringSize( String value ) {
        int length = value.getBytes().length;
        return computeInt32Size( length ).addToAll( length );
    }

}
