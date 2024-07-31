/*
 * Copyright 2019-2024 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.prisminterface.utils;

import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.prisminterface.streaming.StreamableBinaryWrapper;
import org.polypheny.db.prisminterface.streaming.StreamableBlobWrapper;
import org.polypheny.db.prisminterface.streaming.StreamableWrapper;
import org.polypheny.db.prisminterface.streaming.StreamingIndex;
import org.polypheny.db.prisminterface.streaming.StreamingStrategy;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyInterval;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyBlob;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyEdge.EdgeDirection;
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
import org.polypheny.prism.ProtoBigDecimal;
import org.polypheny.prism.ProtoBinary;
import org.polypheny.prism.ProtoBoolean;
import org.polypheny.prism.ProtoDate;
import org.polypheny.prism.ProtoDocument;
import org.polypheny.prism.ProtoDouble;
import org.polypheny.prism.ProtoEdge;
import org.polypheny.prism.ProtoEdge.Direction;
import org.polypheny.prism.ProtoEntry;
import org.polypheny.prism.ProtoFile;
import org.polypheny.prism.ProtoFloat;
import org.polypheny.prism.ProtoInteger;
import org.polypheny.prism.ProtoInterval;
import org.polypheny.prism.ProtoList;
import org.polypheny.prism.ProtoLong;
import org.polypheny.prism.ProtoNode;
import org.polypheny.prism.ProtoNull;
import org.polypheny.prism.ProtoPath;
import org.polypheny.prism.ProtoString;
import org.polypheny.prism.ProtoTime;
import org.polypheny.prism.ProtoTimestamp;
import org.polypheny.prism.ProtoValue;

public class PolyValueSerializer {

    // TODO: sync this with the limit in the streaming framework. Currently this value is no accessible due to it being in a different module...
    public static final int STREAM_LIMIT = 100000000; // 1 MB

    public static List<ProtoValue> serializeList( List<PolyValue> valuesList, StreamingIndex streamingIndex, StreamingStrategy streamingStrategy ) {
        return valuesList.stream().map( v -> PolyValueSerializer.serialize(v, streamingIndex, streamingStrategy  ) ).collect( Collectors.toList() );
    }


    public static List<ProtoEntry> serializeToProtoEntryList( PolyMap<PolyValue, PolyValue> polyMap, StreamingIndex streamingIndex , StreamingStrategy streamingStrategy) {
        return polyMap.entrySet().stream().map(e -> PolyValueSerializer.serializeToProtoEntry(e, streamingIndex, streamingStrategy) ).collect( Collectors.toList() );
    }


    public static ProtoEntry serializeToProtoEntry( Map.Entry<PolyValue, PolyValue> polyMapEntry, StreamingIndex streamingIndex, StreamingStrategy streamingStrategy ) {
        return ProtoEntry.newBuilder()
                .setKey( serialize( polyMapEntry.getKey(), streamingIndex, streamingStrategy ) )
                .setValue( serialize( polyMapEntry.getValue(), streamingIndex, streamingStrategy ) )
                .build();
    }


    public static ProtoValue serialize( PolyValue polyValue, StreamingIndex streamingIndex, StreamingStrategy streamingStrategy ) {
        if ( polyValue == null || polyValue.isNull() ) {
            return serializeAsProtoNull();
        }
        return switch ( polyValue.getType() ) {
            case BOOLEAN -> serializeAsProtoBoolean( polyValue.asBoolean() );
            case TINYINT, SMALLINT, INTEGER -> serializeAsProtoInteger( polyValue.asInteger() );
            case BIGINT -> serializeAsProtoLong( polyValue.asLong() );
            case DECIMAL -> serializeAsProtoBigDecimal( polyValue.asBigDecimal() );
            case REAL, FLOAT -> serializeAsProtoFloat( polyValue.asFloat() );
            case DOUBLE -> serializeAsProtoDouble( polyValue.asDouble() );
            case DATE -> serializeAsProtoDate( polyValue.asDate() );
            case TIME -> serializeAsProtoTime( polyValue.asTime() );
            case TIMESTAMP -> serializeAsProtoTimestamp( polyValue.asTimestamp() );
            case INTERVAL -> serializeAsProtoInterval( polyValue.asInterval() );
            case CHAR, VARCHAR -> serializeAsProtoString( polyValue.asString() );
            case BINARY, VARBINARY -> serializeAsProtoBinary( polyValue.asBinary(), streamingIndex, streamingStrategy );
            case NULL -> serializeAsProtoNull();
            case ARRAY -> serializeAsProtoList( polyValue.asList(), streamingIndex, streamingStrategy );
            case DOCUMENT -> serializeAsProtoDocument( polyValue.asDocument(), streamingIndex, streamingStrategy );
            case IMAGE, VIDEO, AUDIO, FILE -> serializeAsProtoFile( polyValue.asBlob(), streamingIndex, streamingStrategy ); // used
            case MAP, GRAPH, NODE, EDGE, PATH, DISTINCT, STRUCTURED, ROW, OTHER, CURSOR, COLUMN_LIST, DYNAMIC_STAR, GEOMETRY, SYMBOL, JSON, MULTISET, USER_DEFINED_TYPE, ANY -> throw new NotImplementedException( "Serialization of " + polyValue.getType() + " to proto not implemented" );
            default -> throw new NotImplementedException();
        };
    }


    private static ProtoValue serializeAsProtoDocument( PolyDocument polyDocument, StreamingIndex streamingIndex, StreamingStrategy streamingStrategy ) {
        return ProtoValue.newBuilder()
                .setDocument( buildProtoDocument( polyDocument, streamingIndex, streamingStrategy ) )
                .build();
    }


    private static ProtoValue serializeAsProtoList( PolyList<PolyValue> polyList, StreamingIndex streamingIndex, StreamingStrategy streamingStrategy ) {
        return ProtoValue.newBuilder()
                .setList( serializeToProtoList( polyList, streamingIndex, streamingStrategy ) )
                .build();

    }


    private static ProtoList serializeToProtoList( PolyList<PolyValue> polyList, StreamingIndex streamingIndex, StreamingStrategy streamingStrategy ) {
        return ProtoList.newBuilder()
                .addAllValues( serializeList( polyList.getValue(), streamingIndex, streamingStrategy ) )
                .build();
    }


    private static ProtoValue serializeAsProtoFile( PolyBlob polyBlob, StreamingIndex streamingIndex, StreamingStrategy streamingStrategy ) {
        if ( polyBlob.value == null ) {
            return serializeAsProtoNull();
        }
        ProtoFile.Builder fileBuilder = ProtoFile.newBuilder();
        if (polyBlob.getValue().length > STREAM_LIMIT || streamingStrategy == StreamingStrategy.STREAM_ALL) {
            long streamId = streamingIndex.register( new StreamableBlobWrapper( polyBlob ) );
            fileBuilder
                    .setStreamId(streamId)
                    .setIsForwardOnly(true);
        } else {
            fileBuilder.setBinary( ByteString.copyFrom( polyBlob.getValue() ) );
        }
        ProtoFile protoFile = fileBuilder.build();
        return ProtoValue.newBuilder()
                .setFile(protoFile )
                .build();
    }


    private static ProtoValue serializeAsProtoInterval( PolyInterval polyInterval ) {
        return ProtoValue.newBuilder()
                .setInterval( ProtoInterval.newBuilder().setMonths( polyInterval.getMonths() ).setMilliseconds( polyInterval.getMillis() ).build() )
                .build();
    }


    public static ProtoValue serializeAsProtoBoolean( PolyBoolean polyBoolean ) {
        if ( polyBoolean.value == null ) {
            return serializeAsProtoNull();
        }
        ProtoBoolean protoBoolean = ProtoBoolean.newBuilder()
                .setBoolean( polyBoolean.getValue() )
                .build();
        return ProtoValue.newBuilder()
                .setBoolean( protoBoolean )
                .build();
    }


    public static ProtoValue serializeAsProtoInteger( PolyInteger polyInteger ) {
        if ( polyInteger.value == null ) {
            return serializeAsProtoNull();
        }
        ProtoInteger protoInteger = ProtoInteger.newBuilder()
                .setInteger( polyInteger.getValue() )
                .build();
        return ProtoValue.newBuilder()
                .setInteger( protoInteger )
                .build();
    }


    public static ProtoValue serializeAsProtoLong( PolyLong polyLong ) {
        if ( polyLong.value == null ) {
            return serializeAsProtoNull();
        }
        ProtoLong protoLong = ProtoLong.newBuilder()
                .setLong( polyLong.value )
                .build();
        return ProtoValue.newBuilder()
                .setLong( protoLong )
                .build();
    }


    public static ProtoValue serializeAsProtoBinary( PolyBinary polyBinary, StreamingIndex streamingIndex, StreamingStrategy streamingStrategy ) {
        if ( polyBinary.value == null ) {
            return serializeAsProtoNull();
        }
        ProtoBinary.Builder binaryBuilder = ProtoBinary.newBuilder();
        if (polyBinary.getValue().length > STREAM_LIMIT || streamingStrategy == StreamingStrategy.STREAM_ALL) {
            long streamId = streamingIndex.register( new StreamableBinaryWrapper( polyBinary ) );
            binaryBuilder
                    .setStreamId(streamId)
                    .setIsForwardOnly(true);
        } else {
            binaryBuilder.setBinary( ByteString.copyFrom( polyBinary.getValue() ) );
        }
        ProtoBinary protoBinary = binaryBuilder.build();
        return ProtoValue.newBuilder()
                .setBinary( protoBinary )
                .build();
    }


    public static ProtoValue serializeAsProtoDate( PolyDate polyDate ) {
        if ( polyDate.millisSinceEpoch == null ) {
            return serializeAsProtoNull();
        }
        ProtoDate protoDate = ProtoDate.newBuilder()
                .setDate( polyDate.getDaysSinceEpoch() )
                .build();
        return ProtoValue.newBuilder()
                .setDate( protoDate )
                .build();
    }


    public static ProtoValue serializeAsProtoDouble( PolyDouble polyDouble ) {
        if ( polyDouble.value == null ) {
            return serializeAsProtoNull();
        }
        ProtoDouble protoDouble = ProtoDouble.newBuilder()
                .setDouble( polyDouble.doubleValue() )
                .build();
        return ProtoValue.newBuilder()
                .setDouble( protoDouble )
                .build();
    }


    public static ProtoValue serializeAsProtoFloat( PolyFloat polyFloat ) {
        if ( polyFloat.value == null ) {
            return serializeAsProtoNull();
        }
        ProtoFloat protoFloat = ProtoFloat.newBuilder()
                .setFloat( polyFloat.floatValue() )
                .build();
        return ProtoValue.newBuilder()
                .setFloat( protoFloat )
                .build();
    }


    public static ProtoValue serializeAsProtoString( PolyString polyString ) {
        if ( polyString.value == null ) {
            return serializeAsProtoNull();
        }
        ProtoString protoString = ProtoString.newBuilder()
                .setString( polyString.getValue() )
                .build();
        return ProtoValue.newBuilder()
                .setString( protoString )
                .build();
    }


    public static ProtoValue serializeAsProtoTime( PolyTime polyTime ) {
        if ( polyTime.ofDay == null ) {
            return serializeAsProtoNull();
        }
        ProtoTime protoTime = ProtoTime.newBuilder()
                .setTime( polyTime.getOfDay() )
                .build();
        return ProtoValue.newBuilder()
                .setTime( protoTime )
                .build();
    }


    public static ProtoValue serializeAsProtoTimestamp( PolyTimestamp polyTimestamp ) {
        if ( polyTimestamp.millisSinceEpoch == null ) {
            return serializeAsProtoNull();
        }
        ProtoTimestamp protoTimestamp = ProtoTimestamp.newBuilder()
                .setTimestamp( polyTimestamp.getMillisSinceEpoch() )
                .build();
        return ProtoValue.newBuilder()
                .setTimestamp( protoTimestamp )
                .build();
    }


    public static ProtoValue serializeAsProtoNull() {
        return ProtoValue.newBuilder()
                .setNull( ProtoNull.newBuilder().build() )
                .build();
    }


    public static ProtoValue serializeAsProtoBigDecimal( PolyBigDecimal polyBigDecimal ) {
        if ( polyBigDecimal.value == null ) {
            return serializeAsProtoNull();
        }
        ProtoBigDecimal protoBigDecimal = ProtoBigDecimal.newBuilder()
                .setUnscaledValue( ByteString.copyFrom( polyBigDecimal.getValue().unscaledValue().toByteArray() ) )
                .setScale( polyBigDecimal.getValue().scale() )
                .build();
        return ProtoValue.newBuilder()
                .setBigDecimal( protoBigDecimal )
                .build();
    }


    public static ProtoDocument buildProtoDocument( PolyDocument polyDocument, StreamingIndex streamingIndex, StreamingStrategy streamingStrategy ) {
        return ProtoDocument.newBuilder()
                .addAllEntries( serializeToProtoEntryList( polyDocument.asMap(), streamingIndex, streamingStrategy ) )
                .build();
    }


    public static ProtoNode buildProtoNode( PolyNode polyNode, StreamingIndex streamingIndex, StreamingStrategy streamingStrategy ) {
        ProtoNode.Builder node = ProtoNode.newBuilder()
                .setId( polyNode.getId().getValue() )
                .addAllLabels( polyNode.getLabels().stream().map( l -> l.getValue() ).collect( Collectors.toList() ) )
                .addAllProperties( serializeToProtoEntryList( polyNode.properties.asMap(), streamingIndex, streamingStrategy ) );
        if ( !(polyNode.variableName == null) ) {
            node.setName( polyNode.variableName.getValue() );
        }
        return node.build();
    }


    public static ProtoEdge buildProtoEdge( PolyEdge polyEdge, StreamingIndex streamingIndex, StreamingStrategy streamingStrategy ) {
        ProtoEdge.Builder edge = ProtoEdge.newBuilder()
                .setId( polyEdge.getId().getValue() )
                .addAllLabels( polyEdge.getLabels().stream().map( l -> l.getValue() ).collect( Collectors.toList() ) )
                .addAllProperties( serializeToProtoEntryList( polyEdge.properties.asMap(), streamingIndex, streamingStrategy ) )
                .setSource( polyEdge.getSource().getValue() )
                .setTarget( polyEdge.getTarget().getValue() )
                .setDirection( buildProtoEdgeDirection( polyEdge.getDirection() ) );
        if ( !(polyEdge.variableName == null) ) {
            edge.setName( polyEdge.getVariableName().getValue() );
        }
        return edge.build();
    }


    private static Direction buildProtoEdgeDirection( EdgeDirection direction ) {
        switch ( direction ) {
            case LEFT_TO_RIGHT -> {
                return Direction.LEFT_TO_RIGHT;
            }
            case RIGHT_TO_LEFT -> {
                return Direction.RIGHT_TO_LEFT;
            }
            case NONE -> {
                return Direction.NONE;
            }
        }
        return Direction.UNSPECIFIED;
    }


    public static ProtoPath buildProtoPath( PolyPath polyPath, StreamingIndex streamingIndex, StreamingStrategy streamingStrategy ) {
        /*
            TODO:   implement paths for graph results
                    1) implement the proto value ProtoPath
                    2) implement serialization here
                    3) add fields to dummy class in drivers
        */
        throw new NotImplementedException("Paths can not yet be serialized.");
    }


}
