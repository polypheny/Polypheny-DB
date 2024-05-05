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

package org.polypheny.db.protointerface.utils;

import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.prism.ProtoBigDecimal;
import org.polypheny.prism.ProtoBinary;
import org.polypheny.prism.ProtoBoolean;
import org.polypheny.prism.ProtoDate;
import org.polypheny.prism.ProtoDocument;
import org.polypheny.prism.ProtoDouble;
import org.polypheny.prism.ProtoEntry;
import org.polypheny.prism.ProtoFloat;
import org.polypheny.prism.ProtoInteger;
import org.polypheny.prism.ProtoInterval;
import org.polypheny.prism.ProtoList;
import org.polypheny.prism.ProtoLong;
import org.polypheny.prism.ProtoNull;
import org.polypheny.prism.ProtoString;
import org.polypheny.prism.ProtoTime;
import org.polypheny.prism.ProtoTimestamp;
import org.polypheny.prism.ProtoValue;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyInterval;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyBlob;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;

public class PolyValueSerializer {


    public static List<ProtoValue> serializeList( List<PolyValue> valuesList ) {
        return valuesList.stream().map( PolyValueSerializer::serialize ).collect( Collectors.toList() );
    }


    public static List<ProtoEntry> serializeToProtoEntryList( PolyMap<PolyValue, PolyValue> polyMap ) {
        return polyMap.entrySet().stream().map( PolyValueSerializer::serializeToProtoEntry ).collect( Collectors.toList() );
    }


    public static ProtoEntry serializeToProtoEntry( Map.Entry<PolyValue, PolyValue> polyMapEntry ) {
        return ProtoEntry.newBuilder()
                .setKey( serialize( polyMapEntry.getKey() ) )
                .setValue( serialize( polyMapEntry.getValue() ) )
                .build();
    }


    public static ProtoValue serialize( PolyValue polyValue ) {
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
            case BINARY, VARBINARY -> serializeAsProtoBinary( polyValue.asBinary() );
            case NULL -> serializeAsProtoNull();
            case ARRAY -> serializeAsProtoList( polyValue.asList() );
            case DOCUMENT -> serializeAsProtoDocument( polyValue.asDocument() );
            case IMAGE, VIDEO, AUDIO, FILE -> serializeAsProtoFile( polyValue.asBlob() ); // used
            case MAP, GRAPH, NODE, EDGE, PATH, DISTINCT, STRUCTURED, ROW, OTHER, CURSOR, COLUMN_LIST, DYNAMIC_STAR, GEOMETRY, SYMBOL, JSON, MULTISET, USER_DEFINED_TYPE, ANY -> throw new NotImplementedException( "Serialization of " + polyValue.getType() + " to proto not implemented" );
            default -> throw new NotImplementedException();
        };
    }


    public static ProtoDocument buildProtoDocument( PolyDocument polyDocument ) {
        return ProtoDocument.newBuilder()
                .addAllEntries( serializeToProtoEntryList( polyDocument.asMap() ) )
                .build();
    }


    private static ProtoValue serializeAsProtoDocument( PolyDocument polyDocument ) {
        return ProtoValue.newBuilder()
                .setDocument( buildProtoDocument( polyDocument ) )
                .build();
    }


    private static ProtoValue serializeAsProtoList( PolyList<PolyValue> polyList ) {
        return ProtoValue.newBuilder()
                .setList( serializeToProtoList( polyList ) )
                .build();

    }


    private static ProtoList serializeToProtoList( PolyList<PolyValue> polyList ) {
        return ProtoList.newBuilder()
                .addAllValues( serializeList( polyList.getValue() ) )
                .build();
    }


    private static ProtoValue serializeAsProtoFile( PolyBlob polyBlob ) {
        ProtoBinary protoBinary = ProtoBinary.newBuilder()
                .setBinary( ByteString.copyFrom( polyBlob.getValue() ) )
                .build();
        return ProtoValue.newBuilder()
                .setBinary( protoBinary )
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


    public static ProtoValue serializeAsProtoBinary( PolyBinary polyBinary ) {
        if ( polyBinary.value == null ) {
            return serializeAsProtoNull();
        }
        ProtoBinary protoBinary = ProtoBinary.newBuilder()
                .setBinary( ByteString.copyFrom( polyBinary.getValue() ) )
                .build();
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


    private static ProtoValue serializeAsProtoNull() {
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

}
