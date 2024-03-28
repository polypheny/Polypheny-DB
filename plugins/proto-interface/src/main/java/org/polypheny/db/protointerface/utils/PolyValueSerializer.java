/*
 * Copyright 2019-2023 The Polypheny Project
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
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.protointerface.proto.ProtoBigDecimal;
import org.polypheny.db.protointerface.proto.ProtoBinary;
import org.polypheny.db.protointerface.proto.ProtoBoolean;
import org.polypheny.db.protointerface.proto.ProtoDate;
import org.polypheny.db.protointerface.proto.ProtoDocument;
import org.polypheny.db.protointerface.proto.ProtoDouble;
import org.polypheny.db.protointerface.proto.ProtoEntry;
import org.polypheny.db.protointerface.proto.ProtoFloat;
import org.polypheny.db.protointerface.proto.ProtoInteger;
import org.polypheny.db.protointerface.proto.ProtoInterval;
import org.polypheny.db.protointerface.proto.ProtoList;
import org.polypheny.db.protointerface.proto.ProtoLong;
import org.polypheny.db.protointerface.proto.ProtoNull;
import org.polypheny.db.protointerface.proto.ProtoPolyType;
import org.polypheny.db.protointerface.proto.ProtoString;
import org.polypheny.db.protointerface.proto.ProtoTime;
import org.polypheny.db.protointerface.proto.ProtoTimestamp;
import org.polypheny.db.protointerface.proto.ProtoValue;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.PolyInterval;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyBlob;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.relational.PolyMap;

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
        if ( polyValue == null ) {
            return serializeAsProtoNull();
        }
        switch ( polyValue.getType() ) {
            case BOOLEAN:
                // used by PolyBoolean
                return serializeAsProtoBoolean( polyValue.asBoolean() );
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                // used by PolyInteger
                return serializeAsProtoInteger( polyValue.asInteger() );
            case BIGINT:
                // used by PolyLong
                return serializeAsProtoLong( polyValue.asLong() );
            case DECIMAL:
                // used by PolyBigDecimal
                return serializeAsProtoBigDecimal( polyValue.asBigDecimal() );
            case REAL:
            case FLOAT:
                // used by PolyFloat
                return serializeAsProtoFloat( polyValue.asFloat() );
            case DOUBLE:
                // used by PolyDouble
                return serializeAsProtoDouble( polyValue.asDouble() );
            case DATE:
                // used by PolyDate
                return serializeAsProtoDate( polyValue.asDate() );
            case TIME:
                // used by PolyTime
            case TIME_WITH_LOCAL_TIME_ZONE:
                return serializeAsProtoTime( polyValue.asTime() );
            case TIMESTAMP:
                // used by PolyTimeStamp
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return serializeAsProtoTimestamp( polyValue.asTimestamp() );
            case INTERVAL_SECOND:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY:
            case INTERVAL_MONTH:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_YEAR:
                return serializeAsProtoInterval( polyValue.asInterval() );
            case CHAR:
            case VARCHAR:
                // used by PolyString
                return serializeAsProtoString( polyValue.asString() );
            case BINARY:
            case VARBINARY:
                // used by PolyBinary
                return serializeAsProtoBinary( polyValue.asBinary() );
            case NULL:
                // used by PolyNull
                return serializeAsProtoNull( polyValue.asNull() );
            case ARRAY:
                // used by PolyList
                return serializeAsProtoList( polyValue.asList() );
            case DOCUMENT:
                // used by PolyDocument
                return serializeAsProtoDocument( polyValue.asDocument() );
            case IMAGE:
            case VIDEO:
            case AUDIO:
            case FILE:
                // used by PolyFile
                return serializeAsProtoFile( polyValue.asBlob() );
            case MAP:
            case GRAPH:
            case NODE:
            case EDGE:
            case PATH:
            case DISTINCT:
            case STRUCTURED:
            case ROW:
            case OTHER:
            case CURSOR:
            case COLUMN_LIST:
            case DYNAMIC_STAR:
            case GEOMETRY:
            case SYMBOL: // used
            case JSON: // used
            case MULTISET:
            case USER_DEFINED_TYPE:
            case ANY:
                throw new NotImplementedException( "Serialization of " + polyValue.getType() + " to proto not implemented" );
        }
        throw new NotImplementedException();
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


    private static ProtoInterval getInterval( PolyInterval polyInterval ) {
        switch ( polyInterval.getType() ) {
            case INTERVAL_SECOND:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY:
                return ProtoInterval.newBuilder().setMilliseconds( polyInterval.getValue().longValue() ).build();
            case INTERVAL_MONTH:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_YEAR:
                return ProtoInterval.newBuilder().setMonths( polyInterval.getValue().longValue() ).build();
            default:
                throw new GenericRuntimeException( "Invalid type for PolyInterval: " + polyInterval.getType().getTypeName() );
        }
    }


    private static ProtoValue serializeAsProtoInterval( PolyInterval polyInterval ) {
        return ProtoValue.newBuilder()
                .setInterval( getInterval( polyInterval ) )
                .build();
    }


    private static ProtoPolyType getType( PolyValue polyValue ) {
        return getType( polyValue.getType() );
    }


    private static ProtoPolyType getType( PolyType polyType ) {
        return ProtoPolyType.valueOf( polyType.getName() );
    }


    public static ProtoValue serializeAsProtoBoolean( PolyBoolean polyBoolean ) {
        ProtoBoolean protoBoolean = ProtoBoolean.newBuilder()
                .setBoolean( polyBoolean.getValue() )
                .build();
        return ProtoValue.newBuilder()
                .setBoolean( protoBoolean )
                .build();
    }


    public static ProtoValue serializeAsProtoInteger( PolyInteger polyInteger ) {
        ProtoInteger protoInteger = ProtoInteger.newBuilder()
                .setInteger( polyInteger.getValue() )
                .build();
        return ProtoValue.newBuilder()
                .setInteger( protoInteger )
                .build();
    }


    public static ProtoValue serializeAsProtoLong( PolyLong polyLong ) {
        ProtoLong protoLong = ProtoLong.newBuilder()
                .setLong( polyLong.value )
                .build();
        return ProtoValue.newBuilder()
                .setLong( protoLong )
                .build();
    }


    public static ProtoValue serializeAsProtoBinary( PolyBinary polyBinary ) {
        ProtoBinary protoBinary = ProtoBinary.newBuilder()
                .setBinary( ByteString.copyFrom( polyBinary.getValue() ) )
                .build();
        return ProtoValue.newBuilder()
                .setBinary( protoBinary )
                .build();
    }


    public static ProtoValue serializeAsProtoDate( PolyDate polyDate ) {
        ProtoDate protoDate = ProtoDate.newBuilder()
                .setDate( polyDate.getDaysSinceEpoch() )
                .build();
        return ProtoValue.newBuilder()
                .setDate( protoDate )
                .build();
    }


    public static ProtoValue serializeAsProtoDouble( PolyDouble polyDouble ) {
        ProtoDouble protoDouble = ProtoDouble.newBuilder()
                .setDouble( polyDouble.doubleValue() )
                .build();
        return ProtoValue.newBuilder()
                .setDouble( protoDouble )
                .build();
    }


    public static ProtoValue serializeAsProtoFloat( PolyFloat polyFloat ) {
        ProtoFloat protoFloat = ProtoFloat.newBuilder()
                .setFloat( polyFloat.floatValue() )
                .build();
        return ProtoValue.newBuilder()
                .setFloat( protoFloat )
                .build();
    }


    public static ProtoValue serializeAsProtoString( PolyString polyString ) {
        return ProtoValue.newBuilder()
                .setString( serializeToProtoString( polyString ) )
                .build();
    }


    public static ProtoString serializeToProtoString( PolyString polyString ) {
        return ProtoString.newBuilder()
                .setString( polyString.getValue() )
                .build();
    }


    public static ProtoValue serializeAsProtoTime( PolyTime polyTime ) {
        ProtoTime protoTime = ProtoTime.newBuilder()
                .setTime( polyTime.ofDay )
                .build();
        return ProtoValue.newBuilder()
                .setTime( protoTime )
                .build();
    }


    public static ProtoValue serializeAsProtoTimestamp( PolyTimestamp polyTimestamp ) {
        ProtoTimestamp protoTimestamp = ProtoTimestamp.newBuilder()
                .setTimestamp( polyTimestamp.getMillisSinceEpoch() )
                .build();
        return ProtoValue.newBuilder()
                .setTimestamp( protoTimestamp )
                .build();
    }


    public static ProtoValue serializeAsProtoNull( PolyNull polyNull ) {
        return ProtoValue.newBuilder()
                .setNull( ProtoNull.newBuilder().build() )
                .build();
    }


    private static ProtoValue serializeAsProtoNull() {
        return ProtoValue.newBuilder()
                .setNull( ProtoNull.newBuilder().build() )
                .build();
    }


    public static ProtoValue serializeAsProtoBigDecimal( PolyBigDecimal polyBigDecimal ) {
        ProtoBigDecimal protoBigDecimal = serializeBigDecimal( polyBigDecimal.getValue() );
        return ProtoValue.newBuilder()
                .setBigDecimal( protoBigDecimal )
                .build();
    }


    private static ProtoBigDecimal serializeBigDecimal( BigDecimal bigDecimal ) {
        return ProtoBigDecimal.newBuilder()
                .setUnscaledValue( ByteString.copyFrom( bigDecimal.unscaledValue().toByteArray() ) )
                .setScale( bigDecimal.scale() )
                .build();
    }

}
