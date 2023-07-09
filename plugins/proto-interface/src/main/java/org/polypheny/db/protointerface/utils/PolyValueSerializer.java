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
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.protointerface.proto.ProtoBigDecimal;
import org.polypheny.db.protointerface.proto.ProtoBinary;
import org.polypheny.db.protointerface.proto.ProtoBoolean;
import org.polypheny.db.protointerface.proto.ProtoDate;
import org.polypheny.db.protointerface.proto.ProtoDouble;
import org.polypheny.db.protointerface.proto.ProtoFloat;
import org.polypheny.db.protointerface.proto.ProtoInteger;
import org.polypheny.db.protointerface.proto.ProtoInterval;
import org.polypheny.db.protointerface.proto.ProtoLong;
import org.polypheny.db.protointerface.proto.ProtoNull;
import org.polypheny.db.protointerface.proto.ProtoString;
import org.polypheny.db.protointerface.proto.ProtoTime;
import org.polypheny.db.protointerface.proto.ProtoTimeStamp;
import org.polypheny.db.protointerface.proto.ProtoValue;
import org.polypheny.db.protointerface.proto.ProtoValueType;
import org.polypheny.db.protointerface.proto.TimeUnit;
import org.polypheny.db.type.entity.PolyBigDecimal;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyDate;
import org.polypheny.db.type.entity.PolyDouble;
import org.polypheny.db.type.entity.PolyFloat;
import org.polypheny.db.type.entity.PolyInteger;
import org.polypheny.db.type.entity.PolyInterval;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyTime;
import org.polypheny.db.type.entity.PolyTimeStamp;
import org.polypheny.db.type.entity.PolyValue;

public class PolyValueSerializer {

    private static final String PROTO_TYPE_PREFIX = "PROTO_VALUE_TYPE_";


    public static List<ProtoValue> serializeList( List<PolyValue> valuesList ) {
        return valuesList.stream().map( PolyValueSerializer::serialize ).collect( Collectors.toList() );
    }


    public static ProtoValue serialize( PolyValue polyValue ) {
        switch ( polyValue.getType() ) {
            case BOOLEAN:
                // used by PolyBoolean
                return serializeAsProtoBoolean( polyValue.asBoolean() );
            case INTEGER:
                // used by PolyInteger
                return serializeAsProtoInteger( polyValue.asInteger() );
            case BIGINT:
                //used by PolyLong
                return serializeAsProtoLong( polyValue.asLong() );
            case DECIMAL:
                // used by PolyBigDecimal
                return serializeAsProtoBigDecimal( polyValue.asBigDecimal() );
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
                //used by PolyTime
                return serializeAsProtoTime( polyValue.asTime() );
            case TIMESTAMP:
                //used by PolyTimeStamp
                return serializeAsProtoTimeStamp( polyValue.asTimeStamp() );
            case INTERVAL_SECOND:
                //used by PolyInterval
            case INTERVAL_MINUTE_SECOND:
                //used by PolyInterval
            case INTERVAL_MINUTE:
                //used by PolyInterval
            case INTERVAL_HOUR_SECOND:
                //used by PolyInterval
            case INTERVAL_HOUR_MINUTE:
                //used by PolyInterval
            case INTERVAL_HOUR:
                //used by PolyInterval
            case INTERVAL_DAY_SECOND:
                //used by PolyInterval
            case INTERVAL_DAY_MINUTE:
                //used by PolyInterval
            case INTERVAL_DAY_HOUR:
                //used by PolyInterval
            case INTERVAL_DAY:
                //used by PolyInterval
            case INTERVAL_MONTH:
                //used by PolyInterval
            case INTERVAL_YEAR_MONTH:
                //used by PolyInterval
            case INTERVAL_YEAR:
                return serializeAsProtoInterval( polyValue.asInterval() );
            case VARCHAR:
                // used by PolyString
                return serializeAsProtoString( polyValue.asString() );
            case BINARY:
                // used by PolyBinary
                return serializeAsProtoBinary( polyValue.asBinary() );
            case NULL:
                // used by PolyNull
                return serializeAsProtoNull( polyValue.asNull() );
            case SYMBOL:
                // used by PolySymbol
                throw new NotImplementedException( "serialization of type SYMBOL as PolySymbol is not supported" );
            case ARRAY:
                // used by PolyList
                throw new NotImplementedException( "serialization of type MULTISET as PolyList is not supported" );
            case MAP:
                // used by PolyDictionary
                //used by PolyMap
                throw new NotImplementedException( "serialization of type MAP as PolyMap is not supported" );
            case DOCUMENT:
                //used by PolyDocument
                throw new NotImplementedException( "serialization of type DOCUMENT as PolyDocument is not supported" );
            case GRAPH:
                //used by PolyGraph
                throw new NotImplementedException( "serialization of type GRAPH as PolyGraph is not supported" );
            case NODE:
                //used by PolyNode
                throw new NotImplementedException( "serialization of type NODE as PolyNode is not supported" );
            case EDGE:
                //used by PolyEdge
                throw new NotImplementedException( "serialization of type EDGE as PolyEdge is not supported" );
            case PATH:
                //used by PolyPath
                throw new NotImplementedException( "serialization of type PATH as PolyPath is not supported" );
            case FILE:
                // used by PolyFile
                // used by PolyStream
                throw new NotImplementedException( "serialization of type FILE" );
            case USER_DEFINED_TYPE:
                // used by PolyUserDefinedType
                throw new NotImplementedException( "serialization of type USER_DEFINED_TYPE as PolyString is not supported" );
        }
        throw new NotImplementedException();
    }


    private static ProtoValue serializeAsProtoInterval( PolyInterval polyInterval ) {
        ProtoInterval protoInterval = ProtoInterval.newBuilder()
                .setValue(serializeBigDecimal( polyInterval.getValue() ))
                .build();
        return  ProtoValue.newBuilder()
                .setInterval( protoInterval )
                .setType( getType( polyInterval ) )
                .build();
    }


    private static ProtoValueType getType( PolyValue polyValue ) {
        return ProtoValueType.valueOf( PROTO_TYPE_PREFIX + polyValue.getType() );
    }


    public static ProtoValue serializeAsProtoBoolean( PolyBoolean polyBoolean ) {
        ProtoBoolean protoBoolean = ProtoBoolean.newBuilder()
                .setBoolean( polyBoolean.getValue() )
                .build();
        return ProtoValue.newBuilder()
                .setBoolean( protoBoolean )
                .setType( getType( polyBoolean ) )
                .build();
    }


    public static ProtoValue serializeAsProtoInteger( PolyInteger polyInteger ) {
        ProtoInteger protoInteger = ProtoInteger.newBuilder()
                .setInteger( polyInteger.getValue() )
                .build();
        return ProtoValue.newBuilder()
                .setInteger( protoInteger )
                .setType( getType( polyInteger ) )
                .build();
    }


    public static ProtoValue serializeAsProtoLong( PolyLong polyLong ) {
        ProtoLong protoLong = ProtoLong.newBuilder()
                .setLong( polyLong.value )
                .build();
        return ProtoValue.newBuilder()
                .setLong( protoLong )
                .setType( getType( polyLong ) )
                .build();
    }


    public static ProtoValue serializeAsProtoBinary( PolyBinary polyBinary ) {
        ProtoBinary protoBinary = ProtoBinary.newBuilder()
                .setBinary( ByteString.copyFrom( polyBinary.getValue().getBytes() ) )
                .build();
        return ProtoValue.newBuilder()
                .setBinary( protoBinary )
                .setType( getType( polyBinary ) )
                .build();
    }


    public static ProtoValue serializeAsProtoDate( PolyDate polyDate ) {
        ProtoDate protoDate = ProtoDate.newBuilder()
                .setDate( polyDate.getSinceEpoch() )
                .build();
        return ProtoValue.newBuilder()
                .setDate( protoDate )
                .setType( getType( polyDate ) )
                .build();
    }


    public static ProtoValue serializeAsProtoDouble( PolyDouble polyDouble ) {
        ProtoDouble protoDouble = ProtoDouble.newBuilder()
                .setDouble( polyDouble.doubleValue() )
                .build();
        return ProtoValue.newBuilder()
                .setDouble( protoDouble )
                .setType( getType( polyDouble ) )
                .build();
    }


    public static ProtoValue serializeAsProtoFloat( PolyFloat polyFloat ) {
        ProtoFloat protoFloat = ProtoFloat.newBuilder()
                .setFloat( polyFloat.floatValue() )
                .build();
        return ProtoValue.newBuilder()
                .setFloat( protoFloat )
                .setType( getType( polyFloat ) )
                .build();
    }


    public static ProtoValue serializeAsProtoString( PolyString polyString ) {
        ProtoString protoString = ProtoString.newBuilder()
                .setString( polyString.getValue() )
                .build();
        return ProtoValue.newBuilder()
                .setString( protoString )
                .setType( getType( polyString ) )
                .build();
    }


    public static ProtoValue serializeAsProtoTime( PolyTime polyTime ) {
        ProtoTime protoTime = ProtoTime.newBuilder()
                .setValue( polyTime.ofDay )
                .setTimeUnit( TimeUnit.valueOf( polyTime.getTimeUnit().name() ) )
                .build();
        return ProtoValue.newBuilder()
                .setTime( protoTime )
                .setType( getType( polyTime ) )
                .build();
    }


    public static ProtoValue serializeAsProtoTimeStamp( PolyTimeStamp polyTimeStamp ) {
        ProtoTimeStamp protoTimeStamp = ProtoTimeStamp.newBuilder()
                .setTimeStamp( polyTimeStamp.asLong().longValue() )
                .build();
        return ProtoValue.newBuilder()
                .setTimeStamp( protoTimeStamp )
                .setType( getType( polyTimeStamp ) )
                .build();
    }


    public static ProtoValue serializeAsProtoNull( PolyNull polyNull ) {
        return ProtoValue.newBuilder()
                .setNull( ProtoNull.newBuilder().build() )
                .setType( getType( polyNull ) )
                .build();
    }


    public static ProtoValue serializeAsProtoBigDecimal( PolyBigDecimal polyBigDecimal ) {
        ProtoBigDecimal protoBigDecimal = serializeBigDecimal( polyBigDecimal.getValue() );
        return ProtoValue.newBuilder()
                .setBigDecimal( protoBigDecimal )
                .setType( getType( polyBigDecimal ) )
                .build();
    }


    private static ProtoBigDecimal serializeBigDecimal( BigDecimal bigDecimal ) {
        return ProtoBigDecimal.newBuilder()
                .setUnscaledValue( ByteString.copyFrom( bigDecimal.unscaledValue().toByteArray() ) )
                .setScale( bigDecimal.scale() )
                .setPrecision( bigDecimal.precision() )
                .build();
    }

}
