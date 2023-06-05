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
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.protointerface.proto.ProtoBigDecimal;
import org.polypheny.db.protointerface.proto.ProtoBinary;
import org.polypheny.db.protointerface.proto.ProtoBoolean;
import org.polypheny.db.protointerface.proto.ProtoDate;
import org.polypheny.db.protointerface.proto.ProtoDouble;
import org.polypheny.db.protointerface.proto.ProtoFloat;
import org.polypheny.db.protointerface.proto.ProtoInteger;
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
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyTime;
import org.polypheny.db.type.entity.PolyTimeStamp;
import org.polypheny.db.type.entity.PolyValue;

public class PolyValueSerializer {
    public static Value serialize( PolyValue polyValue) {
        switch ( polyValue.getType() ) {
            case BOOLEAN:
                return serialize( polyValue.asBoolean() );
            case TINYINT:
            case INTEGER:
            case SMALLINT:
                return serialize( polyValue.asInteger() );
            case BIGINT:
                throw new NotImplementedException("serialization of type BIGINT as PolyBigDecimal is not supported");
            case DECIMAL:
                throw new NotImplementedException("serialization of type DECIMAL as PolyBigDecimal is not supported");
            case FLOAT:
            case REAL:
                return serialize( polyValue.asFloat() );
            case DOUBLE:
                return serialize( polyValue.asDouble() );
            case DATE:
                return serialize( polyValue.asDate() );
            case TIME:
                return serialize( polyValue.asTime() );
            case TIME_WITH_LOCAL_TIME_ZONE:
                throw new NotImplementedException("serialization of type TIME_WITH_LOCAL_TIME_ZONE as PolyTime is not supported");
            case TIMESTAMP:
                return serialize( polyValue.asTimeStamp() );
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                throw new NotImplementedException("serialization of type TIMESTAMP_WITH_LOCAL_TIME_ZONE as PolyTimeStamp is not supported");
            case INTERVAL_YEAR:
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
                return serialize(polyValue.asInterval());
            case CHAR:
            case VARCHAR:
                return serialize( polyValue.asString() );
            case BINARY:
            case VARBINARY:
                return serialize( polyValue.asBinary() );
            case NULL:
                return serialize( polyValue.asNull() );
            case ANY:
                throw new NotImplementedException("serialization of type ANY as PolyValue is not supported");
            case SYMBOL:
                throw new NotImplementedException("serialization of type SYMBOL as PolySymbol is not supported");
            case MULTISET:
                throw new NotImplementedException("serialization of type MULTISET as PolyList is not supported");
            case ARRAY:
                throw new NotImplementedException("serialization of type MULTISET as PolyList is not supported");
            case MAP:
                throw new NotImplementedException("serialization of type MAP as PolyMap is not supported");
            case DOCUMENT:
                throw new NotImplementedException("serialization of type DOCUMENT as PolyDocument is not supported");
            case GRAPH:
                throw new NotImplementedException("serialization of type GRAPH as PolyGraph is not supported");
            case NODE:
                throw new NotImplementedException("serialization of type NODE as PolyNode is not supported");
            case EDGE:
                throw new NotImplementedException("serialization of type EDGE as PolyEdge is not supported");
            case PATH:
                throw new NotImplementedException("serialization of type PATH as PolyPath is not supported");
            case DISTINCT:
                throw new NotImplementedException("serialization of type DISTINCT as PolyValue is not supported");
            case STRUCTURED:
                throw new NotImplementedException("serialization of type DISTINCT as PolyValue is not supported");
            case ROW:
                throw new NotImplementedException("serialization of type ROW as PolyList is not supported");
            case OTHER:
                throw new NotImplementedException("serialization of type OTHER as PolyValue is not supported");
            case CURSOR:
                throw new NotImplementedException("serialization of type CURSOR as PolyValue is not supported");
            case COLUMN_LIST:
                throw new NotImplementedException("serialization of type COLUMN_LIST as PolyList is not supported");
            case DYNAMIC_STAR:
                throw new NotImplementedException("serialization of type COLUMN_STAR as PolyValue is not supported");
            case GEOMETRY:
                throw new NotImplementedException("serialization of type GEOMETRY as PolyValue is not supported");
            case FILE:
                throw new NotImplementedException("serialization of type FILE");
            case IMAGE:
                throw new NotImplementedException("serialization of type FILE");
            case VIDEO:
                throw new NotImplementedException("serialization of type FILE");
            case AUDIO:
                throw new NotImplementedException("serialization of type FILE");
            case JSON:
                throw new NotImplementedException("serialization of type JSON as PolyString is not supported");
        }
        throw new NotImplementedException();
    }

    public static Value serialize( PolyBoolean polyBoolean ) {
        ProtoBoolean protoBoolean = ProtoBoolean.newBuilder()
                .setBoolean( polyBoolean.getValue() )
                .build();
        return  Value.newBuilder()
                .setBoolean( protoBoolean )
                .build();
    }

    public static Value serialize( PolyInteger polyInteger) {
        ProtoInteger protoInteger = ProtoInteger.newBuilder()
                .setInteger( polyInteger.getValue() )
                .build();
        return Value.newBuilder()
                .setInteger( protoInteger )
                .build();
    }

    public static Value serialize( PolyLong polyLong ) {
        ProtoLong protoLong = ProtoLong.newBuilder()
                .setLong( polyLong.value )
                .build();
        return Value.newBuilder()
                .setLong( protoLong )
                .build();
    }

    public static Value serialize( PolyBinary polyBinary ) {
        ProtoBinary protoBinary = ProtoBinary.newBuilder()
                .setBinary( ByteString.copyFrom( polyBinary.getValue().getBytes() ) )
                .build();
        return Value.newBuilder()
                .setBinary( protoBinary )
                .build();
    }

    public static Value serialize( PolyDate polyDate ) {
        ProtoDate protoDate = ProtoDate.newBuilder()
                .setDate( polyDate.getValue() )
                .build();
        return Value.newBuilder()
                .setDate( protoDate )
                .build();
    }

    public static Value serialize( PolyDouble polyDouble ) {
        ProtoDouble protoDouble = ProtoDouble.newBuilder()
                .setDouble( polyDouble.getValue() )
                .build();
        return Value.newBuilder()
                .setDouble( protoDouble )
                .build();
    }

    private static final String PROTO_TYPE_PREFIX = "PROTO_VALUE_TYPE_";


    public static ProtoValue serialize( PolyValue polyValue ) {
        switch ( polyValue.getType() ) {
            case BOOLEAN:
                return serialize( polyValue.asBoolean() );
            case TINYINT:
            case INTEGER:
            case SMALLINT:
                return serialize( polyValue.asInteger() );
            case BIGINT:
                return serialize(polyValue.asLong());
            case DECIMAL:
                return serialize(polyValue.asBigDecimal());
            case FLOAT:
            case REAL:
                return serialize( polyValue.asFloat() );
            case DOUBLE:
                return serialize( polyValue.asDouble() );
            case DATE:
                return serialize( polyValue.asDate() );
            case TIME:
                return serialize( polyValue.asTime() );
            case TIME_WITH_LOCAL_TIME_ZONE:
                throw new NotImplementedException( "serialization of type TIME_WITH_LOCAL_TIME_ZONE as PolyTime is not supported" );
            case TIMESTAMP:
                return serialize( polyValue.asTimeStamp() );
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                throw new NotImplementedException( "serialization of type TIMESTAMP_WITH_LOCAL_TIME_ZONE as PolyTimeStamp is not supported" );
            case INTERVAL_YEAR:
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
                return serialize( polyValue.asInterval() );
            case CHAR:
            case VARCHAR:
                return serialize( polyValue.asString() );
            case BINARY:
            case VARBINARY:
                return serialize( polyValue.asBinary() );
            case NULL:
                return serialize( polyValue.asNull() );
            case ANY:
                throw new NotImplementedException( "serialization of type ANY as PolyValue is not supported" );
            case SYMBOL:
                throw new NotImplementedException( "serialization of type SYMBOL as PolySymbol is not supported" );
            case MULTISET:
                throw new NotImplementedException( "serialization of type MULTISET as PolyList is not supported" );
            case ARRAY:
                throw new NotImplementedException( "serialization of type MULTISET as PolyList is not supported" );
            case MAP:
                throw new NotImplementedException( "serialization of type MAP as PolyMap is not supported" );
            case DOCUMENT:
                throw new NotImplementedException( "serialization of type DOCUMENT as PolyDocument is not supported" );
            case GRAPH:
                throw new NotImplementedException( "serialization of type GRAPH as PolyGraph is not supported" );
            case NODE:
                throw new NotImplementedException( "serialization of type NODE as PolyNode is not supported" );
            case EDGE:
                throw new NotImplementedException( "serialization of type EDGE as PolyEdge is not supported" );
            case PATH:
                throw new NotImplementedException( "serialization of type PATH as PolyPath is not supported" );
            case DISTINCT:
                throw new NotImplementedException( "serialization of type DISTINCT as PolyValue is not supported" );
            case STRUCTURED:
                throw new NotImplementedException( "serialization of type DISTINCT as PolyValue is not supported" );
            case ROW:
                throw new NotImplementedException( "serialization of type ROW as PolyList is not supported" );
            case OTHER:
                throw new NotImplementedException( "serialization of type OTHER as PolyValue is not supported" );
            case CURSOR:
                throw new NotImplementedException( "serialization of type CURSOR as PolyValue is not supported" );
            case COLUMN_LIST:
                throw new NotImplementedException( "serialization of type COLUMN_LIST as PolyList is not supported" );
            case DYNAMIC_STAR:
                throw new NotImplementedException( "serialization of type COLUMN_STAR as PolyValue is not supported" );
            case GEOMETRY:
                throw new NotImplementedException( "serialization of type GEOMETRY as PolyValue is not supported" );
            case FILE:
                throw new NotImplementedException( "serialization of type FILE" );
            case IMAGE:
                throw new NotImplementedException( "serialization of type FILE" );
            case VIDEO:
                throw new NotImplementedException( "serialization of type FILE" );
            case AUDIO:
                throw new NotImplementedException( "serialization of type FILE" );
            case JSON:
                throw new NotImplementedException( "serialization of type JSON as PolyString is not supported" );
        }
        throw new NotImplementedException();
    }


    private static ProtoValueType getType( PolyValue polyValue ) {
        return ProtoValueType.valueOf( PROTO_TYPE_PREFIX + polyValue.getType() );
    }


    public static ProtoValue serialize( PolyBoolean polyBoolean ) {
        ProtoBoolean protoBoolean = ProtoBoolean.newBuilder()
                .setBoolean( polyBoolean.getValue() )
                .build();
        return ProtoValue.newBuilder()
                .setBoolean( protoBoolean )
                .setType( getType( polyBoolean ) )
                .build();
    }


    public static ProtoValue serialize( PolyInteger polyInteger ) {
        ProtoInteger protoInteger = ProtoInteger.newBuilder()
                .setInteger( polyInteger.getValue() )
                .build();
        return ProtoValue.newBuilder()
                .setInteger( protoInteger )
                .setType( getType( polyInteger ) )
                .build();
    }


    public static ProtoValue serialize( PolyLong polyLong ) {
        ProtoLong protoLong = ProtoLong.newBuilder()
                .setLong( polyLong.value )
                .build();
        return ProtoValue.newBuilder()
                .setLong( protoLong )
                .setType( getType( polyLong ) )
                .build();
    }


    public static ProtoValue serialize( PolyBinary polyBinary ) {
        ProtoBinary protoBinary = ProtoBinary.newBuilder()
                .setBinary( ByteString.copyFrom( polyBinary.getValue().getBytes() ) )
                .build();
        return ProtoValue.newBuilder()
                .setBinary( protoBinary )
                .setType( getType( polyBinary ) )
                .build();
    }


    public static ProtoValue serialize( PolyDate polyDate ) {
        ProtoDate protoDate = ProtoDate.newBuilder()
                .setDate( polyDate.getSinceEpoch() )
                .build();
        return ProtoValue.newBuilder()
                .setDate( protoDate )
                .setType( getType( polyDate ) )
                .build();
    }


    public static ProtoValue serialize( PolyDouble polyDouble ) {
        ProtoDouble protoDouble = ProtoDouble.newBuilder()
                .setDouble( polyDouble.doubleValue() )
                .build();
        return ProtoValue.newBuilder()
                .setDouble( protoDouble )
                .setType( getType( polyDouble ) )
                .build();
    }


    public static ProtoValue serialize( PolyFloat polyFloat ) {
        ProtoFloat protoFloat = ProtoFloat.newBuilder()
                .setFloat( polyFloat.floatValue() )
                .build();
        return ProtoValue.newBuilder()
                .setFloat( protoFloat )
                .setType( getType( polyFloat ) )
                .build();
    }


    public static ProtoValue serialize( PolyString polyString ) {
        ProtoString protoString = ProtoString.newBuilder()
                .setString( polyString.getValue() )
                .build();
        return ProtoValue.newBuilder()
                .setString( protoString )
                .setType( getType( polyString ) )
                .build();
    }


    public static ProtoValue serialize( PolyTime polyTime ) {
        ProtoTime protoTime = ProtoTime.newBuilder()
                .setValue( polyTime.getSinceEpoch() )
                .setTimeUnit( TimeUnit.valueOf( polyTime.getTimeUnit().name() ) )
                .build();
        return ProtoValue.newBuilder()
                .setTime( protoTime )
                .setType( getType( polyTime ) )
                .build();
    }


    public static ProtoValue serialize( PolyTimeStamp polyTimeStamp ) {
        ProtoTimeStamp protoTimeStamp = ProtoTimeStamp.newBuilder()
                .setTimeStamp( polyTimeStamp.asLong().longValue() )
                .build();
        return ProtoValue.newBuilder()
                .setTimeStamp( protoTimeStamp )
                .setType( getType( polyTimeStamp ) )
                .build();
    }


    public static ProtoValue serialize( PolyNull polyNull ) {
        return ProtoValue.newBuilder()
                .setNull( ProtoNull.newBuilder().build() )
                .setType( getType( polyNull ) )
                .build();
    }


    public static ProtoValue serialize( PolyBigDecimal polyBigDecimal ) {
        ProtoBigDecimal protoBigDecimal = ProtoBigDecimal.newBuilder()
                .setUnscaledValue( ByteString.copyFrom( polyBigDecimal.getValue().unscaledValue().toByteArray() ) )
                .setScale( polyBigDecimal.getValue().scale() )
                .setPrecision( polyBigDecimal.getValue().precision() )
                .build();
        return ProtoValue.newBuilder()
                .setBigDecimal( protoBigDecimal )
                .setType( getType( polyBigDecimal ) )
                .build();
    }

}
