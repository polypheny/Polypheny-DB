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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.polypheny.db.protointerface.proto.IndexedParameters;
import org.polypheny.db.protointerface.proto.ProtoBigDecimal;
import org.polypheny.db.protointerface.proto.ProtoValue;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;

public class ProtoValueDeserializer {

    public static List<List<PolyValue>> deserializeParameterLists( List<IndexedParameters> parameterListsList ) {
        return transpose( parameterListsList.stream()
                .map( parameterList -> deserializeParameterList( parameterList.getParametersList() ) )
                .collect( Collectors.toList() ) );
    }


    private static List<List<PolyValue>> transpose( List<List<PolyValue>> values ) {
        int cols = values.get( 0 ).size();
        List<List<PolyValue>> transposed = new ArrayList<>();
        for ( int i = 0; i < cols; i++ ) {
            List<PolyValue> newRow = new ArrayList<>();
            for ( List<PolyValue> value : values ) {
                newRow.add( value.get( i ) );
            }
            transposed.add( newRow );
        }
        return transposed;
    }


    public static List<PolyValue> deserializeParameterList( List<ProtoValue> valuesList ) {
        return valuesList.stream().map( ProtoValueDeserializer::deserializeProtoValue ).collect( Collectors.toList() );
    }


    public static Map<String, PolyValue> deserilaizeParameterMap( Map<String, ProtoValue> valueMap ) {
        Map<String, PolyValue> deserializedValues = new HashMap<>();
        valueMap.forEach( ( name, value ) -> deserializedValues.put( name, deserializeProtoValue( value ) ) );
        return deserializedValues;
    }


    public static PolyValue deserializeProtoValue( ProtoValue protoValue ) {
        return switch ( protoValue.getValueCase() ) {
            case BOOLEAN -> deserializeToPolyBoolean( protoValue );
            case INTEGER -> deserializeToPolyInteger( protoValue );
            case LONG -> deserializeToPolyLong( protoValue );
            case BINARY -> deserializeToPolyBinary( protoValue );
            case DATE -> deserializeToPolyDate( protoValue );
            case DOUBLE -> deserializeToPolyDouble( protoValue );
            case FLOAT -> deserializeToPolyFloat( protoValue );
            case STRING -> deserializeToPolyString( protoValue );
            case TIME -> deserializeToPolyTime( protoValue );
            case TIME_STAMP -> deserializeToPolyTimeStamp( protoValue );
            case NULL -> deserializeToPolyNull( protoValue );
            case BIG_DECIMAL -> deserializeToPolyBigDecimal( protoValue );
            case LIST -> deserializeToPolyList( protoValue );
            default -> throw new RuntimeException( "Should never be thrown" );
        };
    }


    private static PolyValue deserializeToPolyList( ProtoValue protoValue ) {
        List<PolyValue> values = protoValue.getList().getValuesList().stream()
                .map( ProtoValueDeserializer::deserializeProtoValue )
                .collect( Collectors.toList() );
        return new PolyList<>( values );
    }


    private static PolyBoolean deserializeToPolyBoolean( ProtoValue protoValue ) {
        return new PolyBoolean( protoValue.getBoolean().getBoolean() );
    }


    private static PolyInteger deserializeToPolyInteger( ProtoValue protoValue ) {
        return new PolyInteger( protoValue.getInteger().getInteger() );
    }


    private static PolyLong deserializeToPolyLong( ProtoValue protoValue ) {
        return new PolyLong( protoValue.getLong().getLong() );
    }


    private static PolyBinary deserializeToPolyBinary( ProtoValue protoValue ) {
        //As poly binary's constructor uses avatica's byte string, we can't call it directly.
        return PolyBinary.of( protoValue.getBinary().getBinary().toByteArray() );
    }


    private static PolyDate deserializeToPolyDate( ProtoValue protoValue ) {
        return new PolyDate( protoValue.getDate().getDate() );
    }


    private static PolyDouble deserializeToPolyDouble( ProtoValue protoValue ) {
        return new PolyDouble( protoValue.getDouble().getDouble() );
    }


    private static PolyFloat deserializeToPolyFloat( ProtoValue protoValue ) {
        return new PolyFloat( protoValue.getFloat().getFloat() );
    }


    private static PolyString deserializeToPolyString( ProtoValue protoValue ) {
        return new PolyString( protoValue.getString().getString() );
    }


    private static PolyTime deserializeToPolyTime( ProtoValue protoValue ) {
        return new PolyTime( protoValue.getTime().getValue() );
    }


    private static PolyTimestamp deserializeToPolyTimeStamp( ProtoValue protoValue ) {
        return new PolyTimestamp( protoValue.getTimeStamp().getTimeStamp() );
    }


    private static PolyNull deserializeToPolyNull( ProtoValue protoValue ) {
        return PolyNull.NULL;
    }


    private static PolyBigDecimal deserializeToPolyBigDecimal( ProtoValue protoValue ) {
        return new PolyBigDecimal( deserializeToBigDecimal( protoValue ) );
    }


    private static BigDecimal deserializeToBigDecimal( ProtoValue protoValue ) {
        ProtoBigDecimal protoBigDecimal = protoValue.getBigDecimal();
        MathContext context = new MathContext( protoBigDecimal.getPrecision() );
        byte[] unscaledValue = protoBigDecimal.getUnscaledValue().toByteArray();
        return new BigDecimal( new BigInteger( unscaledValue ), protoBigDecimal.getScale(), context );
    }


}
