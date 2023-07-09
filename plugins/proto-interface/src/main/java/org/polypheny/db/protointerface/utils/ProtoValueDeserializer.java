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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.TimeUnit;
import org.polypheny.db.protointerface.proto.ParameterList;
import org.polypheny.db.protointerface.proto.ProtoBigDecimal;
import org.polypheny.db.protointerface.proto.ProtoTime;
import org.polypheny.db.protointerface.proto.ProtoValue;
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

public class ProtoValueDeserializer {

    public static List<List<PolyValue>> deserializeParameterLists( List<ParameterList> parameterListsList ) {
        return parameterListsList.stream()
                .map( parameterList -> deserializeParameterList( parameterList.getParametersList() ) )
                .collect( Collectors.toList() );
    }


    public static List<PolyValue> deserializeParameterList( List<ProtoValue> valuesList ) {
        return valuesList.stream().map( ProtoValueDeserializer::deserializeProtoValue ).collect( Collectors.toList() );
    }


    public static Map<String, PolyValue> deserilaizeValueMap( Map<String, ProtoValue> valueMap ) {
        Map<String, PolyValue> deserializedValues = new HashMap<>();
        valueMap.forEach( ( name, value ) -> deserializedValues.put( name, deserializeProtoValue( value ) ) );
        return deserializedValues;
    }


    public static PolyValue deserializeProtoValue( ProtoValue protoValue ) {
        switch ( protoValue.getValueCase() ) {
            case BOOLEAN:
                return deserializeToPolyBoolean( protoValue );
            case INTEGER:
                return deserializeToPolyInteger( protoValue );
            case LONG:
                return deserializeToPolyLong( protoValue );
            case BINARY:
                return deserializeToPolyBinary( protoValue );
            case DATE:
                return deserializeToPolyDate( protoValue );
            case DOUBLE:
                return deserializeToPolyDouble( protoValue );
            case FLOAT:
                return deserializeToPolyFloat( protoValue );
            case STRING:
                return deserializeToPolyString( protoValue );
            case TIME:
                return deserializeToPolyTime( protoValue );
            case TIME_STAMP:
                return deserializeToPolyTimeStamp( protoValue );
            case NULL:
                return deserializeToPolyNull( protoValue );
            case BIG_DECIMAL:
                return deserializeToPolyBigDecimal( protoValue );
        }
        throw new RuntimeException( "Should never be thrown" );
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
        TimeUnit timeUnit = getTimeUnit( protoValue.getTime().getTimeUnit() );
        return new PolyTime( protoValue.getTime().getValue(), timeUnit );
    }


    private static PolyTimeStamp deserializeToPolyTimeStamp( ProtoValue protoValue ) {
        return new PolyTimeStamp( protoValue.getTimeStamp().getTimeStamp() );
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


    private static TimeUnit getTimeUnit( ProtoTime.TimeUnit timeUnit ) {
        return TimeUnit.valueOf( timeUnit.name() );
    }

}
