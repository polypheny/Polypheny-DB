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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.prisminterface.streaming.StreamSink;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyBlob;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.prism.IndexedParameters;
import org.polypheny.prism.ProtoBigDecimal;
import org.polypheny.prism.ProtoBinary;
import org.polypheny.prism.ProtoFile;
import org.polypheny.prism.ProtoValue;
import org.polypheny.prism.ProtoValue.ValueCase;

public class PrismValueDeserializer {

    public static List<List<PolyValue>> deserializeParameterLists( List<IndexedParameters> parameterListsList, StreamSink streamSink ) {
        return transpose( parameterListsList.stream()
                .map( parameterList -> deserializeParameterList( parameterList.getParametersList(), streamSink ) )
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


    public static List<PolyValue> deserializeParameterList( List<ProtoValue> valuesList, StreamSink streamSink ) {
        return valuesList.stream().map( l -> PrismValueDeserializer.deserializeProtoValue(l, streamSink) ).collect( Collectors.toList() );
    }


    public static Map<String, PolyValue> deserilaizeParameterMap( Map<String, ProtoValue> valueMap, StreamSink streamSink ) {
        Map<String, PolyValue> deserializedValues = new HashMap<>();
        valueMap.forEach( ( name, value ) -> deserializedValues.put( name, deserializeProtoValue( value, streamSink ) ) );
        return deserializedValues;
    }


    public static PolyValue deserializeProtoValue( ProtoValue protoValue, StreamSink streamSink ) {
        return switch ( protoValue.getValueCase() ) {
            case BOOLEAN -> deserializeToPolyBoolean( protoValue );
            case INTEGER -> deserializeToPolyInteger( protoValue );
            case LONG -> deserializeToPolyLong( protoValue );
            case BIG_DECIMAL -> deserializeToPolyBigDecimal( protoValue );
            case FLOAT -> deserializeToPolyFloat( protoValue );
            case DOUBLE -> deserializeToPolyDouble( protoValue );
            case DATE -> deserializeToPolyDate( protoValue );
            case TIME -> deserializeToPolyTime( protoValue );
            case TIMESTAMP -> deserializeToPolyTimestamp( protoValue );
            case STRING -> deserializeToPolyString( protoValue );
            case BINARY -> deserializeToPolyBinary( protoValue, streamSink );
            case NULL -> deserializeToPolyNull();
            case LIST -> deserializeToPolyList( protoValue, streamSink );
            case FILE -> deserializeToPolyBlob( protoValue, streamSink );
            case DOCUMENT -> deserializeToPolyDocument( protoValue, streamSink );
            case VALUE_NOT_SET -> throw new GenericRuntimeException( "Invalid ProtoValue: no value is set" );
            default -> throw new GenericRuntimeException( "Deserialization of type " + protoValue.getValueCase() + " is not supported" );
        };
    }


    private static PolyValue deserializeToPolyDocument( ProtoValue protoValue, StreamSink streamSink ) {
        PolyDocument document = new PolyDocument();
        protoValue.getDocument().getEntriesList().stream()
                .filter( e -> e.getKey().getValueCase() == ValueCase.STRING )
                .forEach( e -> document.put(
                        new PolyString( e.getKey().getString().getString() ),
                        deserializeProtoValue( e.getValue(), streamSink )
                ) );
        return document;
    }


    private static PolyValue deserializeToPolyBlob( ProtoValue protoValue, StreamSink streamSink ) {
        ProtoFile protoFile = protoValue.getFile();
        if (protoFile.hasBinary()) {
            return PolyBlob.of( protoValue.getFile().getBinary().toByteArray() );
        }
        return new PolyBlob(null, streamSink.getStream( protoFile.getStreamId() ).getBinaryInputStream());

    }


    private static PolyValue deserializeToPolyList( ProtoValue protoValue, StreamSink streamSink ) {
        List<PolyValue> values = protoValue.getList().getValuesList().stream()
                .map( v -> PrismValueDeserializer.deserializeProtoValue(v, streamSink) )
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


    private static PolyBinary deserializeToPolyBinary( ProtoValue protoValue, StreamSink streamSink ) {
        ProtoBinary protoBinary = protoValue.getBinary();
        if (protoBinary.hasBinary()) {
            // As poly binary's constructor uses avatica's byte string, we can't call it directly.
            return PolyBinary.of( protoBinary.getBinary().toByteArray() );
        }
        try {
            // As a poly binary stores it's value as a byte array we know that the stream will fit into one as well.
            byte[] data = streamSink.getStream( protoBinary.getStreamId() ).getBinaryInputStream().readAllBytes();
            return PolyBinary.of( data );
        } catch (IOException e) {
            throw new RuntimeException("Failed to read binary from prism stream", e);
        }
    }


    private static PolyDate deserializeToPolyDate( ProtoValue protoValue ) {
        return PolyDate.ofDays( (int) protoValue.getDate().getDate() );
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
        return new PolyTime( protoValue.getTime().getTime() );
    }


    private static PolyTimestamp deserializeToPolyTimestamp( ProtoValue protoValue ) {
        return new PolyTimestamp( protoValue.getTimestamp().getTimestamp() );
    }


    private static PolyNull deserializeToPolyNull() {
        return PolyNull.NULL;
    }


    private static PolyBigDecimal deserializeToPolyBigDecimal( ProtoValue protoValue ) {
        return new PolyBigDecimal( deserializeToBigDecimal( protoValue ) );
    }


    private static BigDecimal deserializeToBigDecimal( ProtoValue protoValue ) {
        ProtoBigDecimal protoBigDecimal = protoValue.getBigDecimal();
        byte[] unscaledValue = protoBigDecimal.getUnscaledValue().toByteArray();
        return new BigDecimal( new BigInteger( unscaledValue ), protoBigDecimal.getScale() );
    }

}
