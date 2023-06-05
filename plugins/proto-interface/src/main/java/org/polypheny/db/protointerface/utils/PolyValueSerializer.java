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
import org.polypheny.db.protointerface.proto.TimeUnit;
import org.polypheny.db.protointerface.proto.Value;
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
        throw new NotImplementedException("serialization of PolyValue not implemented yet.");
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

    public static Value serialize( PolyFloat polyFloat ) {
        ProtoFloat protoFloat = ProtoFloat.newBuilder()
                .setFloat( polyFloat.getValue() )
                .build();
        return Value.newBuilder()
                .setFloat( protoFloat )
                .build();
    }

    public static Value serialize( PolyString polyString ) {
        ProtoString protoString = ProtoString.newBuilder()
                .setString( polyString.getValue() )
                .build();
        return Value.newBuilder()
                .setString( protoString )
                .build();
    }

    public static Value serialize ( PolyTime polyTime ) {
        ProtoTime protoTime = ProtoTime.newBuilder()
                .setValue( polyTime.getValue() )
                .setTimeUnit( TimeUnit.valueOf( polyTime.getTimeUnit().name() ) )
                .build();
        return Value.newBuilder()
                .setTime( protoTime )
                .build();
    }

    public static Value serialize( PolyTimeStamp polyTimeStamp ) {
        ProtoTimeStamp protoTimeStamp = ProtoTimeStamp.newBuilder()
                .setTimeStamp( polyTimeStamp.getValue() )
                .build();
        return Value.newBuilder()
                .setTimeStamp( protoTimeStamp )
                .build();
    }

    public static Value serialize( PolyNull polyNull ) {
        return Value.newBuilder()
                .setNull( ProtoNull.newBuilder().build() )
                .build();
    }
}
