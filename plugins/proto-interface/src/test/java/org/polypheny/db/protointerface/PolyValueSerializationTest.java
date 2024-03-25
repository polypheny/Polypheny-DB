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

package org.polypheny.db.protointerface;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.sql.Time;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.protointerface.proto.ProtoValue;
import org.polypheny.db.protointerface.proto.ProtoValue.ValueCase;
import org.polypheny.db.protointerface.utils.PolyValueSerializer;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyUserDefinedValue;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;

public class PolyValueSerializationTest {

    @BeforeAll
    public static void init() {
        // needed to launch polypheny
        TestHelper.getInstance();
    }


    private enum TestEnum {
        UNTESTED
    }


    public static PolyUserDefinedValue buildTestUdt() {
        Map<String, PolyType> template = new HashMap<>();
        template.put( "binaryField", PolyType.BINARY );
        template.put( "booleanField", PolyType.BOOLEAN );
        template.put( "dateField", PolyType.DATE );
        template.put( "doubleField", PolyType.DOUBLE );
        template.put( "fileField", PolyType.FILE );
        template.put( "floatField", PolyType.FLOAT );
        template.put( "integerField", PolyType.INTEGER );
        template.put( "arrayField", PolyType.ARRAY );
        template.put( "bigIntField", PolyType.BIGINT );
        template.put( "nullField", PolyType.NULL );
        template.put( "stringField", PolyType.VARCHAR );
        template.put( "symbolField", PolyType.SYMBOL );
        template.put( "timeField", PolyType.TIME );
        template.put( "timestampField", PolyType.TIMESTAMP );
        Map<String, PolyValue> value = new HashMap<>();
        value.put( "binaryField", PolyBinary.of( new byte[]{ 0x01, 0x02, 0x03 } ) );
        value.put( "booleanField", new PolyBoolean( true ) );
        value.put( "dateField", new PolyDate( 119581L ) );
        value.put( "doubleField", new PolyDouble( 123.456 ) );
        value.put( "floatField", new PolyFloat( 45.67f ) );
        value.put( "integerField", new PolyInteger( 1234 ) );
        value.put( "arrayField", new PolyList(
                        new PolyInteger( 1 ),
                        new PolyInteger( 2 ),
                        new PolyInteger( 3 ),
                        new PolyInteger( 4 )
                )
        );
        value.put( "bigIntField", new PolyLong( 1234567890L ) );
        value.put( "nullField", new PolyNull() );
        value.put( "stringField", new PolyString( "sample string" ) );
        value.put( "timeField", PolyTime.of( new Time( 1691879380700L ) ) );
        value.put( "timestampField", new PolyTimestamp( 1691879380700L ) );
        return new PolyUserDefinedValue( template, value );
    }


    @Test
    public void polyBigDecimalSeriaizationTest() {
        BigDecimal expectedValue = new BigDecimal( 1691879380700L );
        PolyBigDecimal expected = new PolyBigDecimal( expectedValue );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        MathContext context = new MathContext( protoValue.getBigDecimal().getPrecision() );
        int scale = protoValue.getBigDecimal().getScale();
        BigInteger value = new BigInteger( protoValue.getBigDecimal().getUnscaledValue().toByteArray() );
        BigDecimal result = new BigDecimal( value, scale, context );
        assertEquals( expectedValue, result );
    }


    @Test
    public void polyBinarySerializationTest() {
        PolyBinary expected = PolyBinary.of( new byte[]{ 0x01, 0x02, 0x03 } );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertArrayEquals( new byte[]{ 0x01, 0x02, 0x03 }, protoValue.getBinary().toByteArray() );
    }


    @Test
    public void polyBooleanSerializationTest() {
        PolyBoolean expected = new PolyBoolean( true );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertTrue( protoValue.getBoolean().getBoolean() );
    }


    @Test
    public void polyDateSerializationTest() {
        long daysSinceEpoch = 119581;
        PolyDate expected = new PolyDate( daysSinceEpoch );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( daysSinceEpoch, protoValue.getDate().getDate() );
    }


    @Test
    public void polyDoubleSerializationTest() {
        PolyDouble expected = new PolyDouble( 123.456 );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( 123.456, protoValue.getDouble().getDouble(), 1e-9 );
    }


    @Test
    public void polyFloatSerializationTest() {
        PolyFloat expected = new PolyFloat( 45.67f );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( 45.67f, protoValue.getFloat().getFloat(), 1e-9 );
    }


    @Test
    public void polyIntegerSerializationTest() {
        PolyInteger expected = new PolyInteger( 1234 );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( 1234, protoValue.getInteger().getInteger() );
    }


    @Test
    public void polyListSerializationTest() {
        PolyList<PolyInteger> expected = new PolyList(
                new PolyInteger( 1 ),
                new PolyInteger( 2 ),
                new PolyInteger( 3 ),
                new PolyInteger( 4 )
        );
        List<ProtoValue> protoValues = PolyValueSerializer.serialize( expected ).getList().getValuesList();
        assertEquals( 1, protoValues.get( 0 ).getInteger().getInteger() );
        assertEquals( 2, protoValues.get( 1 ).getInteger().getInteger() );
        assertEquals( 3, protoValues.get( 2 ).getInteger().getInteger() );
        assertEquals( 4, protoValues.get( 3 ).getInteger().getInteger() );
    }


    @Test
    public void polyLongSerializationTest() {
        PolyLong expected = new PolyLong( 1234567890L );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( 1234567890L, protoValue.getLong().getLong() );
    }


    @Test
    public void polyStringSerializationTest() {
        PolyString expected = new PolyString( "sample string" );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( "sample string", protoValue.getString().getString() );
    }


    @Test
    public void polyTimeSerializationTest() {
        PolyTime expected = PolyTime.of( new Time( 1691879380700L ) );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( 1691879380700L, protoValue.getTime().getTime() );
    }


    @Test
    public void polyTimeStampSerializationTest() {
        PolyTimestamp expected = new PolyTimestamp( 1691879380700L );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( 1691879380700L, protoValue.getTimestamp().getTimestamp() );
    }


    @Test
    public void polyBigDecimalSerializationCorrectTypeTest() {
        PolyBigDecimal expected = new PolyBigDecimal( new BigDecimal( 1691879380700L ) );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ValueCase.BIG_DECIMAL, protoValue.getValueCase() );
    }


    @Test
    public void polyBinarySerializationCorrectTypeTest() {
        PolyBinary expected = PolyBinary.of( new byte[]{ 0x01, 0x02, 0x03 } );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ValueCase.BINARY, protoValue.getValueCase() );
    }


    @Test
    public void polyBooleanSerializationCorrectTypeTest() {
        PolyBoolean expected = new PolyBoolean( true );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ValueCase.BOOLEAN, protoValue.getValueCase() );
    }


    @Test
    public void polyDateSerializationCorrectTypeTest() {
        long daysSinceEpoch = 119581;
        PolyDate expected = new PolyDate( daysSinceEpoch );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ValueCase.DATE, protoValue.getValueCase() );
    }


    @Test
    public void polyDoubleSerializationCorrectTypeTest() {
        PolyDouble expected = new PolyDouble( 123.456 );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ValueCase.DOUBLE, protoValue.getValueCase() );
    }


    @Test
    public void polyFloatSerializationCorrectTypeTest() {
        PolyFloat expected = new PolyFloat( 45.67f );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ValueCase.FLOAT, protoValue.getValueCase() );
    }


    @Test
    public void polyIntegerSerializationCorrectTypeTest() {
        PolyInteger expected = new PolyInteger( 1234 );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ValueCase.INTEGER, protoValue.getValueCase() );
    }


    @Test
    public void polyListSerializationCorrectTypeTest() {
        PolyList<PolyInteger> expected = new PolyList<>(
                new PolyInteger( 1 ),
                new PolyInteger( 2 ),
                new PolyInteger( 3 ),
                new PolyInteger( 4 )
        );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ValueCase.LIST, protoValue.getValueCase() );
    }


    @Test
    public void polyLongSerializationCorrectTypeTest() {
        PolyLong expected = new PolyLong( 1234567890L );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ValueCase.LONG, protoValue.getValueCase() );
    }


    @Test
    public void polyNullSerializationCorrectTypeTest() {
        PolyNull expected = new PolyNull();
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ValueCase.NULL, protoValue.getValueCase() );
    }


    @Test
    public void polyStringSerializationCorrectTypeTest() {
        PolyString expected = new PolyString( "sample string" );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ValueCase.STRING, protoValue.getValueCase() );
    }


    @Test
    public void polyTimeSerializationCorrectTypeTest() {
        PolyTime expected = PolyTime.of( new Time( 1691879380700L ) );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ValueCase.TIME, protoValue.getValueCase() );
    }


    @Test
    public void polyTimeStampSerializationCorrectTypeTest() {
        PolyTimestamp expected = new PolyTimestamp( 1691879380700L );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ValueCase.TIMESTAMP, protoValue.getValueCase() );
    }


}
