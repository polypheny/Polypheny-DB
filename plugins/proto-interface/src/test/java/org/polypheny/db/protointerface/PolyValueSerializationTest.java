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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.sql.Time;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.protointerface.proto.ProtoValue;
import org.polypheny.db.protointerface.proto.ProtoValue.ProtoValueType;
import org.polypheny.db.protointerface.proto.ProtoValue.ValueCase;
import org.polypheny.db.protointerface.utils.PolyValueSerializer;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBigDecimal;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyDate;
import org.polypheny.db.type.entity.PolyDouble;
import org.polypheny.db.type.entity.PolyFile;
import org.polypheny.db.type.entity.PolyFloat;
import org.polypheny.db.type.entity.PolyInteger;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyStream;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolySymbol;
import org.polypheny.db.type.entity.PolyTime;
import org.polypheny.db.type.entity.PolyTimeStamp;
import org.polypheny.db.type.entity.PolyUserDefinedValue;
import org.polypheny.db.type.entity.PolyValue;

public class PolyValueSerializationTest {

    @BeforeClass
    public static void init() {
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
        value.put( "dateField", new PolyDate( 119581 ) );
        value.put( "doubleField", new PolyDouble( 123.456 ) );
        value.put( "fileField", new PolyFile( new byte[]{ 0x0A, 0x1B, 0x2C, 0x3D, 0x4E } ) );
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
        value.put( "symbolField", new PolySymbol( TestEnum.UNTESTED ) );
        value.put( "timeField", PolyTime.of( new Time( 1691879380700L ) ) );
        value.put( "timestampField", new PolyTimeStamp( 1691879380700L ) );
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
        assertEquals( new byte[]{ 0x01, 0x02, 0x03 }, protoValue.getBinary().toByteArray() );
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
    public void polyFileSerializationTest() {
        PolyFile expected = new PolyFile( new byte[]{ 0x0A, 0x1B, 0x2C, 0x3D, 0x4E } );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( new byte[]{ 0x0A, 0x1B, 0x2C, 0x3D, 0x4E }, protoValue.getFile().getBytes().toByteArray() );
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
    public void polyIntervalSerializationTest() {
        //TODO TH: create test
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
        assertEquals( 1, protoValues.get( 1 ).getInteger().getInteger() );
        assertEquals( 2, protoValues.get( 2 ).getInteger().getInteger() );
        assertEquals( 3, protoValues.get( 3 ).getInteger().getInteger() );
        assertEquals( 4, protoValues.get( 4 ).getInteger().getInteger() );
    }


    @Test
    public void polyLongSerializationTest() {
        PolyLong expected = new PolyLong( 1234567890L );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( 1234567890L, protoValue.getLong().getLong() );
    }


    @Test
    public void polyStreamSerializationTest() {
        ByteArrayInputStream stream = new ByteArrayInputStream( "test data".getBytes() );
        PolyStream expected = new PolyStream( stream );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        //TODO TH: create test
    }


    @Test
    public void polyStringSerializationTest() {
        PolyString expected = new PolyString( "sample string" );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( "sample string", protoValue.getString().getString() );
    }


    @Test
    public void polySymbolSerializationTest() {
        PolySymbol expected = new PolySymbol( TestEnum.UNTESTED );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        //TODO TH: create test
    }


    @Test
    public void polyTimeSerializationTest() {
        PolyTime expected = PolyTime.of( new Time( 1691879380700L ) );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( 1691879380700L, protoValue.getTime().getValue() );
    }


    @Test
    public void polyTimeStampSerializationTest() {
        PolyTimeStamp expected = new PolyTimeStamp( 1691879380700L );  // Assuming ISO-8601 format
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( 1691879380700L, protoValue.getTimeStamp().getTimeStamp() );
    }


    @Test
    public void polyUserDefinedValueSerializationTest() {
        PolyUserDefinedValue expected = buildTestUdt();
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        //TODO TH: create test
    }


    @Test
    public void polyBigDecimalSerializationCorrectTypeTest() {
        PolyBigDecimal expected = new PolyBigDecimal( new BigDecimal( 1691879380700L ) );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ValueCase.BIG_DECIMAL, protoValue.getValueCase() );
        assertEquals( ProtoValueType.DECIMAL, protoValue.getType() );
    }


    @Test
    public void polyBinarySerializationCorrectTypeTest() {
        PolyBinary expected = PolyBinary.of( new byte[]{ 0x01, 0x02, 0x03 } );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ValueCase.BINARY, protoValue.getValueCase() );
        assertEquals( ProtoValueType.BINARY, protoValue.getType() );
    }


    @Test
    public void polyBooleanSerializationCorrectTypeTest() {
        PolyBoolean expected = new PolyBoolean( true );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ValueCase.BOOLEAN, protoValue.getValueCase() );
        assertEquals( ProtoValueType.BOOLEAN, protoValue.getType() );
    }


    @Test
    public void polyDateSerializationCorrectTypeTest() {
        long daysSinceEpoch = 119581;
        PolyDate expected = new PolyDate( daysSinceEpoch );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ValueCase.DATE, protoValue.getValueCase() );
        assertEquals( ProtoValueType.DATE, protoValue.getType() );
    }


    @Test
    public void polyDoubleSerializationCorrectTypeTest() {
        PolyDouble expected = new PolyDouble( 123.456 );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ValueCase.DOUBLE, protoValue.getValueCase() );
        assertEquals( ProtoValueType.DOUBLE, protoValue.getType() );
    }


    @Test
    public void polyFileSerializationCorrectTypeTest() {
        PolyFile expected = new PolyFile( new byte[]{ 0x0A, 0x1B, 0x2C, 0x3D, 0x4E } );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ValueCase.FILE, protoValue.getValueCase() );
        assertEquals( ProtoValueType.FILE, protoValue.getType() );
    }


    @Test
    public void polyFloatSerializationCorrectTypeTest() {
        PolyFloat expected = new PolyFloat( 45.67f );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ValueCase.FLOAT, protoValue.getValueCase() );
        assertEquals( ProtoValueType.FLOAT, protoValue.getType() );
    }


    @Test
    public void polyIntegerSerializationCorrectTypeTest() {
        PolyInteger expected = new PolyInteger( 1234 );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ValueCase.INTEGER, protoValue.getValueCase() );
        assertEquals( ProtoValueType.INTEGER, protoValue.getType() );
    }


    @Test
    public void polyIntervalSerializationCorrectTypeTest() {
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
        assertEquals( ProtoValueType.ARRAY, protoValue.getType() );
    }


    @Test
    public void polyLongSerializationCorrectTypeTest() {
        PolyLong expected = new PolyLong( 1234567890L );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ValueCase.LONG, protoValue.getValueCase() );
        assertEquals( ProtoValueType.BIGINT, protoValue.getType() );
    }


    @Test
    public void polyNullSerializationCorrectTypeTest() {
        PolyNull expected = new PolyNull();
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ValueCase.NULL, protoValue.getValueCase() );
        assertEquals( ProtoValueType.NULL, protoValue.getType() );
    }


    @Test
    public void polyStreamSerializationCorrectTypeTest() {
        ByteArrayInputStream stream = new ByteArrayInputStream( "test data".getBytes() );
        PolyStream expected = new PolyStream( stream );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ProtoValueType.FILE, protoValue.getType() );

    }


    @Test
    public void polyStringSerializationCorrectTypeTest() {
        PolyString expected = new PolyString( "sample string" );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ValueCase.STRING, protoValue.getValueCase() );
        assertEquals( ProtoValueType.VARCHAR, protoValue.getType() );
    }


    @Test
    public void polySymbolSerializationCorrectTypeTest() {
        PolySymbol expected = new PolySymbol( TestEnum.UNTESTED );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ProtoValueType.SYMBOL, protoValue.getType() );

    }


    @Test
    public void polyTimeSerializationCorrectTypeTest() {
        PolyTime expected = PolyTime.of( new Time( 1691879380700L ) );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ValueCase.TIME, protoValue.getValueCase() );
        assertEquals( ProtoValueType.TIME, protoValue.getType() );
    }


    @Test
    public void polyTimeStampSerializationCorrectTypeTest() {
        PolyTimeStamp expected = new PolyTimeStamp( 1691879380700L );
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ValueCase.TIME_STAMP, protoValue.getValueCase() );
        assertEquals( ProtoValueType.TIMESTAMP, protoValue.getType() );
    }


    @Test
    public void polyUserDefinedValueSerializationCorrectTypeTest() {
        PolyUserDefinedValue expected = buildTestUdt();
        ProtoValue protoValue = PolyValueSerializer.serialize( expected );
        assertEquals( ValueCase.USER_DEFINED_TYPE, protoValue.getValueCase() );
        assertEquals( ProtoValueType.USER_DEFINED_TYPE, protoValue.getType() );
    }

}
