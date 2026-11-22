/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.prisminterface;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;
import org.polypheny.prism.ErrorResponse;

class PrismNativeCastErrorTest {

    private static final String INVALID_NUMBER = "abc";


    @BeforeAll
    static void initializeTestMode() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
    }


    @TestFactory
    Stream<DynamicTest> preservesLegacyNumericCastErrors() {
        return Stream.of(
                new NumericCast( "INTEGER", Integer::parseInt, PolyInteger::convert ),
                new NumericCast( "BIGINT", Long::parseLong, PolyLong::convert ),
                new NumericCast( "DECIMAL", BigDecimal::new, PolyBigDecimal::convert ),
                new NumericCast( "FLOAT", Float::parseFloat, PolyFloat::convert ),
                new NumericCast( "DOUBLE", Double::parseDouble, PolyDouble::convert )
        ).map( cast -> dynamicTest( cast.type(), () -> assertLegacyResponses( cast ) ) );
    }


    private static void assertLegacyResponses( NumericCast cast ) {
        NumberFormatException previous = assertThrows( NumberFormatException.class, () -> cast.parser().apply( INVALID_NUMBER ) );
        PolyString value = PolyString.of( INVALID_NUMBER );
        NumberFormatException direct = assertThrows( NumberFormatException.class, () -> cast.converter().apply( value ) );
        NumberFormatException cause = assertInstanceOf( NumberFormatException.class, direct.getCause() );
        assertEquals( previous.toString(), cause.toString() );
        assertPrismError( direct, previous.getMessage() );

        Iterator<PolyValue[]> rows = new Iterator<>() {
            @Override
            public boolean hasNext() {
                return true;
            }


            @Override
            public PolyValue[] next() {
                return new PolyValue[]{ cast.converter().apply( value ) };
            }
        };
        ResultIterator results = new ResultIterator( rows, null, 1, false, false, false, null, null, null );
        GenericRuntimeException resultFailure = assertThrows( GenericRuntimeException.class, results::getNextBatch );
        assertPrismError( resultFailure, previous.toString() );

        // Named Prism execution adds another cause-only wrapper.
        GenericRuntimeException namedFailure = new GenericRuntimeException( resultFailure );
        assertPrismError( namedFailure, new GenericRuntimeException( previous ).toString() );
    }


    private static void assertPrismError( Throwable failure, String expectedMessage ) {
        ErrorResponse error = PIService.createErrorResponse( 1, failure ).getErrorResponse();
        assertEquals( expectedMessage, error.getMessage() );
        assertTrue( error.hasDetails() );
        assertEquals( expectedMessage, error.getDetails().getMessage() );
        assertEquals( "22018", error.getDetails().getState() );
    }


    private record NumericCast( String type, Function<String, Number> parser, Function<PolyValue, PolyNumber> converter ) {
    }

}
