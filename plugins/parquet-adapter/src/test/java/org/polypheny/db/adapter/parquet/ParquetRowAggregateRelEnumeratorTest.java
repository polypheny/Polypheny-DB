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

package org.polypheny.db.adapter.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.linq4j.Linq4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.adapter.parquet.relational.execution.aggregate.ParquetRowAggregateRelEnumerator;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;


class ParquetRowAggregateRelEnumeratorTest {

    @BeforeAll
    static void initHomeDir() {
        try {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        } catch ( Exception e ) {
            // Already initialized by another test.
        }
    }


    @Test
    void supportsCountStarWithSumMinAndMax() {
        PolyValue[][] rows = {
                { PolyLong.of( 2021 ), PolyLong.of( 10 ), PolyDouble.of( 2D ) },
                { PolyLong.of( 2021 ), PolyLong.of( 20 ), PolyNull.NULL },
                { PolyLong.of( 2021 ), PolyLong.of( 30 ), PolyDouble.of( 5D ) },
                { PolyLong.of( 2022 ), PolyLong.of( 5 ), PolyDouble.of( 4D ) },
                { PolyLong.of( 2023 ), PolyLong.of( 7 ), PolyNull.NULL }
        };

        try ( ParquetRowAggregateRelEnumerator enumerator = new ParquetRowAggregateRelEnumerator(
                Linq4j.asEnumerable( rows ).enumerator(),
                new int[]{ 0 },
                new String[]{ Kind.COUNT.name(), Kind.SUM.name(), Kind.MIN.name(), Kind.MAX.name() },
                new int[]{ -1, 1, 2, 2 } ) ) {
            assertTrue( enumerator.moveNext() );
            PolyValue[] first = enumerator.current();
            assertEquals( 2021L, first[0].asNumber().longValue() );
            assertEquals( 3L, first[1].asNumber().longValue() );
            assertEquals( 60D, first[2].asNumber().doubleValue() );
            assertEquals( 2D, first[3].asNumber().doubleValue() );
            assertEquals( 5D, first[4].asNumber().doubleValue() );

            assertTrue( enumerator.moveNext() );
            PolyValue[] second = enumerator.current();
            assertEquals( 2022L, second[0].asNumber().longValue() );
            assertEquals( 1L, second[1].asNumber().longValue() );
            assertEquals( 5D, second[2].asNumber().doubleValue() );
            assertEquals( 4D, second[3].asNumber().doubleValue() );
            assertEquals( 4D, second[4].asNumber().doubleValue() );

            assertTrue( enumerator.moveNext() );
            PolyValue[] third = enumerator.current();
            assertEquals( 2023L, third[0].asNumber().longValue() );
            assertEquals( 1L, third[1].asNumber().longValue() );
            assertEquals( 7D, third[2].asNumber().doubleValue() );
            assertTrue( third[3].isNull() );
            assertTrue( third[4].isNull() );

            assertFalse( enumerator.moveNext() );
        }
    }


    @Test
    void supportsMultipleGroupingColumnsAndNullGroupKeys() {
        PolyValue[][] rows = {
                { PolyLong.of( 2021 ), PolyLong.of( 1 ), PolyDouble.of( 2D ) },
                { PolyLong.of( 2021 ), PolyLong.of( 1 ), PolyDouble.of( 4D ) },
                { PolyLong.of( 2021 ), PolyLong.of( 2 ), PolyDouble.of( 8D ) },
                { PolyNull.NULL, PolyLong.of( 1 ), PolyDouble.of( 16D ) },
                { PolyNull.NULL, PolyLong.of( 1 ), PolyNull.NULL }
        };

        try ( ParquetRowAggregateRelEnumerator enumerator = new ParquetRowAggregateRelEnumerator(
                Linq4j.asEnumerable( rows ).enumerator(),
                new int[]{ 0, 1 },
                new String[]{ Kind.COUNT.name(), Kind.SUM.name(), Kind.MIN.name(), Kind.MAX.name() },
                new int[]{ -1, 2, 2, 2 } ) ) {
            assertTrue( enumerator.moveNext() );
            PolyValue[] first = enumerator.current();
            assertEquals( 2021L, first[0].asNumber().longValue() );
            assertEquals( 1L, first[1].asNumber().longValue() );
            assertEquals( 2L, first[2].asNumber().longValue() );
            assertEquals( 6D, first[3].asNumber().doubleValue() );
            assertEquals( 2D, first[4].asNumber().doubleValue() );
            assertEquals( 4D, first[5].asNumber().doubleValue() );

            assertTrue( enumerator.moveNext() );
            PolyValue[] second = enumerator.current();
            assertEquals( 2021L, second[0].asNumber().longValue() );
            assertEquals( 2L, second[1].asNumber().longValue() );
            assertEquals( 1L, second[2].asNumber().longValue() );
            assertEquals( 8D, second[3].asNumber().doubleValue() );

            assertTrue( enumerator.moveNext() );
            PolyValue[] third = enumerator.current();
            assertTrue( third[0].isNull() );
            assertEquals( 1L, third[1].asNumber().longValue() );
            assertEquals( 2L, third[2].asNumber().longValue() );
            assertEquals( 16D, third[3].asNumber().doubleValue() );
            assertEquals( 16D, third[4].asNumber().doubleValue() );
            assertEquals( 16D, third[5].asNumber().doubleValue() );

            assertFalse( enumerator.moveNext() );
        }
    }

}
