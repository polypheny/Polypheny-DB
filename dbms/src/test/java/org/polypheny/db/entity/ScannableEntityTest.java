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

package org.polypheny.db.entity;


import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Iterator;
import java.util.List;
import org.apache.calcite.linq4j.Enumerator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.types.ScannableEntity;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyInteger;


/**
 * Unit test for {@link ScannableEntity}.
 */
public class ScannableEntityTest {

    @BeforeAll
    public static void setUpClass() {
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }


    @Test
    public void testTens() {
        final Enumerator<PolyValue[]> cursor = tens();
        assertTrue( cursor.moveNext() );
        assertThat( cursor.current()[0], equalTo( PolyInteger.of( 0 ) ) );
        assertThat( cursor.current().length, equalTo( 1 ) );
        assertTrue( cursor.moveNext() );
        assertThat( cursor.current()[0], equalTo( PolyInteger.of( 10 ) ) );
        assertTrue( cursor.moveNext() );
        assertThat( cursor.current()[0], equalTo( PolyInteger.of( 20 ) ) );
        assertTrue( cursor.moveNext() );
        assertThat( cursor.current()[0], equalTo( PolyInteger.of( 30 ) ) );
        assertFalse( cursor.moveNext() );
    }

    private static Integer getFilter( boolean cooperative, List<RexNode> filters ) {
        final Iterator<RexNode> filterIter = filters.iterator();
        while ( filterIter.hasNext() ) {
            final RexNode node = filterIter.next();
            if ( cooperative
                    && node instanceof RexCall
                    && ((RexCall) node).getOperator().getOperatorName() == OperatorName.EQUALS
                    && ((RexCall) node).getOperands().get( 0 ) instanceof RexIndexRef
                    && ((RexIndexRef) ((RexCall) node).getOperands().get( 0 )).getIndex() == 0
                    && ((RexCall) node).getOperands().get( 1 ) instanceof RexLiteral ) {
                final RexNode op1 = ((RexCall) node).getOperands().get( 1 );
                filterIter.remove();
                return ((RexLiteral) op1).getValue().asBigDecimal().intValue();
            }
        }
        return null;
    }


    private static Enumerator<PolyValue[]> tens() {
        return new Enumerator<>() {
            int row = -1;
            PolyValue[] current;


            @Override
            public PolyValue[] current() {
                return current;
            }


            @Override
            public boolean moveNext() {
                if ( ++row < 4 ) {
                    current = new PolyValue[]{ PolyInteger.of( row * 10 ) };
                    return true;
                } else {
                    return false;
                }
            }


            @Override
            public void reset() {
                row = -1;
            }


            @Override
            public void close() {
                current = null;
            }
        };
    }


}

