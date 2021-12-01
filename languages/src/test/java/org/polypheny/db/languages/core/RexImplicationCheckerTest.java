/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.languages.core;


import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.hamcrest.core.Is;
import org.junit.Test;
import org.polypheny.db.core.enums.Kind;
import org.polypheny.db.core.operators.OperatorName;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.RexImplicationChecker;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexSimplify;
import org.polypheny.db.rex.RexUnknownAs;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.TimeString;
import org.polypheny.db.util.TimestampString;
import org.polypheny.db.util.Util;


/**
 * Unit tests for {@link RexImplicationChecker}.
 */
public class RexImplicationCheckerTest extends LanguageManagerDependant {

    // Simple Tests for Operators
    @Test
    public void testSimpleGreaterCond() {
        final TestFixture f = new TestFixture();
        final RexNode iGt10 = f.gt( f.i, f.literal( 10 ) );
        final RexNode iGt30 = f.gt( f.i, f.literal( 30 ) );
        final RexNode iGe30 = f.ge( f.i, f.literal( 30 ) );
        final RexNode iGe10 = f.ge( f.i, f.literal( 10 ) );
        final RexNode iEq30 = f.eq( f.i, f.literal( 30 ) );
        final RexNode iNe10 = f.ne( f.i, f.literal( 10 ) );

        f.checkImplies( iGt30, iGt10 );
        f.checkNotImplies( iGt10, iGt30 );
        f.checkNotImplies( iGt10, iGe30 );
        f.checkImplies( iGe30, iGt10 );
        f.checkImplies( iEq30, iGt10 );
        f.checkNotImplies( iGt10, iEq30 );
        f.checkNotImplies( iGt10, iNe10 );
        f.checkNotImplies( iGe10, iNe10 );
        // identity
        f.checkImplies( iGt10, iGt10 );
        f.checkImplies( iGe30, iGe30 );
    }


    @Test
    public void testSimpleLesserCond() {
        final TestFixture f = new TestFixture();
        final RexNode iLt10 = f.lt( f.i, f.literal( 10 ) );
        final RexNode iLt30 = f.lt( f.i, f.literal( 30 ) );
        final RexNode iLe30 = f.le( f.i, f.literal( 30 ) );
        final RexNode iLe10 = f.le( f.i, f.literal( 10 ) );
        final RexNode iEq10 = f.eq( f.i, f.literal( 10 ) );
        final RexNode iNe10 = f.ne( f.i, f.literal( 10 ) );

        f.checkImplies( iLt10, iLt30 );
        f.checkNotImplies( iLt30, iLt10 );
        f.checkImplies( iLt10, iLe30 );
        f.checkNotImplies( iLe30, iLt10 );
        f.checkImplies( iEq10, iLt30 );
        f.checkNotImplies( iLt30, iEq10 );
        f.checkNotImplies( iLt10, iEq10 );
        f.checkNotImplies( iLt10, iNe10 );
        f.checkNotImplies( iLe10, iNe10 );
        // identity
        f.checkImplies( iLt10, iLt10 );
        f.checkImplies( iLe30, iLe30 );
    }


    @Test
    public void testSimpleEq() {
        final TestFixture f = new TestFixture();
        final RexNode iEq30 = f.eq( f.i, f.literal( 30 ) );
        final RexNode iNe10 = f.ne( f.i, f.literal( 10 ) );
        final RexNode iNe30 = f.ne( f.i, f.literal( 30 ) );

        f.checkImplies( iEq30, iEq30 );
        f.checkImplies( iNe10, iNe10 );
        f.checkImplies( iEq30, iNe10 );
        f.checkNotImplies( iNe10, iEq30 );
        f.checkNotImplies( iNe30, iEq30 );
    }


    // Simple Tests for DataTypes
    @Test
    public void testSimpleDec() {
        final TestFixture f = new TestFixture();
        final RexNode node1 = f.lt( f.dec, f.floatLiteral( 30.9 ) );
        final RexNode node2 = f.lt( f.dec, f.floatLiteral( 40.33 ) );

        f.checkImplies( node1, node2 );
        f.checkNotImplies( node2, node1 );
    }


    @Test
    public void testSimpleBoolean() {
        final TestFixture f = new TestFixture();
        final RexNode bEqTrue = f.eq( f.bl, f.rexBuilder.makeLiteral( true ) );
        final RexNode bEqFalse = f.eq( f.bl, f.rexBuilder.makeLiteral( false ) );

        // TODO: Need to support false => true
        //f.checkImplies(bEqFalse, bEqTrue);
        f.checkNotImplies( bEqTrue, bEqFalse );
    }


    @Test
    public void testSimpleLong() {
        final TestFixture f = new TestFixture();
        final RexNode xGeBig = f.ge( f.lg, f.longLiteral( 324324L ) );
        final RexNode xGtBigger = f.gt( f.lg, f.longLiteral( 324325L ) );
        final RexNode xGeBigger = f.ge( f.lg, f.longLiteral( 324325L ) );

        f.checkImplies( xGtBigger, xGeBig );
        f.checkImplies( xGtBigger, xGeBigger );
        f.checkImplies( xGeBigger, xGeBig );
        f.checkNotImplies( xGeBig, xGtBigger );
    }


    @Test
    public void testSimpleShort() {
        final TestFixture f = new TestFixture();
        final RexNode xGe10 = f.ge( f.sh, f.shortLiteral( (short) 10 ) );
        final RexNode xGe11 = f.ge( f.sh, f.shortLiteral( (short) 11 ) );

        f.checkImplies( xGe11, xGe10 );
        f.checkNotImplies( xGe10, xGe11 );
    }


    @Test
    public void testSimpleChar() {
        final TestFixture f = new TestFixture();
        final RexNode xGeB = f.ge( f.ch, f.charLiteral( "b" ) );
        final RexNode xGeA = f.ge( f.ch, f.charLiteral( "a" ) );

        f.checkImplies( xGeB, xGeA );
        f.checkNotImplies( xGeA, xGeB );
    }


    @Test
    public void testSimpleString() {
        final TestFixture f = new TestFixture();
        final RexNode node1 = f.eq( f.str, f.rexBuilder.makeLiteral( "en" ) );

        f.checkImplies( node1, node1 );
    }


    @Test
    public void testSimpleDate() {
        final TestFixture f = new TestFixture();
        final DateString d = DateString.fromCalendarFields( Util.calendar() );
        final RexNode node1 = f.ge( f.d, f.dateLiteral( d ) );
        final RexNode node2 = f.eq( f.d, f.dateLiteral( d ) );
        f.checkImplies( node2, node1 );
        f.checkNotImplies( node1, node2 );

        final DateString dBeforeEpoch1 = DateString.fromDaysSinceEpoch( -12345 );
        final DateString dBeforeEpoch2 = DateString.fromDaysSinceEpoch( -123 );
        final RexNode nodeBe1 = f.lt( f.d, f.dateLiteral( dBeforeEpoch1 ) );
        final RexNode nodeBe2 = f.lt( f.d, f.dateLiteral( dBeforeEpoch2 ) );
        f.checkImplies( nodeBe1, nodeBe2 );
        f.checkNotImplies( nodeBe2, nodeBe1 );
    }


    @Test
    public void testSimpleTimeStamp() {
        final TestFixture f = new TestFixture();
        final TimestampString ts = TimestampString.fromCalendarFields( Util.calendar() );
        final RexNode node1 = f.lt( f.ts, f.timestampLiteral( ts ) );
        final RexNode node2 = f.le( f.ts, f.timestampLiteral( ts ) );
        f.checkImplies( node1, node2 );
        f.checkNotImplies( node2, node1 );

        final TimestampString tsBeforeEpoch1 = TimestampString.fromMillisSinceEpoch( -1234567890L );
        final TimestampString tsBeforeEpoch2 = TimestampString.fromMillisSinceEpoch( -1234567L );
        final RexNode nodeBe1 = f.lt( f.ts, f.timestampLiteral( tsBeforeEpoch1 ) );
        final RexNode nodeBe2 = f.lt( f.ts, f.timestampLiteral( tsBeforeEpoch2 ) );
        f.checkImplies( nodeBe1, nodeBe2 );
        f.checkNotImplies( nodeBe2, nodeBe1 );
    }


    @Test
    public void testSimpleTime() {
        final TestFixture f = new TestFixture();
        final TimeString t = TimeString.fromCalendarFields( Util.calendar() );
        final RexNode node1 = f.lt( f.t, f.timeLiteral( t ) );
        final RexNode node2 = f.le( f.t, f.timeLiteral( t ) );
        f.checkImplies( node1, node2 );
        f.checkNotImplies( node2, node1 );
    }


    @Test
    public void testSimpleBetween() {
        final TestFixture f = new TestFixture();
        final RexNode iGe30 = f.ge( f.i, f.literal( 30 ) );
        final RexNode iLt70 = f.lt( f.i, f.literal( 70 ) );
        final RexNode iGe30AndLt70 = f.and( iGe30, iLt70 );
        final RexNode iGe50 = f.ge( f.i, f.literal( 50 ) );
        final RexNode iLt60 = f.lt( f.i, f.literal( 60 ) );
        final RexNode iGe50AndLt60 = f.and( iGe50, iLt60 );

        f.checkNotImplies( iGe30AndLt70, iGe50 );
        f.checkNotImplies( iGe30AndLt70, iLt60 );
        f.checkNotImplies( iGe30AndLt70, iGe50AndLt60 );
        f.checkNotImplies( iGe30, iGe50AndLt60 );
        f.checkNotImplies( iLt70, iGe50AndLt60 );
        f.checkImplies( iGe50AndLt60, iGe30AndLt70 );
        f.checkImplies( iGe50AndLt60, iLt70 );
        f.checkImplies( iGe50AndLt60, iGe30 );
    }


    @Test
    public void testSimpleBetweenCornerCases() {
        final TestFixture f = new TestFixture();
        final RexNode node1 = f.gt( f.i, f.literal( 30 ) );
        final RexNode node2 = f.gt( f.i, f.literal( 50 ) );
        final RexNode node3 = f.lt( f.i, f.literal( 60 ) );
        final RexNode node4 = f.lt( f.i, f.literal( 80 ) );
        final RexNode node5 = f.lt( f.i, f.literal( 90 ) );
        final RexNode node6 = f.lt( f.i, f.literal( 100 ) );

        f.checkNotImplies( f.and( node1, node2 ), f.and( node3, node4 ) );
        f.checkNotImplies( f.and( node5, node6 ), f.and( node3, node4 ) );
        f.checkNotImplies( f.and( node1, node2 ), node6 );
        f.checkNotImplies( node6, f.and( node1, node2 ) );
        f.checkImplies( f.and( node3, node4 ), f.and( node5, node6 ) );
    }


    /**
     * {@code x > 1 OR (y > 2 AND z > 4)}
     * implies
     * {@code (y > 3 AND z > 5)}.
     */
    @Test
    public void testOr() {
        final TestFixture f = new TestFixture();
        final RexNode xGt1 = f.gt( f.i, f.literal( 1 ) );
        final RexNode yGt2 = f.gt( f.dec, f.literal( 2 ) );
        final RexNode yGt3 = f.gt( f.dec, f.literal( 3 ) );
        final RexNode zGt4 = f.gt( f.lg, f.literal( 4 ) );
        final RexNode zGt5 = f.gt( f.lg, f.literal( 5 ) );
        final RexNode yGt2AndZGt4 = f.and( yGt2, zGt4 );
        final RexNode yGt3AndZGt5 = f.and( yGt3, zGt5 );
        final RexNode or = f.or( xGt1, yGt2AndZGt4 );
        //f.checkNotImplies(or, yGt3AndZGt5);
        f.checkImplies( yGt3AndZGt5, or );
    }


    @Test
    public void testNotNull() {
        final TestFixture f = new TestFixture();
        final RexNode node1 = f.eq( f.str, f.rexBuilder.makeLiteral( "en" ) );
        final RexNode node2 = f.notNull( f.str );
        final RexNode node3 = f.gt( f.str, f.rexBuilder.makeLiteral( "abc" ) );
        f.checkImplies( node1, node2 );
        f.checkNotImplies( node2, node1 );
        f.checkImplies( node3, node2 );
        f.checkImplies( node2, node2 );
    }


    @Test
    public void testIsNull() {
        final TestFixture f = new TestFixture();
        final RexNode sEqEn = f.eq( f.str, f.charLiteral( "en" ) );
        final RexNode sIsNotNull = f.notNull( f.str );
        final RexNode sIsNull = f.isNull( f.str );
        final RexNode iEq5 = f.eq( f.i, f.literal( 5 ) );
        final RexNode iIsNull = f.isNull( f.i );
        final RexNode iIsNotNull = f.notNull( f.i );
        f.checkNotImplies( sIsNotNull, sIsNull );
        f.checkNotImplies( sIsNull, sIsNotNull );
        f.checkNotImplies( sEqEn, sIsNull );
        f.checkNotImplies( sIsNull, sEqEn );
        f.checkImplies( sEqEn, sIsNotNull ); // "s = literal" implies "s is not null"
        f.checkImplies( sIsNotNull, sIsNotNull ); // "s is not null" implies "s is not null"
        f.checkImplies( sIsNull, sIsNull ); // "s is null" implies "s is null"

        // "s is not null and y = 5" implies "s is not null"
        f.checkImplies( f.and( sIsNotNull, iEq5 ), sIsNotNull );

        // "y = 5 and s is not null" implies "s is not null"
        f.checkImplies( f.and( iEq5, sIsNotNull ), sIsNotNull );

        // "y is not null" does not imply "s is not null"
        f.checkNotImplies( iIsNull, sIsNotNull );

        // "s is not null or i = 5" does not imply "s is not null"
        f.checkNotImplies( f.or( sIsNotNull, iEq5 ), sIsNotNull );

        // "s is not null" implies "s is not null or i = 5"
        f.checkImplies( sIsNotNull, f.or( sIsNotNull, iEq5 ) );

        // "s is not null" implies "i = 5 or s is not null"
        f.checkImplies( sIsNotNull, f.or( iEq5, sIsNotNull ) );

        // "i > 10" implies "x is not null"
        f.checkImplies( f.gt( f.i, f.literal( 10 ) ), iIsNotNull );

        // "-20 > i" implies "x is not null"
        f.checkImplies( f.gt( f.literal( -20 ), f.i ), iIsNotNull );

        // "s is null and -20 > i" implies "x is not null"
        f.checkImplies( f.and( sIsNull, f.gt( f.literal( -20 ), f.i ) ), iIsNotNull );

        // "i > 10" does not imply "x is null"
        f.checkNotImplies( f.gt( f.i, f.literal( 10 ) ), iIsNull );
    }


    /**
     * Test case for "When simplifying a nullable expression, allow the result to change type to NOT NULL" and match nullability.
     *
     * @see RexSimplify#simplifyPreservingType(RexNode, RexUnknownAs, boolean)
     */
    @Test
    public void testSimplifyCastMatchNullability() {
        final TestFixture f = new TestFixture();

        // The cast is nullable, while the literal is not nullable. When we simplify it, we end up with the literal. If defaultSimplifier is used, a CAST is introduced on top of the expression, as nullability of the new expression
        // does not match the nullability of the original one. If nonMatchingNullabilitySimplifier is used, the CAST is not added and the simplified expression only consists of the literal.
        final RexNode e = f.cast( f.intAlgDataType, f.literal( 2014 ) );
        assertThat(
                f.simplify.simplifyPreservingType( e, RexUnknownAs.UNKNOWN, true ).toString(),
                is( "CAST(2014):JavaType(class java.lang.Integer)" ) );
        assertThat(
                f.simplify.simplifyPreservingType( e, RexUnknownAs.UNKNOWN, false ).toString(),
                is( "2014" ) );

        // In this case, the cast is not nullable. Thus, in both cases, the simplified expression only consists of the literal.
        AlgDataType notNullIntRelDataType = f.typeFactory.createJavaType( int.class );
        final RexNode e2 = f.cast( notNullIntRelDataType, f.cast( notNullIntRelDataType, f.literal( 2014 ) ) );
        assertThat(
                f.simplify.simplifyPreservingType( e2, RexUnknownAs.UNKNOWN, true ).toString(),
                is( "2014" ) );
        assertThat(
                f.simplify.simplifyPreservingType( e2, RexUnknownAs.UNKNOWN, false ).toString(),
                is( "2014" ) );
    }


    /**
     * Test case for simplifier of ceil/floor.
     */
    @Test
    public void testSimplifyCeilFloor() {
        // We can add more time units here once they are supported in RexInterpreter, e.g., TimeUnitRange.HOUR, TimeUnitRange.MINUTE, TimeUnitRange.SECOND.
        final ImmutableList<TimeUnitRange> timeUnitRanges = ImmutableList.of( TimeUnitRange.YEAR, TimeUnitRange.MONTH );
        final TestFixture f = new TestFixture();

        final RexNode literalTs = f.timestampLiteral( new TimestampString( "2010-10-10 00:00:00" ) );
        for ( int i = 0; i < timeUnitRanges.size(); i++ ) {
            final RexNode innerFloorCall = f.rexBuilder.makeCall(
                    OperatorRegistry.get( OperatorName.FLOOR ),
                    literalTs,
                    f.rexBuilder.makeFlag( timeUnitRanges.get( i ) ) );
            final RexNode innerCeilCall = f.rexBuilder.makeCall(
                    OperatorRegistry.get( OperatorName.CEIL ),
                    literalTs,
                    f.rexBuilder.makeFlag( timeUnitRanges.get( i ) ) );
            for ( int j = 0; j <= i; j++ ) {
                final RexNode outerFloorCall = f.rexBuilder.makeCall(
                        OperatorRegistry.get( OperatorName.FLOOR ),
                        innerFloorCall,
                        f.rexBuilder.makeFlag( timeUnitRanges.get( j ) ) );
                final RexNode outerCeilCall = f.rexBuilder.makeCall(
                        OperatorRegistry.get( OperatorName.CEIL ),
                        innerCeilCall,
                        f.rexBuilder.makeFlag( timeUnitRanges.get( j ) ) );
                final RexCall floorSimplifiedExpr = (RexCall) f.simplify.simplifyPreservingType(
                        outerFloorCall,
                        RexUnknownAs.UNKNOWN,
                        true );
                assertThat( floorSimplifiedExpr.getKind(), Is.is( Kind.FLOOR ) );
                assertThat( ((RexLiteral) floorSimplifiedExpr.getOperands().get( 1 )).getValue().toString(), is( timeUnitRanges.get( j ).toString() ) );
                assertThat( floorSimplifiedExpr.getOperands().get( 0 ).toString(), is( literalTs.toString() ) );
                final RexCall ceilSimplifiedExpr = (RexCall) f.simplify.simplifyPreservingType( outerCeilCall, RexUnknownAs.UNKNOWN, true );
                assertThat( ceilSimplifiedExpr.getKind(), is( Kind.CEIL ) );
                assertThat( ((RexLiteral) ceilSimplifiedExpr.getOperands().get( 1 )).getValue().toString(), is( timeUnitRanges.get( j ).toString() ) );
                assertThat( ceilSimplifiedExpr.getOperands().get( 0 ).toString(), is( literalTs.toString() ) );
            }
        }

        // Negative test
        for ( int i = timeUnitRanges.size() - 1; i >= 0; i-- ) {
            final RexNode innerFloorCall = f.rexBuilder.makeCall(
                    OperatorRegistry.get( OperatorName.FLOOR ),
                    literalTs,
                    f.rexBuilder.makeFlag( timeUnitRanges.get( i ) ) );
            final RexNode innerCeilCall = f.rexBuilder.makeCall(
                    OperatorRegistry.get( OperatorName.CEIL ),
                    literalTs,
                    f.rexBuilder.makeFlag( timeUnitRanges.get( i ) ) );
            for ( int j = timeUnitRanges.size() - 1; j > i; j-- ) {
                final RexNode outerFloorCall = f.rexBuilder.makeCall(
                        OperatorRegistry.get( OperatorName.FLOOR ),
                        innerFloorCall,
                        f.rexBuilder.makeFlag( timeUnitRanges.get( j ) ) );
                final RexNode outerCeilCall = f.rexBuilder.makeCall(
                        OperatorRegistry.get( OperatorName.CEIL ),
                        innerCeilCall,
                        f.rexBuilder.makeFlag( timeUnitRanges.get( j ) ) );
                final RexCall floorSimplifiedExpr = (RexCall) f.simplify.simplifyPreservingType(
                        outerFloorCall,
                        RexUnknownAs.UNKNOWN,
                        true );
                assertThat( floorSimplifiedExpr.toString(), is( outerFloorCall.toString() ) );
                final RexCall ceilSimplifiedExpr = (RexCall) f.simplify.simplifyPreservingType(
                        outerCeilCall,
                        RexUnknownAs.UNKNOWN,
                        true );
                assertThat( ceilSimplifiedExpr.toString(), is( outerCeilCall.toString() ) );
            }
        }
    }


}

