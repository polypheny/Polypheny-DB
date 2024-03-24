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

package org.polypheny.db.sql;


import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import org.polypheny.db.adapter.DataContext.SlimDataContext;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgOptPredicateList;
import org.polypheny.db.plan.RexImplicationChecker;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexExecutorImpl;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexSimplify;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.NlsString;
import org.polypheny.db.util.TimeString;
import org.polypheny.db.util.TimestampString;

/**
 * Contains all the nourishment a test case could possibly need.
 * <p>
 * We put the data in here, rather than as fields in the test case, so that the data can be garbage-collected as soon as the test has executed.
 */
@SuppressWarnings("WeakerAccess")
public class TestFixture {

    public final AlgDataTypeFactory typeFactory;
    public final RexBuilder rexBuilder;
    public final AlgDataType boolRelDataType;
    public final AlgDataType intAlgDataType;
    public final AlgDataType decRelDataType;
    public final AlgDataType longRelDataType;
    public final AlgDataType shortDataType;
    public final AlgDataType byteDataType;
    public final AlgDataType floatDataType;
    public final AlgDataType charDataType;
    public final AlgDataType dateDataType;
    public final AlgDataType timestampDataType;
    public final AlgDataType timeDataType;
    public final AlgDataType stringDataType;

    public final RexNode bl; // a field of Java type "Boolean"
    public final RexNode i; // a field of Java type "Integer"
    public final RexNode dec; // a field of Java type "Double"
    public final RexNode lg; // a field of Java type "Long"
    public final RexNode sh; // a  field of Java type "Short"
    public final RexNode by; // a field of Java type "Byte"
    public final RexNode fl; // a field of Java type "Float" (not a SQL FLOAT)
    public final RexNode d; // a field of Java type "Date"
    public final RexNode ch; // a field of Java type "Character"
    public final RexNode ts; // a field of Java type "Timestamp"
    public final RexNode t; // a field of Java type "Time"
    public final RexNode str; // a field of Java type "String"

    public final RexImplicationChecker checker;
    public final AlgDataType rowType;
    public final RexExecutorImpl executor;
    public final RexSimplify simplify;


    public TestFixture() {
        typeFactory = new JavaTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        rexBuilder = new RexBuilder( typeFactory );
        boolRelDataType = typeFactory.createJavaType( PolyBoolean.class );
        intAlgDataType = typeFactory.createJavaType( PolyInteger.class );
        decRelDataType = typeFactory.createJavaType( PolyDouble.class );
        longRelDataType = typeFactory.createJavaType( PolyLong.class );
        shortDataType = typeFactory.createJavaType( PolyInteger.class );
        byteDataType = typeFactory.createJavaType( PolyBinary.class );
        floatDataType = typeFactory.createJavaType( PolyFloat.class );
        charDataType = typeFactory.createJavaType( PolyString.class );
        dateDataType = typeFactory.createJavaType( PolyDate.class );
        timestampDataType = typeFactory.createJavaType( PolyTimestamp.class );
        timeDataType = typeFactory.createJavaType( PolyTime.class );
        stringDataType = typeFactory.createJavaType( PolyString.class );

        bl = ref( 0, boolRelDataType );
        i = ref( 1, intAlgDataType );
        dec = ref( 2, decRelDataType );
        lg = ref( 3, longRelDataType );
        sh = ref( 4, shortDataType );
        by = ref( 5, byteDataType );
        fl = ref( 6, floatDataType );
        ch = ref( 7, charDataType );
        d = ref( 8, dateDataType );
        ts = ref( 9, timestampDataType );
        t = ref( 10, timeDataType );
        str = ref( 11, stringDataType );

        rowType = typeFactory.builder()
                .add( null, "bool", null, this.boolRelDataType )
                .add( null, "int", null, intAlgDataType )
                .add( null, "dec", null, decRelDataType )
                .add( null, "long", null, longRelDataType )
                .add( null, "short", null, shortDataType )
                .add( null, "byte", null, byteDataType )
                .add( null, "float", null, floatDataType )
                .add( null, "char", null, charDataType )
                .add( null, "date", null, dateDataType )
                .add( null, "timestamp", null, timestampDataType )
                .add( null, "time", null, timeDataType )
                .add( null, "string", null, stringDataType )
                .build();
        executor = new RexExecutorImpl( new SlimDataContext() {
            @Override
            public JavaTypeFactory getTypeFactory() {
                return new JavaTypeFactoryImpl();
            }
        } );

        simplify = new RexSimplify( rexBuilder, AlgOptPredicateList.EMPTY, executor ).withParanoid( true );
        checker = new RexImplicationChecker( rexBuilder, executor, rowType );
    }


    public RexIndexRef ref( int i, AlgDataType type ) {
        return new RexIndexRef( i, typeFactory.createTypeWithNullability( type, true ) );
    }


    public RexLiteral literal( int i ) {
        return rexBuilder.makeExactLiteral( new BigDecimal( i ) );
    }


    public RexNode gt( RexNode node1, RexNode node2 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.GREATER_THAN ), node1, node2 );
    }


    public RexNode ge( RexNode node1, RexNode node2 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL ), node1, node2 );
    }


    public RexNode eq( RexNode node1, RexNode node2 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.EQUALS ), node1, node2 );
    }


    public RexNode ne( RexNode node1, RexNode node2 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.NOT_EQUALS ), node1, node2 );
    }


    public RexNode lt( RexNode node1, RexNode node2 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.LESS_THAN ), node1, node2 );
    }


    public RexNode le( RexNode node1, RexNode node2 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL ), node1, node2 );
    }


    public RexNode notNull( RexNode node1 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NOT_NULL ), node1 );
    }


    public RexNode isNull( RexNode node2 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NULL ), node2 );
    }


    public RexNode and( RexNode... nodes ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.AND ), nodes );
    }


    public RexNode or( RexNode... nodes ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.OR ), nodes );
    }


    public RexNode longLiteral( long value ) {
        return rexBuilder.makeLiteral( value, longRelDataType, true );
    }


    public RexNode shortLiteral( short value ) {
        return rexBuilder.makeLiteral( value, shortDataType, true );
    }


    public RexLiteral floatLiteral( double value ) {
        return rexBuilder.makeApproxLiteral( new BigDecimal( value ) );
    }


    public RexLiteral charLiteral( String z ) {
        return rexBuilder.makeCharLiteral( new NlsString( z, null, Collation.COERCIBLE ) );
    }


    public RexNode dateLiteral( DateString d ) {
        return rexBuilder.makeDateLiteral( d );
    }


    public RexNode timestampLiteral( TimestampString ts ) {
        return rexBuilder.makeTimestampLiteral( ts, timestampDataType.getPrecision() );
    }


    public RexNode timeLiteral( TimeString t ) {
        return rexBuilder.makeTimeLiteral( t, timeDataType.getPrecision() );
    }


    public RexNode cast( AlgDataType type, RexNode exp ) {
        return rexBuilder.makeCast( type, exp, true );
    }


    void checkImplies( RexNode node1, RexNode node2 ) {
        final String message = node1 + " does not imply " + node2 + " when it should";
        assertTrue( checker.implies( node1, node2 ), message );
    }


    void checkNotImplies( RexNode node1, RexNode node2 ) {
        final String message = node1 + " does implies " + node2 + " when it should not";
        assertFalse( checker.implies( node1, node2 ), message );
    }

}
