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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.rex.fuzzer;


import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgramBuilderBase;
import org.polypheny.db.type.PolyType;


/**
 * Generates random {@link RexNode} instances for tests.
 */
public class RexFuzzer extends RexProgramBuilderBase {

    private static final int MAX_VARS = 2;

    private static final Operator[] BOOL_TO_BOOL = {
            OperatorRegistry.get( OperatorName.NOT ),
            OperatorRegistry.get( OperatorName.IS_TRUE ),
            OperatorRegistry.get( OperatorName.IS_FALSE ),
            OperatorRegistry.get( OperatorName.IS_NOT_TRUE ),
            OperatorRegistry.get( OperatorName.IS_NOT_FALSE ),
    };

    private static final Operator[] ANY_TO_BOOL = {
            OperatorRegistry.get( OperatorName.IS_NULL ),
            OperatorRegistry.get( OperatorName.IS_NOT_NULL ),
            OperatorRegistry.get( OperatorName.IS_UNKNOWN ),
            OperatorRegistry.get( OperatorName.IS_NOT_UNKNOWN ),
    };

    private static final Operator[] COMPARABLE_TO_BOOL = {
            OperatorRegistry.get( OperatorName.EQUALS ),
            OperatorRegistry.get( OperatorName.NOT_EQUALS ),
            OperatorRegistry.get( OperatorName.GREATER_THAN ),
            OperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL ),
            OperatorRegistry.get( OperatorName.LESS_THAN ),
            OperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL ),
            OperatorRegistry.get( OperatorName.IS_DISTINCT_FROM ),
            OperatorRegistry.get( OperatorName.IS_NOT_DISTINCT_FROM ),
    };

    private static final Operator[] BOOL_TO_BOOL_MULTI_ARG = {
            OperatorRegistry.get( OperatorName.OR ),
            OperatorRegistry.get( OperatorName.AND ),
            OperatorRegistry.get( OperatorName.COALESCE ),
    };

    private static final Operator[] ANY_SAME_TYPE_MULTI_ARG = {
            OperatorRegistry.get( OperatorName.COALESCE ),
    };

    private static final Operator[] NUMERIC_TO_NUMERIC = {
            OperatorRegistry.get( OperatorName.PLUS ),
            OperatorRegistry.get( OperatorName.MINUS ),
            OperatorRegistry.get( OperatorName.MULTIPLY ),
            // Divide by zero is not allowed, so we do not generate divide StdOperatorRegistry.get( OperatorName.DIVIDE ), StdOperatorRegistry.get( OperatorName.DIVIDE_INTEGER ),
    };

    private static final Operator[] UNARY_NUMERIC = {
            OperatorRegistry.get( OperatorName.UNARY_MINUS ),
            OperatorRegistry.get( OperatorName.UNARY_PLUS ),
    };


    private static final int[] INT_VALUES = { -1, 0, 1, 100500 };

    private final AlgDataType intType;
    private final AlgDataType nullableIntType;


    /**
     * Generates randomized {@link RexNode}.
     *
     * @param rexBuilder builder to be used to create nodes
     * @param typeFactory type factory
     */
    public RexFuzzer( RexBuilder rexBuilder, JavaTypeFactory typeFactory ) {
        setUp();
        this.rexBuilder = rexBuilder;
        this.typeFactory = typeFactory;

        intType = typeFactory.createPolyType( PolyType.INTEGER );
        nullableIntType = typeFactory.createTypeWithNullability( intType, true );
    }


    public RexNode getExpression( Random r, int depth ) {
        return getComparableExpression( r, depth );
    }


    private RexNode fuzzOperator( Random r, Node[] operators, RexNode... args ) {
        return rexBuilder.makeCall( (Operator) operators[r.nextInt( operators.length )], args );
    }


    private RexNode fuzzOperator( Random r, Node[] operators, int length, Function<Random, RexNode> factory ) {
        List<RexNode> args = new ArrayList<>( length );
        for ( int i = 0; i < length; i++ ) {
            args.add( factory.apply( r ) );
        }
        return rexBuilder.makeCall( (Operator) operators[r.nextInt( operators.length )], args );
    }


    public RexNode getComparableExpression( Random r, int depth ) {
        int v = r.nextInt( 2 );
        switch ( v ) {
            case 0:
                return getBoolExpression( r, depth );
            case 1:
                return getIntExpression( r, depth );
        }
        throw new AssertionError( "should not reach here" );
    }


    public RexNode getSimpleBool( Random r ) {
        int v = r.nextInt( 2 );
        switch ( v ) {
            case 0:
                boolean nullable = r.nextBoolean();
                int field = r.nextInt( MAX_VARS );
                return nullable ? vBool( field ) : vBoolNotNull( field );
            case 1:
                return r.nextBoolean() ? trueLiteral : falseLiteral;
            case 2:
                return nullBool;
        }
        throw new AssertionError( "should not reach here" );
    }


    public RexNode getBoolExpression( Random r, int depth ) {
        int v = depth <= 0 ? 0 : r.nextInt( 7 );
        switch ( v ) {
            case 0:
                return getSimpleBool( r );
            case 1:
                return fuzzOperator( r, (Node[]) ANY_TO_BOOL, getExpression( r, depth - 1 ) );
            case 2:
                return fuzzOperator( r, (Node[]) BOOL_TO_BOOL, getBoolExpression( r, depth - 1 ) );
            case 3:
                return fuzzOperator( r, (Node[]) COMPARABLE_TO_BOOL, getBoolExpression( r, depth - 1 ), getBoolExpression( r, depth - 1 ) );
            case 4:
                return fuzzOperator( r, (Node[]) COMPARABLE_TO_BOOL, getIntExpression( r, depth - 1 ), getIntExpression( r, depth - 1 ) );
            case 5:
                return fuzzOperator( r, (Node[]) BOOL_TO_BOOL_MULTI_ARG, r.nextInt( 3 ) + 2, x -> getBoolExpression( x, depth - 1 ) );
            case 6:
                return fuzzCase( r, depth - 1, x -> getBoolExpression( x, depth - 1 ) );
        }
        throw new AssertionError( "should not reach here" );
    }


    public RexNode getSimpleInt( Random r ) {
        int v = r.nextInt( 3 );
        switch ( v ) {
            case 0:
                boolean nullable = r.nextBoolean();
                int field = r.nextInt( MAX_VARS );
                return nullable ? vInt( field ) : vIntNotNull( field );
            case 1: {
                int i = r.nextInt( INT_VALUES.length + 1 );
                int val = i < INT_VALUES.length ? INT_VALUES[i] : r.nextInt();
                return rexBuilder.makeLiteral( val, r.nextBoolean() ? intType : nullableIntType, false );
            }
            case 2:
                return nullInt;
        }
        throw new AssertionError( "should not reach here" );
    }


    public RexNode getIntExpression( Random r, int depth ) {
        int v = depth <= 0 ? 0 : r.nextInt( 5 );
        switch ( v ) {
            case 0:
                return getSimpleInt( r );
            case 1:
                return fuzzOperator( r, (Node[]) UNARY_NUMERIC, getIntExpression( r, depth - 1 ) );
            case 2:
                return fuzzOperator( r, (Node[]) NUMERIC_TO_NUMERIC, getIntExpression( r, depth - 1 ), getIntExpression( r, depth - 1 ) );
            case 3:
                return fuzzOperator( r, (Node[]) ANY_SAME_TYPE_MULTI_ARG, r.nextInt( 3 ) + 2, x -> getIntExpression( x, depth - 1 ) );
            case 4:
                return fuzzCase( r, depth - 1, x -> getIntExpression( x, depth - 1 ) );
        }
        throw new AssertionError( "should not reach here" );
    }


    public RexNode fuzzCase( Random r, int depth, Function<Random, RexNode> resultFactory ) {
        boolean caseArgWhen = r.nextBoolean();
        int caseBranches = 1 + (depth <= 0 ? 0 : r.nextInt( 3 ));
        List<RexNode> args = new ArrayList<>( caseBranches + 1 );

        Function<Random, RexNode> exprFactory;
        if ( !caseArgWhen ) {
            exprFactory = x -> getBoolExpression( x, depth - 1 );
        } else {
            int type = r.nextInt( 2 );
            RexNode arg;
            Function<Random, RexNode> baseExprFactory;
            switch ( type ) {
                case 0:
                    baseExprFactory = x -> getBoolExpression( x, depth - 1 );
                    break;
                case 1:
                    baseExprFactory = x -> getIntExpression( x, depth - 1 );
                    break;
                default:
                    throw new AssertionError( "should not reach here: " + type );
            }
            arg = baseExprFactory.apply( r );
            // emulate  case when arg=2 then .. when arg=4 then ...
            exprFactory = x -> eq( arg, baseExprFactory.apply( x ) );
        }

        for ( int i = 0; i < caseBranches; i++ ) {
            args.add( exprFactory.apply( r ) ); // when
            args.add( resultFactory.apply( r ) ); // then
        }
        args.add( resultFactory.apply( r ) ); // else
        return case_( args );
    }

}

