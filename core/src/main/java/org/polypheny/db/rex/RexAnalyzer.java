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

package org.polypheny.db.rex;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.linq4j.Linq4j;
import org.polypheny.db.plan.AlgOptPredicateList;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * Analyzes an expression, figures out what are the unbound variables, assigns a variety of values to each unbound variable, and evaluates the expression.
 */
public class RexAnalyzer {

    public final RexNode e;
    public final List<RexNode> variables;
    public final int unsupportedCount;


    /**
     * Creates a RexAnalyzer.
     */
    public RexAnalyzer( RexNode e, AlgOptPredicateList predicates ) {
        this.e = e;
        final VariableCollector variableCollector = new VariableCollector();
        e.accept( variableCollector );
        predicates.pulledUpPredicates.forEach( p -> p.accept( variableCollector ) );
        variables = ImmutableList.copyOf( variableCollector.builder );
        unsupportedCount = variableCollector.unsupportedCount;
    }


    /**
     * Generates a map of variables and lists of values that could be assigned to them.
     */
    public Iterable<Map<RexNode, PolyValue>> assignments() {
        final List<List<PolyValue>> generators = variables.stream().map( RexAnalyzer::getComparables ).collect( Util.toImmutableList() );
        final Iterable<List<PolyValue>> product = Linq4j.product( generators );

        return Iterables.transform( product, values -> ImmutableMap.copyOf( Pair.zip( variables, values ) ) );
    }


    private static List<PolyValue> getComparables( RexNode variable ) {
        final ImmutableList.Builder<PolyValue> values = ImmutableList.builder();
        switch ( variable.getType().getPolyType() ) {
            case BOOLEAN -> {
                values.add( PolyBoolean.TRUE );
                values.add( PolyBoolean.FALSE );
            }
            case INTEGER -> {
                values.add( PolyBigDecimal.of( -1L ) );
                values.add( PolyBigDecimal.of( 0L ) );
                values.add( PolyBigDecimal.of( 1L ) );
                values.add( PolyBigDecimal.of( 1_000_000L ) );
            }
            case VARCHAR -> {
                values.add( PolyString.of( "" ) );
                values.add( PolyString.of( "hello" ) );
            }
            case TIMESTAMP -> values.add( PolyTimestamp.of( 0L ) ); // 1970-01-01 00:00:00
            case DATE -> {
                values.add( PolyDate.ofDays( 0 ) ); // 1970-01-01
                values.add( PolyDate.ofDays( 365 ) ); // 1971-01-01
                values.add( PolyDate.ofDays( -365 ) ); // 1969-01-01
            }
            case TIME -> {
                values.add( PolyTime.of( 0 ) ); // 00:00:00.000
                values.add( PolyTime.of( 86_399_000 ) ); // 23:59:59.000
            }
            default -> throw new AssertionError( "don't know values for " + variable + " of type " + variable.getType() );
        }
        if ( variable.getType().isNullable() ) {
            values.add( PolyNull.NULL );
        }
        return values.build();
    }


    /**
     * Collects the variables (or other bindable sites) in an expression, and counts features (such as CAST) that {@link RexInterpreter} cannot handle.
     */
    private static class VariableCollector extends RexVisitorImpl<Void> {

        private final Set<RexNode> builder = new LinkedHashSet<>();
        private int unsupportedCount = 0;


        VariableCollector() {
            super( true );
        }


        @Override
        public Void visitIndexRef( RexIndexRef inputRef ) {
            builder.add( inputRef );
            return super.visitIndexRef( inputRef );
        }


        @Override
        public Void visitFieldAccess( RexFieldAccess fieldAccess ) {
            if ( fieldAccess.getReferenceExpr() instanceof RexDynamicParam ) {
                builder.add( fieldAccess );
                return null;
            } else {
                return super.visitFieldAccess( fieldAccess );
            }
        }


        @Override
        public Void visitCall( RexCall call ) {
            return switch ( call.getKind() ) {
                case CAST -> {
                    ++unsupportedCount;
                    yield null;
                }
                default -> super.visitCall( call );
            };
        }

    }

}

