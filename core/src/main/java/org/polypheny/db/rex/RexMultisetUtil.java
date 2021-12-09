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


import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Set;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.type.PolyType;


/**
 * Utility class for various methods related to multisets.
 */
public class RexMultisetUtil {

    /**
     * A set defining all implementable multiset calls
     */
    private static final Set<Operator> MULTISET_OPERATORS =
            ImmutableSet.of(
                    OperatorRegistry.get( OperatorName.CARDINALITY ),
                    OperatorRegistry.get( OperatorName.CAST ),
                    OperatorRegistry.get( OperatorName.ELEMENT ),
                    OperatorRegistry.get( OperatorName.ELEMENT_SLICE ),
                    OperatorRegistry.get( OperatorName.MULTISET_EXCEPT_DISTINCT ),
                    OperatorRegistry.get( OperatorName.MULTISET_EXCEPT ),
                    OperatorRegistry.get( OperatorName.MULTISET_INTERSECT_DISTINCT ),
                    OperatorRegistry.get( OperatorName.MULTISET_INTERSECT ),
                    OperatorRegistry.get( OperatorName.MULTISET_UNION_DISTINCT ),
                    OperatorRegistry.get( OperatorName.MULTISET_UNION ),
                    OperatorRegistry.get( OperatorName.IS_A_SET ),
                    OperatorRegistry.get( OperatorName.IS_NOT_A_SET ),
                    OperatorRegistry.get( OperatorName.MEMBER_OF ),
                    OperatorRegistry.get( OperatorName.NOT_SUBMULTISET_OF ),
                    OperatorRegistry.get( OperatorName.SUBMULTISET_OF ) );


    private RexMultisetUtil() {
    }


    /**
     * Returns true if any expression in a program contains a mixing between multiset and non-multiset calls.
     */
    public static boolean containsMixing( RexProgram program ) {
        RexCallMultisetOperatorCounter counter = new RexCallMultisetOperatorCounter();
        for ( RexNode expr : program.getExprList() ) {
            counter.reset();
            expr.accept( counter );

            if ( (counter.totalCount != counter.multisetCount) && (0 != counter.multisetCount) ) {
                return true;
            }
        }
        return false;
    }


    /**
     * Returns true if a node contains a mixing between multiset and non-multiset calls.
     */
    public static boolean containsMixing( RexNode node ) {
        RexCallMultisetOperatorCounter counter = new RexCallMultisetOperatorCounter();
        node.accept( counter );
        return (counter.totalCount != counter.multisetCount) && (0 != counter.multisetCount);
    }


    /**
     * Returns true if node contains a multiset operator, otherwise false. Use it with deep=false when checking if a RexCall is a multiset call.
     *
     * @param node Expression
     * @param deep If true, returns whether expression contains a multiset. If false, returns whether expression <em>is</em> a multiset.
     */
    public static boolean containsMultiset( final RexNode node, boolean deep ) {
        return null != findFirstMultiset( node, deep );
    }


    /**
     * Returns whether a list of expressions contains a multiset.
     */
    public static boolean containsMultiset( List<RexNode> nodes, boolean deep ) {
        for ( RexNode node : nodes ) {
            if ( containsMultiset( node, deep ) ) {
                return true;
            }
        }
        return false;
    }


    /**
     * Returns whether a program contains a multiset.
     */
    public static boolean containsMultiset( RexProgram program ) {
        return containsMultiset( program.getExprList(), true );
    }


    /**
     * Returns true if {@code call} is a call to <code>CAST</code> and the to/from cast types are of multiset types.
     */
    public static boolean isMultisetCast( RexCall call ) {
        switch ( call.getKind() ) {
            case CAST:
                return call.getType().getPolyType() == PolyType.MULTISET;
            default:
                return false;
        }
    }


    /**
     * Returns a reference to the first found multiset call or null if none was found
     */
    public static RexCall findFirstMultiset( final RexNode node, boolean deep ) {
        if ( node instanceof RexFieldAccess ) {
            return findFirstMultiset( ((RexFieldAccess) node).getReferenceExpr(), deep );
        }

        if ( !(node instanceof RexCall) ) {
            return null;
        }
        final RexCall call = (RexCall) node;
        RexCall firstOne = null;
        for ( Operator op : MULTISET_OPERATORS ) {
            firstOne = RexUtil.findOperatorCall( op, call );
            if ( null != firstOne ) {
                if ( firstOne.getOperator().getOperatorName() == OperatorName.CAST && !isMultisetCast( firstOne ) ) {
                    firstOne = null;
                    continue;
                }
                break;
            }
        }

        if ( !deep && (firstOne != call) ) {
            return null;
        }
        return firstOne;
    }


    /**
     * A RexShuttle that traverse all RexNode and counts total number of RexCalls traversed and number of multiset calls traversed.
     *
     * totalCount {@code >=} multisetCount always holds true.
     */
    private static class RexCallMultisetOperatorCounter extends RexVisitorImpl<Void> {

        int totalCount = 0;
        int multisetCount = 0;


        RexCallMultisetOperatorCounter() {
            super( true );
        }


        void reset() {
            totalCount = 0;
            multisetCount = 0;
        }


        @Override
        public Void visitCall( RexCall call ) {
            ++totalCount;
            if ( MULTISET_OPERATORS.contains( call.getOperator() ) ) {
                if ( call.getOperator().getOperatorName() != OperatorName.CAST || isMultisetCast( call ) ) {
                    ++multisetCount;
                }
            }
            return super.visitCall( call );
        }

    }

}

