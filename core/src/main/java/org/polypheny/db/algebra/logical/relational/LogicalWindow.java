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

package org.polypheny.db.algebra.logical.relational;


import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Window;
import org.polypheny.db.algebra.core.relational.RelAlg;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexLocalRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexOver;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.rex.RexWindow;
import org.polypheny.db.rex.RexWindowBound;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Util;


/**
 * Sub-class of {@link Window} not targeted at any particular engine or calling convention.
 */
public final class LogicalWindow extends Window implements RelAlg {

    /**
     * Creates a LogicalWindow.
     *
     * Use {@link #create} unless you know what you're doing.
     *
     * @param cluster Cluster
     * @param traitSet Trait set
     * @param input Input relational expression
     * @param constants List of constants that are additional inputs
     * @param rowType Output row type
     * @param groups Window groups
     */
    public LogicalWindow( AlgCluster cluster, AlgTraitSet traitSet, AlgNode input, List<RexLiteral> constants, AlgDataType rowType, List<Group> groups ) {
        super( cluster, traitSet, input, constants, rowType, groups );
    }


    @Override
    public LogicalWindow copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalWindow( getCluster(), traitSet, sole( inputs ), constants, rowType, groups );
    }


    /**
     * Creates a LogicalWindow.
     *
     * @param input Input relational expression
     * @param traitSet Trait set
     * @param constants List of constants that are additional inputs
     * @param rowType Output row type
     * @param groups Window groups
     */
    public static LogicalWindow create( AlgTraitSet traitSet, AlgNode input, List<RexLiteral> constants, AlgDataType rowType, List<Group> groups ) {
        return new LogicalWindow( input.getCluster(), traitSet, input, constants, rowType, groups );
    }


    /**
     * Creates a LogicalWindow by parsing a {@link RexProgram}.
     */
    public static AlgNode create( AlgCluster cluster, AlgTraitSet traitSet, AlgBuilder algBuilder, AlgNode child, final RexProgram program ) {
        final AlgDataType outRowType = program.getOutputRowType();
        // Build a list of distinct groups, partitions and aggregate functions.
        final Multimap<WindowKey, RexOver> windowMap = LinkedListMultimap.create();

        final int inputFieldCount = child.getTupleType().getFieldCount();

        final Map<RexLiteral, RexIndexRef> constantPool = new HashMap<>();
        final List<RexLiteral> constants = new ArrayList<>();

        // Identify constants in the expression tree and replace them with references to newly generated constant pool.
        RexShuttle replaceConstants = new RexShuttle() {
            @Override
            public RexNode visitLiteral( RexLiteral literal ) {
                RexIndexRef ref = constantPool.get( literal );
                if ( ref != null ) {
                    return ref;
                }
                constants.add( literal );
                ref = new RexIndexRef( constantPool.size() + inputFieldCount, literal.getType() );
                constantPool.put( literal, ref );
                return ref;
            }
        };

        // Build a list of groups, partitions, and aggregate functions. Each aggregate function will add its arguments as outputs of the input program.
        final Map<RexOver, RexOver> origToNewOver = new IdentityHashMap<>();
        for ( RexNode agg : program.getExprList() ) {
            if ( agg instanceof RexOver ) {
                final RexOver origOver = (RexOver) agg;
                final RexOver newOver = (RexOver) origOver.accept( replaceConstants );
                origToNewOver.put( origOver, newOver );
                addWindows( windowMap, newOver, inputFieldCount );
            }
        }

        final Map<RexOver, Window.RexWinAggCall> aggMap = new HashMap<>();
        List<Group> groups = new ArrayList<>();
        for ( Map.Entry<WindowKey, Collection<RexOver>> entry : windowMap.asMap().entrySet() ) {
            final WindowKey windowKey = entry.getKey();
            final List<RexWinAggCall> aggCalls = new ArrayList<>();
            for ( RexOver over : entry.getValue() ) {
                final RexWinAggCall aggCall = new RexWinAggCall(
                        over.getAggOperator(),
                        over.getType(),
                        toInputRefs( over.operands ),
                        aggMap.size(),
                        over.isDistinct() );
                aggCalls.add( aggCall );
                aggMap.put( over, aggCall );
            }
            RexShuttle toInputRefs = new RexShuttle() {
                @Override
                public RexNode visitLocalRef( RexLocalRef localRef ) {
                    return new RexIndexRef( localRef.getIndex(), localRef.getType() );
                }
            };
            groups.add(
                    new Group(
                            windowKey.groupSet,
                            windowKey.isRows,
                            windowKey.lowerBound.accept( toInputRefs ),
                            windowKey.upperBound.accept( toInputRefs ),
                            windowKey.orderKeys,
                            aggCalls ) );
        }

        // Figure out the type of the inputs to the output program.
        // They are: the inputs to this alg, followed by the outputs of each window.
        final List<Window.RexWinAggCall> flattenedAggCallList = new ArrayList<>();
        final List<AlgDataTypeField> fieldList = new ArrayList<>( child.getTupleType().getFields() );
        final int offset = fieldList.size();

        // Use better field names for agg calls that are projected.
        final Map<Integer, String> fieldNames = new HashMap<>();
        for ( Ord<RexLocalRef> ref : Ord.zip( program.getProjectList() ) ) {
            final int index = ref.e.getIndex();
            if ( index >= offset ) {
                fieldNames.put( index - offset, outRowType.getFieldNames().get( ref.i ) );
            }
        }
        int j = 0;
        for ( Ord<Group> window : Ord.zip( groups ) ) {
            for ( Ord<RexWinAggCall> over : Ord.zip( window.e.aggCalls ) ) {
                // Add the k-th over expression of the i-th window to the output of the program.
                String name = fieldNames.get( over.i );
                if ( name == null || name.startsWith( "$" ) ) {
                    name = "w" + window.i + "$o" + over.i;
                }
                fieldList.add( new AlgDataTypeFieldImpl( -1L, name, j++, over.e.getType() ) );
                flattenedAggCallList.add( over.e );
            }
        }
        final AlgDataType intermediateRowType = cluster.getTypeFactory().createStructType( fieldList );

        // The output program is the windowed agg's program, combined with the output calc (if it exists).
        RexShuttle shuttle =
                new RexShuttle() {
                    @Override
                    public RexNode visitOver( RexOver over ) {
                        // Look up the aggCall which this expr was translated to.
                        final Window.RexWinAggCall aggCall = aggMap.get( origToNewOver.get( over ) );
                        assert aggCall != null;
                        assert AlgOptUtil.eq(
                                "over",
                                over.getType(),
                                "aggCall",
                                aggCall.getType(),
                                Litmus.THROW );

                        // Find the index of the aggCall among all partitions of all groups.
                        final int aggCallIndex = flattenedAggCallList.indexOf( aggCall );
                        assert aggCallIndex >= 0;

                        // Replace expression with a reference to the window slot.
                        final int index = inputFieldCount + aggCallIndex;
                        assert AlgOptUtil.eq(
                                "over",
                                over.getType(),
                                "intermed",
                                intermediateRowType.getFields().get( index ).getType(),
                                Litmus.THROW );
                        return new RexIndexRef( index, over.getType() );
                    }


                    @Override
                    public RexNode visitLocalRef( RexLocalRef localRef ) {
                        final int index = localRef.getIndex();
                        if ( index < inputFieldCount ) {
                            // Reference to input field.
                            return localRef;
                        }
                        return new RexLocalRef( flattenedAggCallList.size() + index, localRef.getType() );
                    }
                };

        final LogicalWindow window = LogicalWindow.create( traitSet, child, constants, intermediateRowType, groups );

        // The order that the "over" calls occur in the groups and partitions may not match the order in which they occurred in the original expression.
        // Add a project to permute them.
        final List<RexNode> rexNodesWindow = new ArrayList<>();
        for ( RexNode rexNode : program.getExprList() ) {
            rexNodesWindow.add( rexNode.accept( shuttle ) );
        }
        final List<RexNode> refToWindow = toInputRefs( rexNodesWindow );

        final List<RexNode> projectList = new ArrayList<>();
        for ( RexLocalRef inputRef : program.getProjectList() ) {
            final int index = inputRef.getIndex();
            final RexIndexRef ref = (RexIndexRef) refToWindow.get( index );
            projectList.add( ref );
        }

        return algBuilder.push( window )
                .project( projectList, outRowType.getFieldNames() )
                .build();
    }


    private static List<RexNode> toInputRefs( final List<? extends RexNode> operands ) {
        return new AbstractList<RexNode>() {
            @Override
            public int size() {
                return operands.size();
            }


            @Override
            public RexNode get( int index ) {
                final RexNode operand = operands.get( index );
                if ( operand instanceof RexIndexRef ) {
                    return operand;
                }
                assert operand instanceof RexLocalRef;
                final RexLocalRef ref = (RexLocalRef) operand;
                return new RexIndexRef( ref.getIndex(), ref.getType() );
            }
        };
    }


    /**
     * Group specification. All windowed aggregates over the same window (regardless of how it is specified, in terms of a named window or specified attribute by attribute)
     * will end up with the same window key.
     */
    private static class WindowKey {

        private final ImmutableBitSet groupSet;
        private final AlgCollation orderKeys;
        private final boolean isRows;
        private final RexWindowBound lowerBound;
        private final RexWindowBound upperBound;


        WindowKey( ImmutableBitSet groupSet, AlgCollation orderKeys, boolean isRows, RexWindowBound lowerBound, RexWindowBound upperBound ) {
            this.groupSet = groupSet;
            this.orderKeys = orderKeys;
            this.isRows = isRows;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }


        @Override
        public int hashCode() {
            return Objects.hash( groupSet, orderKeys, isRows, lowerBound, upperBound );
        }


        @Override
        public boolean equals( Object obj ) {
            return obj == this
                    || obj instanceof WindowKey
                    && groupSet.equals( ((WindowKey) obj).groupSet )
                    && orderKeys.equals( ((WindowKey) obj).orderKeys )
                    && Objects.equals( lowerBound, ((WindowKey) obj).lowerBound )
                    && Objects.equals( upperBound, ((WindowKey) obj).upperBound )
                    && isRows == ((WindowKey) obj).isRows;
        }

    }


    private static void addWindows( Multimap<WindowKey, RexOver> windowMap, RexOver over, final int inputFieldCount ) {
        final RexWindow aggWindow = over.getWindow();

        // Look up or create a window.
        AlgCollation orderKeys = getCollation(
                Lists.newArrayList(
                        Util.filter(
                                aggWindow.orderKeys,
                                rexFieldCollation ->
                                        // If ORDER BY references constant (i.e. RexInputRef), then we can ignore such ORDER BY key.
                                        rexFieldCollation.left instanceof RexLocalRef ) ) );
        ImmutableBitSet groupSet = ImmutableBitSet.of( getProjectOrdinals( aggWindow.partitionKeys ) );
        final int groupLength = groupSet.length();
        if ( inputFieldCount < groupLength ) {
            // If PARTITION BY references constant, we can ignore such partition key.
            // All the inputs after inputFieldCount are literals, thus we can clear.
            groupSet = groupSet.except( ImmutableBitSet.range( inputFieldCount, groupLength ) );
        }

        WindowKey windowKey = new WindowKey( groupSet, orderKeys, aggWindow.isRows(), aggWindow.getLowerBound(), aggWindow.getUpperBound() );
        windowMap.put( windowKey, over );
    }

}

