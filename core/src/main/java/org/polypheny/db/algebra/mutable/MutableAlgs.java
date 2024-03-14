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

package org.polypheny.db.algebra.mutable;


import com.google.common.collect.Lists;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Calc;
import org.polypheny.db.algebra.core.Collect;
import org.polypheny.db.algebra.core.Correlate;
import org.polypheny.db.algebra.core.Exchange;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Intersect;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.Minus;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.RelTableFunctionScan;
import org.polypheny.db.algebra.core.Sample;
import org.polypheny.db.algebra.core.SemiJoin;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Uncollect;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.core.Window;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.logical.relational.LogicalCalc;
import org.polypheny.db.algebra.logical.relational.LogicalRelCorrelate;
import org.polypheny.db.algebra.logical.relational.LogicalRelExchange;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelSort;
import org.polypheny.db.algebra.logical.relational.LogicalRelTableFunctionScan;
import org.polypheny.db.algebra.logical.relational.LogicalWindow;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.hep.HepAlgVertex;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.mapping.Mappings;


/**
 * Utilities for dealing with {@link MutableAlg}s.
 */
public abstract class MutableAlgs {

    public static boolean contains( MutableAlg ancestor, final MutableAlg target ) {
        if ( ancestor.equals( target ) ) {
            // Short-cut common case.
            return true;
        }
        try {
            new MutableAlgVisitor() {
                @Override
                public void visit( MutableAlg node ) {
                    if ( node.equals( target ) ) {
                        throw Util.FoundOne.NULL;
                    }
                    super.visit( node );
                }
                // CHECKSTYLE: IGNORE 1
            }.go( ancestor );
            return false;
        } catch ( Util.FoundOne e ) {
            return true;
        }
    }


    public static MutableAlg preOrderTraverseNext( MutableAlg node ) {
        MutableAlg parent = node.getParent();
        int ordinal = node.ordinalInParent + 1;
        while ( parent != null ) {
            if ( parent.getInputs().size() > ordinal ) {
                return parent.getInputs().get( ordinal );
            }
            node = parent;
            parent = node.getParent();
            ordinal = node.ordinalInParent + 1;
        }
        return null;
    }


    public static List<MutableAlg> descendants( MutableAlg query ) {
        final List<MutableAlg> list = new ArrayList<>();
        descendantsRecurse( list, query );
        return list;
    }


    private static void descendantsRecurse( List<MutableAlg> list, MutableAlg alg ) {
        list.add( alg );
        for ( MutableAlg input : alg.getInputs() ) {
            descendantsRecurse( list, input );
        }
    }


    /**
     * Based on {@link org.polypheny.db.algebra.rules.ProjectRemoveRule#strip}.
     */
    public static MutableAlg strip( MutableProject project ) {
        return isTrivial( project ) ? project.getInput() : project;
    }


    /**
     * Based on {@link org.polypheny.db.algebra.rules.ProjectRemoveRule#isTrivial(org.polypheny.db.algebra.core.Project)}.
     */
    public static boolean isTrivial( MutableProject project ) {
        MutableAlg child = project.getInput();
        return RexUtil.isIdentity( project.projects, child.rowType );
    }


    /**
     * Equivalent to {@link AlgOptUtil#createProject(AlgNode, java.util.List)} for {@link MutableAlg}.
     */
    public static MutableAlg createProject( final MutableAlg child, final List<Integer> posList ) {
        final AlgDataType rowType = child.rowType;
        if ( Mappings.isIdentity( posList, rowType.getFieldCount() ) ) {
            return child;
        }
        return MutableProject.of(
                AlgOptUtil.permute( child.cluster.getTypeFactory(), rowType, Mappings.bijection( posList ) ),
                child,
                new AbstractList<RexNode>() {
                    @Override
                    public int size() {
                        return posList.size();
                    }


                    @Override
                    public RexNode get( int index ) {
                        final int pos = posList.get( index );
                        return RexIndexRef.of( pos, rowType );
                    }
                } );
    }


    /**
     * Equivalence to {@link AlgOptUtil#createCastAlg} for {@link MutableAlg}.
     */
    public static MutableAlg createCastAlg( MutableAlg alg, AlgDataType castRowType, boolean rename ) {
        AlgDataType rowType = alg.rowType;
        if ( AlgOptUtil.areRowTypesEqual( rowType, castRowType, rename ) ) {
            // nothing to do
            return alg;
        }
        List<RexNode> castExps = RexUtil.generateCastExpressions( alg.cluster.getRexBuilder(), castRowType, rowType );
        final List<String> fieldNames = rename
                ? castRowType.getFieldNames()
                : rowType.getFieldNames();
        return MutableProject.of( alg, castExps, fieldNames );
    }


    public static AlgNode fromMutable( MutableAlg node ) {
        return fromMutable( node, AlgFactories.LOGICAL_BUILDER.create( node.cluster, null ) );
    }


    public static AlgNode fromMutable( MutableAlg node, AlgBuilder algBuilder ) {
        switch ( node.type ) {
            case TABLE_SCAN:
            case VALUES:
                return ((MutableLeafAlg) node).alg;
            case PROJECT:
                final MutableProject project = (MutableProject) node;
                algBuilder.push( fromMutable( project.input, algBuilder ) );
                algBuilder.project( project.projects, project.rowType.getFieldNames(), true );
                return algBuilder.build();
            case FILTER:
                final MutableFilter filter = (MutableFilter) node;
                algBuilder.push( fromMutable( filter.input, algBuilder ) );
                algBuilder.filter( filter.condition );
                return algBuilder.build();
            case AGGREGATE:
                final MutableAggregate aggregate = (MutableAggregate) node;
                algBuilder.push( fromMutable( aggregate.input, algBuilder ) );
                algBuilder.aggregate( algBuilder.groupKey( aggregate.groupSet, aggregate.groupSets ), aggregate.aggCalls );
                return algBuilder.build();
            case SORT:
                final MutableSort sort = (MutableSort) node;
                return LogicalRelSort.create( fromMutable( sort.input, algBuilder ), sort.collation, sort.offset, sort.fetch );
            case CALC:
                final MutableCalc calc = (MutableCalc) node;
                return LogicalCalc.create( fromMutable( calc.input, algBuilder ), calc.program );
            case EXCHANGE:
                final MutableExchange exchange = (MutableExchange) node;
                return LogicalRelExchange.create( fromMutable( exchange.getInput(), algBuilder ), exchange.distribution );
            case COLLECT: {
                final MutableCollect collect = (MutableCollect) node;
                final AlgNode child = fromMutable( collect.getInput(), algBuilder );
                return new Collect( collect.cluster, child.getTraitSet(), child, collect.fieldName );
            }
            case UNCOLLECT: {
                final MutableUncollect uncollect = (MutableUncollect) node;
                final AlgNode child = fromMutable( uncollect.getInput(), algBuilder );
                return Uncollect.create( child.getTraitSet(), child, uncollect.withOrdinality );
            }
            case WINDOW: {
                final MutableWindow window = (MutableWindow) node;
                final AlgNode child = fromMutable( window.getInput(), algBuilder );
                return LogicalWindow.create( child.getTraitSet(), child, window.constants, window.rowType, window.groups );
            }
            case TABLE_MODIFY:
                final MutableTableModify modify = (MutableTableModify) node;
                return LogicalRelModify.create(
                        modify.table,
                        fromMutable( modify.getInput(), algBuilder ),
                        modify.operation,
                        modify.updateColumnList,
                        modify.sourceExpressionList,
                        modify.flattened );
            case SAMPLE:
                final MutableSample sample = (MutableSample) node;
                return new Sample( sample.cluster, fromMutable( sample.getInput(), algBuilder ), sample.params );
            case TABLE_FUNCTION_SCAN:
                final MutableTableFunctionScan tableFunctionScan = (MutableTableFunctionScan) node;
                return LogicalRelTableFunctionScan.create(
                        tableFunctionScan.cluster,
                        fromMutables( tableFunctionScan.getInputs(), algBuilder ),
                        tableFunctionScan.rexCall,
                        tableFunctionScan.elementType,
                        tableFunctionScan.rowType,
                        tableFunctionScan.columnMappings );
            case JOIN:
                final MutableJoin join = (MutableJoin) node;
                algBuilder.push( fromMutable( join.getLeft(), algBuilder ) );
                algBuilder.push( fromMutable( join.getRight(), algBuilder ) );
                algBuilder.join( join.joinType, join.condition, join.variablesSet );
                return algBuilder.build();
            case SEMIJOIN:
                final MutableSemiJoin semiJoin = (MutableSemiJoin) node;
                algBuilder.push( fromMutable( semiJoin.getLeft(), algBuilder ) );
                algBuilder.push( fromMutable( semiJoin.getRight(), algBuilder ) );
                algBuilder.semiJoin( semiJoin.condition );
                return algBuilder.build();
            case CORRELATE:
                final MutableCorrelate correlate = (MutableCorrelate) node;
                return LogicalRelCorrelate.create(
                        fromMutable( correlate.getLeft(), algBuilder ),
                        fromMutable( correlate.getRight(), algBuilder ),
                        correlate.correlationId,
                        correlate.requiredColumns,
                        correlate.joinType );
            case UNION:
                final MutableUnion union = (MutableUnion) node;
                algBuilder.pushAll( MutableAlgs.fromMutables( union.inputs, algBuilder ) );
                algBuilder.union( union.all, union.inputs.size() );
                return algBuilder.build();
            case MINUS:
                final MutableMinus minus = (MutableMinus) node;
                algBuilder.pushAll( MutableAlgs.fromMutables( minus.inputs, algBuilder ) );
                algBuilder.minus( minus.all, minus.inputs.size() );
                return algBuilder.build();
            case INTERSECT:
                final MutableIntersect intersect = (MutableIntersect) node;
                algBuilder.pushAll( MutableAlgs.fromMutables( intersect.inputs, algBuilder ) );
                algBuilder.intersect( intersect.all, intersect.inputs.size() );
                return algBuilder.build();
            default:
                throw new AssertionError( node.deep() );
        }
    }


    private static List<AlgNode> fromMutables( List<MutableAlg> nodes, final AlgBuilder algBuilder ) {
        return Lists.transform( nodes, mutableRel -> fromMutable( mutableRel, algBuilder ) );
    }


    public static MutableAlg toMutable( AlgNode alg ) {
        if ( alg instanceof HepAlgVertex ) {
            return toMutable( ((HepAlgVertex) alg).getCurrentAlg() );
        }
        if ( alg instanceof AlgSubset ) {
            return toMutable(
                    Util.first( ((AlgSubset) alg).getBest(), ((AlgSubset) alg).getOriginal() ) );
        }
        if ( alg instanceof RelScan ) {
            return MutableScan.of( (RelScan) alg );
        }
        if ( alg instanceof Values ) {
            return MutableValues.of( (Values) alg );
        }
        if ( alg instanceof Project ) {
            final Project project = (Project) alg;
            final MutableAlg input = toMutable( project.getInput() );
            return MutableProject.of( input, project.getProjects(), project.getTupleType().getFieldNames() );
        }
        if ( alg instanceof Filter ) {
            final Filter filter = (Filter) alg;
            final MutableAlg input = toMutable( filter.getInput() );
            return MutableFilter.of( input, filter.getCondition() );
        }
        if ( alg instanceof Aggregate ) {
            final Aggregate aggregate = (Aggregate) alg;
            final MutableAlg input = toMutable( aggregate.getInput() );
            return MutableAggregate.of( input, aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList() );
        }
        if ( alg instanceof Sort ) {
            final Sort sort = (Sort) alg;
            final MutableAlg input = toMutable( sort.getInput() );
            return MutableSort.of( input, sort.getCollation(), sort.offset, sort.fetch );
        }
        if ( alg instanceof Calc ) {
            final Calc calc = (Calc) alg;
            final MutableAlg input = toMutable( calc.getInput() );
            return MutableCalc.of( input, calc.getProgram() );
        }
        if ( alg instanceof Exchange ) {
            final Exchange exchange = (Exchange) alg;
            final MutableAlg input = toMutable( exchange.getInput() );
            return MutableExchange.of( input, exchange.getDistribution() );
        }
        if ( alg instanceof Collect ) {
            final Collect collect = (Collect) alg;
            final MutableAlg input = toMutable( collect.getInput() );
            return MutableCollect.of( collect.getTupleType(), input, collect.getFieldName() );
        }
        if ( alg instanceof Uncollect ) {
            final Uncollect uncollect = (Uncollect) alg;
            final MutableAlg input = toMutable( uncollect.getInput() );
            return MutableUncollect.of( uncollect.getTupleType(), input, uncollect.withOrdinality );
        }
        if ( alg instanceof Window ) {
            final Window window = (Window) alg;
            final MutableAlg input = toMutable( window.getInput() );
            return MutableWindow.of( window.getTupleType(), input, window.groups, window.getConstants() );
        }
        if ( alg instanceof RelModify ) {
            final RelModify modify = (RelModify) alg;
            final MutableAlg input = toMutable( modify.getInput() );
            return MutableTableModify.of(
                    modify.getTupleType(),
                    input,
                    modify.getEntity(),
                    modify.getOperation(),
                    modify.getUpdateColumns(),
                    modify.getSourceExpressions(),
                    modify.isFlattened() );
        }
        if ( alg instanceof Sample ) {
            final Sample sample = (Sample) alg;
            final MutableAlg input = toMutable( sample.getInput() );
            return MutableSample.of( input, sample.getSamplingParameters() );
        }
        if ( alg instanceof RelTableFunctionScan ) {
            final RelTableFunctionScan relTableFunctionScan = (RelTableFunctionScan) alg;
            final List<MutableAlg> inputs = toMutables( relTableFunctionScan.getInputs() );
            return MutableTableFunctionScan.of(
                    relTableFunctionScan.getCluster(),
                    relTableFunctionScan.getTupleType(),
                    inputs,
                    relTableFunctionScan.getCall(),
                    relTableFunctionScan.getElementType(),
                    relTableFunctionScan.getColumnMappings() );
        }
        // It is necessary that SemiJoin is placed in front of Join here, since SemiJoin is a sub-class of Join.
        if ( alg instanceof SemiJoin ) {
            final SemiJoin semiJoin = (SemiJoin) alg;
            final MutableAlg left = toMutable( semiJoin.getLeft() );
            final MutableAlg right = toMutable( semiJoin.getRight() );
            return MutableSemiJoin.of(
                    semiJoin.getTupleType(),
                    left, right,
                    semiJoin.getCondition(),
                    semiJoin.getLeftKeys(),
                    semiJoin.getRightKeys() );
        }
        if ( alg instanceof Join ) {
            final Join join = (Join) alg;
            final MutableAlg left = toMutable( join.getLeft() );
            final MutableAlg right = toMutable( join.getRight() );
            return MutableJoin.of(
                    join.getTupleType(),
                    left,
                    right,
                    join.getCondition(),
                    join.getJoinType(),
                    join.getVariablesSet() );
        }
        if ( alg instanceof Correlate ) {
            final Correlate correlate = (Correlate) alg;
            final MutableAlg left = toMutable( correlate.getLeft() );
            final MutableAlg right = toMutable( correlate.getRight() );
            return MutableCorrelate.of(
                    correlate.getTupleType(),
                    left,
                    right,
                    correlate.getCorrelationId(),
                    correlate.getRequiredColumns(),
                    correlate.getJoinType() );
        }
        if ( alg instanceof Union ) {
            final Union union = (Union) alg;
            final List<MutableAlg> inputs = toMutables( union.getInputs() );
            return MutableUnion.of( union.getTupleType(), inputs, union.all );
        }
        if ( alg instanceof Minus ) {
            final Minus minus = (Minus) alg;
            final List<MutableAlg> inputs = toMutables( minus.getInputs() );
            return MutableMinus.of( minus.getTupleType(), inputs, minus.all );
        }
        if ( alg instanceof Intersect ) {
            final Intersect intersect = (Intersect) alg;
            final List<MutableAlg> inputs = toMutables( intersect.getInputs() );
            return MutableIntersect.of( intersect.getTupleType(), inputs, intersect.all );
        }
        throw new RuntimeException( "cannot translate " + alg + " to MutableRel" );
    }


    private static List<MutableAlg> toMutables( List<AlgNode> nodes ) {
        return Lists.transform( nodes, MutableAlgs::toMutable );
    }

}
