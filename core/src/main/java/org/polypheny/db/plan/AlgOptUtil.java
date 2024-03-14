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

package org.polypheny.db.plan;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.annotation.Nonnull;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgHomogeneousShuttle;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.AlgVisitor;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.AlgFactories.FilterFactory;
import org.polypheny.db.algebra.core.Calc;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.SemiJoin;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.enumerable.document.MergeDocumentFilterRule;
import org.polypheny.db.algebra.externalize.AlgJsonWriter;
import org.polypheny.db.algebra.externalize.AlgWriterImpl;
import org.polypheny.db.algebra.externalize.AlgXmlWriter;
import org.polypheny.db.algebra.logical.relational.LogicalCalc;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.rules.AggregateProjectPullUpConstantsRule;
import org.polypheny.db.algebra.rules.DateRangeRules;
import org.polypheny.db.algebra.rules.FilterMergeRule;
import org.polypheny.db.algebra.rules.IntersectToDistinctRule;
import org.polypheny.db.algebra.rules.MultiJoin;
import org.polypheny.db.algebra.rules.ProjectToWindowRules;
import org.polypheny.db.algebra.rules.PruneEmptyRules;
import org.polypheny.db.algebra.rules.UnionMergeRule;
import org.polypheny.db.algebra.rules.UnionPullUpConstantsRule;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.rex.LogicVisitor;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexCorrelVariable;
import org.polypheny.db.rex.RexExecutor;
import org.polypheny.db.rex.RexExecutorImpl;
import org.polypheny.db.rex.RexFieldAccess;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexLocalRef;
import org.polypheny.db.rex.RexMultisetUtil;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexOver;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.rex.RexSubQuery;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.MultisetPolyType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Permutation;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.mapping.Mapping;
import org.polypheny.db.util.mapping.MappingType;
import org.polypheny.db.util.mapping.Mappings;
import org.polypheny.db.util.mapping.Mappings.TargetMapping;


/**
 * <code>RelOptUtil</code> defines static utility methods for use in optimizing {@link AlgNode}s.
 */
public abstract class AlgOptUtil {

    static final boolean B = false;

    public static final double EPSILON = 1.0e-5;


    /**
     * Whether this node is a limit without sort specification.
     */
    public static boolean isPureLimit( AlgNode alg ) {
        return isLimit( alg ) && !isOrder( alg );
    }


    /**
     * Whether this node is a sort without limit specification.
     */
    public static boolean isPureOrder( AlgNode alg ) {
        return !isLimit( alg ) && isOrder( alg );
    }


    /**
     * Whether this node contains a limit specification.
     */
    public static boolean isLimit( AlgNode alg ) {
        if ( (alg instanceof Sort) && ((Sort) alg).fetch != null ) {
            return true;
        }
        return false;
    }


    /**
     * Whether this node contains a sort specification.
     */
    public static boolean isOrder( AlgNode alg ) {
        if ( (alg instanceof Sort) && !((Sort) alg).getCollation().getFieldCollations().isEmpty() ) {
            return true;
        }
        return false;
    }


    /**
     * Returns a set of tables used by this expression or its children
     */
    public static Set<Entity> findTables( AlgNode alg ) {
        return new LinkedHashSet<>( findAllTables( alg ) );
    }


    /**
     * Returns a list of all tables used by this expression or its children
     */
    public static List<Entity> findAllTables( AlgNode alg ) {
        final Multimap<Class<? extends AlgNode>, AlgNode> nodes = AlgMetadataQuery.instance().getNodeTypes( alg );
        final List<Entity> usedTables = new ArrayList<>();
        for ( Entry<Class<? extends AlgNode>, Collection<AlgNode>> e : nodes.asMap().entrySet() ) {
            if ( RelScan.class.isAssignableFrom( e.getKey() ) ) {
                for ( AlgNode node : e.getValue() ) {
                    usedTables.add( node.getEntity() );
                }
            }
        }
        return usedTables;
    }


    /**
     * Returns a list of variables set by a relational expression or its descendants.
     */
    public static Set<CorrelationId> getVariablesSet( AlgNode alg ) {
        VariableSetVisitor visitor = new VariableSetVisitor();
        go( visitor, alg );
        return visitor.variables;
    }


    /**
     * Returns a set of variables used by a relational expression or its descendants.
     *
     * The set may contain "duplicates" (variables with different ids that, when resolved, will reference the same source relational expression).
     *
     * The item type is the same as {@link RexCorrelVariable#id}.
     */
    public static Set<CorrelationId> getVariablesUsed( AlgNode alg ) {
        CorrelationCollector visitor = new CorrelationCollector();
        alg.accept( visitor );
        return visitor.vuv.variables;
    }


    /**
     * Finds which columns of a correlation variable are used within a relational expression.
     */
    public static ImmutableBitSet correlationColumns( CorrelationId id, AlgNode alg ) {
        final CorrelationCollector collector = new CorrelationCollector();
        alg.accept( collector );
        final ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
        for ( int field : collector.vuv.variableFields.get( id ) ) {
            if ( field >= 0 ) {
                builder.set( field );
            }
        }
        return builder.build();
    }


    /**
     * Returns true, and calls {@link Litmus#succeed()} if a given relational expression does not contain a given correlation.
     */
    public static boolean notContainsCorrelation( AlgNode r, CorrelationId correlationId, Litmus litmus ) {
        final Set<CorrelationId> set = getVariablesUsed( r );
        if ( !set.contains( correlationId ) ) {
            return litmus.succeed();
        } else {
            return litmus.fail( "contains {}", correlationId );
        }
    }


    /**
     * Sets a {@link AlgVisitor} going on a given relational expression, and returns the result.
     */
    public static void go( AlgVisitor visitor, AlgNode p ) {
        try {
            visitor.go( p );
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "while visiting tree", e );
        }
    }


    /**
     * Returns a list of the types of the fields in a given struct type. The list is immutable.
     *
     * @param type Struct type
     * @return List of field types
     * @see AlgDataType#getFieldNames()
     */
    public static List<AlgDataType> getFieldTypeList( final AlgDataType type ) {
        return Lists.transform( type.getFields(), AlgDataTypeField::getType );
    }


    public static boolean areRowTypesEqual( AlgDataType rowType1, AlgDataType rowType2, boolean compareNames ) {
        if ( rowType1 == rowType2 ) {
            return true;
        }
        if ( rowType1 instanceof DocumentType || rowType2 instanceof DocumentType ) {
            return true;
        }
        if ( compareNames ) {
            // if types are not identity-equal, then either the names or the types must be different
            return false;
        }
        if ( rowType2.getFieldCount() != rowType1.getFieldCount() ) {
            return false;
        }
        final List<AlgDataTypeField> f1 = rowType1.getFields();
        final List<AlgDataTypeField> f2 = rowType2.getFields();
        for ( Pair<AlgDataTypeField, AlgDataTypeField> pair : Pair.zip( f1, f2 ) ) {
            final AlgDataType type1 = pair.left.getType();
            final AlgDataType type2 = pair.right.getType();
            // If one of the types is ANY comparison should succeed
            if ( type1.getPolyType() == PolyType.ANY || type2.getPolyType() == PolyType.ANY ) {
                continue;
            }
            if ( type1.getPolyType() == PolyType.DOCUMENT || type2.getPolyType() == PolyType.DOCUMENT ) {
                continue;
            }

            if ( !type1.equals( type2 ) ) {
                return false;
            }
        }
        return true;
    }


    /**
     * Verifies that a row type being added to an equivalence class matches the existing type, raising an assertion if this is not the case.
     *
     * @param originalAlg canonical alg for equivalence class
     * @param newAlg alg being added to equivalence class
     * @param equivalenceClass object representing equivalence class
     */
    public static void verifyTypeEquivalence( AlgNode originalAlg, AlgNode newAlg, Object equivalenceClass ) {
        AlgDataType expectedRowType = originalAlg.getTupleType();
        AlgDataType actualRowType = newAlg.getTupleType();

        // Row types must be the same, except for field names.
        if ( areRowTypesEqual( expectedRowType, actualRowType, false ) ) {
            return;
        }

        String s = "Cannot add expression of different type to set:\n"
                + "set type is " + expectedRowType.getFullTypeString()
                + "\nexpression type is " + actualRowType.getFullTypeString()
                + "\nset is " + equivalenceClass.toString()
                + "\nexpression is " + AlgOptUtil.toString( newAlg );
        throw new AssertionError( s );
    }


    /**
     * Returns a permutation describing where output fields come from. In the returned map, value of {@code map.getTargetOpt(i)} is {@code n} if field {@code i} projects
     * input field {@code n} or applies a cast on {@code n}, -1 if it is another expression.
     */
    public static TargetMapping permutationIgnoreCast( List<RexNode> nodes, AlgDataType inputRowType ) {
        final TargetMapping mapping =
                Mappings.create(
                        MappingType.PARTIAL_FUNCTION,
                        nodes.size(),
                        inputRowType.getFieldCount() );
        for ( Ord<RexNode> node : Ord.zip( nodes ) ) {
            if ( node.e instanceof RexIndexRef ) {
                mapping.set(
                        node.i,
                        ((RexIndexRef) node.e).getIndex() );
            } else if ( node.e.isA( Kind.CAST ) ) {
                final RexNode operand = ((RexCall) node.e).getOperands().get( 0 );
                if ( operand instanceof RexIndexRef ) {
                    mapping.set( node.i, ((RexIndexRef) operand).getIndex() );
                }
            }
        }
        return mapping;
    }


    /**
     * Returns a permutation describing where output fields come from. In the returned map, value of {@code map.getTargetOpt(i)} is {@code n} if field {@code i} projects input
     * field {@code n}, -1 if it is an expression.
     */
    public static Mappings.TargetMapping permutation( List<RexNode> nodes, AlgDataType inputRowType ) {
        final Mappings.TargetMapping mapping =
                Mappings.create(
                        MappingType.PARTIAL_FUNCTION,
                        nodes.size(),
                        inputRowType.getFieldCount() );
        for ( Ord<RexNode> node : Ord.zip( nodes ) ) {
            if ( node.e instanceof RexIndexRef ) {
                mapping.set( node.i, ((RexIndexRef) node.e).getIndex() );
            }
        }
        return mapping;
    }


    @Deprecated // to be removed before 2.0
    public static AlgNode createExistsPlan( AlgCluster cluster, AlgNode seekRel, List<RexNode> conditions, RexLiteral extraExpr, String extraName ) {
        assert extraExpr == null || extraName != null;
        AlgNode ret = seekRel;

        if ( (conditions != null) && (!conditions.isEmpty()) ) {
            RexNode conditionExp = RexUtil.composeConjunction( cluster.getRexBuilder(), conditions, true );

            final FilterFactory factory = AlgFactories.DEFAULT_FILTER_FACTORY;
            ret = factory.createFilter( ret, conditionExp );
        }

        if ( extraExpr != null ) {
            RexBuilder rexBuilder = cluster.getRexBuilder();

            assert extraExpr == rexBuilder.makeLiteral( true );

            // this should only be called for the exists case first stick an Agg on top of the sub-query
            // agg does not like no agg functions so just pretend it is doing a min(TRUE)

            final AlgBuilder algBuilder = AlgFactories.LOGICAL_BUILDER.create( cluster, null );
            ret = algBuilder.push( ret )
                    .project( ImmutableList.of( extraExpr ) )
                    .build();

            final AggregateCall aggCall =
                    AggregateCall.create(
                            OperatorRegistry.getAgg( OperatorName.MIN ),
                            false,
                            false,
                            ImmutableList.of( 0 ),
                            -1,
                            AlgCollations.EMPTY,
                            0,
                            ret,
                            null,
                            extraName );

            ret = LogicalRelAggregate.create( ret, ImmutableBitSet.of(), null, ImmutableList.of( aggCall ) );
        }

        return ret;
    }


    @Deprecated // to be removed before 2.0
    public static Exists createExistsPlan( AlgNode seekRel, SubQueryType subQueryType, Logic logic, boolean notIn ) {
        final AlgBuilder algBuilder = AlgFactories.LOGICAL_BUILDER.create( seekRel.getCluster(), null );
        return createExistsPlan( seekRel, subQueryType, logic, notIn, algBuilder );
    }


    /**
     * Creates a plan suitable for use in <code>EXISTS</code> or <code>IN</code> statements.
     *
     * @param seekAlg A query alg, for example the resulting alg from 'select * from emp' or 'values (1,2,3)' or '('Foo', 34)'.
     * @param subQueryType Sub-query type
     * @param logic Whether to use 2- or 3-valued boolean logic
     * @param notIn Whether the operator is NOT IN
     * @param algBuilder Builder for relational expressions
     * @return A pair of a relational expression which outer joins a boolean condition column, and a numeric offset.
     * The offset is 2 if column 0 is the number of rows and column 1 is the number of rows with not-null keys; 0 otherwise.
     * //@see org.polypheny.db.sql2alg.SqlToRelConverter#convertExists
     */
    public static Exists createExistsPlan( AlgNode seekAlg, SubQueryType subQueryType, Logic logic, boolean notIn, AlgBuilder algBuilder ) {
        if ( subQueryType == SubQueryType.SCALAR ) {
            return new Exists( seekAlg, false, true );
        }

        switch ( logic ) {
            case TRUE_FALSE_UNKNOWN:
            case UNKNOWN_AS_TRUE:
                if ( notIn && !containsNullableFields( seekAlg ) ) {
                    logic = Logic.TRUE_FALSE;
                }
        }
        AlgNode ret = seekAlg;
        final AlgCluster cluster = seekAlg.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        final int keyCount = ret.getTupleType().getFieldCount();
        final boolean outerJoin = notIn || logic == AlgOptUtil.Logic.TRUE_FALSE_UNKNOWN;
        if ( !outerJoin ) {
            final LogicalRelAggregate aggregate = LogicalRelAggregate.create( ret, ImmutableBitSet.range( keyCount ), null, ImmutableList.of() );
            return new Exists( aggregate, false, false );
        }

        // for IN/NOT IN, it needs to output the fields
        final List<RexNode> exprs = new ArrayList<>();
        if ( subQueryType == SubQueryType.IN ) {
            for ( int i = 0; i < keyCount; i++ ) {
                exprs.add( rexBuilder.makeInputRef( ret, i ) );
            }
        }

        final int projectedKeyCount = exprs.size();
        exprs.add( rexBuilder.makeLiteral( true ) );

        ret = algBuilder.push( ret ).project( exprs ).build();

        final AggregateCall aggCall =
                AggregateCall.create(
                        OperatorRegistry.getAgg( OperatorName.MIN ),
                        false,
                        false,
                        ImmutableList.of( projectedKeyCount ),
                        -1,
                        AlgCollations.EMPTY,
                        projectedKeyCount,
                        ret,
                        null,
                        null );

        ret = LogicalRelAggregate.create( ret, ImmutableBitSet.range( projectedKeyCount ), null, ImmutableList.of( aggCall ) );

        return switch ( logic ) {
            case TRUE_FALSE_UNKNOWN, UNKNOWN_AS_TRUE -> new Exists( ret, true, true );
            default -> new Exists( ret, false, true );
        };
    }


    @Deprecated // to be removed before 2.0
    public static AlgNode createRenameAlg( AlgDataType outputType, AlgNode alg ) {
        AlgDataType inputType = alg.getTupleType();
        List<AlgDataTypeField> inputFields = inputType.getFields();
        int n = inputFields.size();

        List<AlgDataTypeField> outputFields = outputType.getFields();
        assert outputFields.size() == n
                : "rename: field count mismatch: in=" + inputType + ", out" + outputType;

        final List<Pair<RexNode, String>> renames = new ArrayList<>();
        for ( Pair<AlgDataTypeField, AlgDataTypeField> pair : Pair.zip( inputFields, outputFields ) ) {
            final AlgDataTypeField inputField = pair.left;
            final AlgDataTypeField outputField = pair.right;
            assert inputField.getType().equals( outputField.getType() );
            final RexBuilder rexBuilder = alg.getCluster().getRexBuilder();
            renames.add(
                    Pair.of(
                            rexBuilder.makeInputRef( inputField.getType(), inputField.getIndex() ),
                            outputField.getName() ) );
        }
        final AlgBuilder algBuilder = AlgFactories.LOGICAL_BUILDER.create( alg.getCluster(), null );
        return algBuilder.push( alg )
                .project( Pair.left( renames ), Pair.right( renames ), true )
                .build();
    }


    @Deprecated // to be removed before 2.0
    public static AlgNode createFilter( AlgNode child, RexNode condition ) {
        final AlgFactories.FilterFactory factory = AlgFactories.DEFAULT_FILTER_FACTORY;
        return factory.createFilter( child, condition );
    }


    @Deprecated // to be removed before 2.0
    public static AlgNode createFilter( AlgNode child, RexNode condition, AlgFactories.FilterFactory filterFactory ) {
        return filterFactory.createFilter( child, condition );
    }


    /**
     * Creates a filter, using the default filter factory, or returns the original relational expression if the condition is trivial.
     */
    public static AlgNode createFilter( AlgNode child, Iterable<? extends RexNode> conditions ) {
        return createFilter( child, conditions, AlgFactories.DEFAULT_FILTER_FACTORY );
    }


    /**
     * Creates a filter using the default factory, or returns the original relational expression if the condition is trivial.
     */
    public static AlgNode createFilter( AlgNode child, Iterable<? extends RexNode> conditions, AlgFactories.FilterFactory filterFactory ) {
        final AlgCluster cluster = child.getCluster();
        final RexNode condition = RexUtil.composeConjunction( cluster.getRexBuilder(), conditions, true );
        if ( condition == null ) {
            return child;
        } else {
            return filterFactory.createFilter( child, condition );
        }
    }


    @Deprecated // to be removed before 2.0
    public static AlgNode createNullFilter( AlgNode alg, Integer[] fieldOrdinals ) {
        RexNode condition = null;
        final RexBuilder rexBuilder = alg.getCluster().getRexBuilder();
        AlgDataType rowType = alg.getTupleType();
        int n;
        if ( fieldOrdinals != null ) {
            n = fieldOrdinals.length;
        } else {
            n = rowType.getFieldCount();
        }
        List<AlgDataTypeField> fields = rowType.getFields();
        for ( int i = 0; i < n; ++i ) {
            int iField;
            if ( fieldOrdinals != null ) {
                iField = fieldOrdinals[i];
            } else {
                iField = i;
            }
            AlgDataType type = fields.get( iField ).getType();
            if ( !type.isNullable() ) {
                continue;
            }
            RexNode newCondition =
                    rexBuilder.makeCall(
                            OperatorRegistry.get( OperatorName.IS_NOT_NULL ),
                            rexBuilder.makeInputRef( type, iField ) );
            if ( condition == null ) {
                condition = newCondition;
            } else {
                condition =
                        rexBuilder.makeCall(
                                OperatorRegistry.get( OperatorName.AND ),
                                condition,
                                newCondition );
            }
        }
        if ( condition == null ) {
            // no filtering required
            return alg;
        }

        final AlgFactories.FilterFactory factory = AlgFactories.DEFAULT_FILTER_FACTORY;
        return factory.createFilter( alg, condition );
    }


    /**
     * Creates a projection which casts a rel's output to a desired row type.
     *
     * @param alg producer of rows to be converted
     * @param castRowType row type after cast
     * @param rename if true, use field names from castRowType; if false, preserve field names from rel
     * @return conversion rel
     */
    public static AlgNode createCastAlg( final AlgNode alg, AlgDataType castRowType, boolean rename ) {
        return createCastAlg( alg, castRowType, rename, AlgFactories.DEFAULT_PROJECT_FACTORY );
    }


    /**
     * Creates a projection which casts a rel's output to a desired row type.
     *
     * @param alg producer of rows to be converted
     * @param castRowType row type after cast
     * @param rename if true, use field names from castRowType; if false, preserve field names from rel
     * @param projectFactory Project Factory
     * @return conversion rel
     */
    public static AlgNode createCastAlg( final AlgNode alg, AlgDataType castRowType, boolean rename, AlgFactories.ProjectFactory projectFactory ) {
        assert projectFactory != null;
        AlgDataType rowType = alg.getTupleType();
        if ( areRowTypesEqual( rowType, castRowType, rename ) ) {
            // nothing to do
            return alg;
        }
        final RexBuilder rexBuilder = alg.getCluster().getRexBuilder();
        final List<RexNode> castExps = RexUtil.generateCastExpressions( rexBuilder, castRowType, rowType );
        if ( rename ) {
            // Use names and types from castRowType.
            return projectFactory.createProject( alg, castExps, castRowType.getFieldNames() );
        } else {
            // Use names from rowType, types from castRowType.
            return projectFactory.createProject( alg, castExps, rowType.getFieldNames() );
        }
    }


    /**
     * Creates a LogicalAggregate that removes all duplicates from the result of an underlying relational expression.
     *
     * @param alg underlying rel
     * @return alg implementing SingleValueAgg
     */
    public static AlgNode createSingleValueAggAlg( AlgCluster cluster, AlgNode alg ) {
        final int aggCallCnt = alg.getTupleType().getFieldCount();
        final List<AggregateCall> aggCalls = new ArrayList<>();

        for ( int i = 0; i < aggCallCnt; i++ ) {
            aggCalls.add(
                    AggregateCall.create(
                            OperatorRegistry.getAgg( OperatorName.SINGLE_VALUE ),
                            false,
                            false,
                            ImmutableList.of( i ),
                            -1,
                            AlgCollations.EMPTY,
                            0,
                            alg,
                            null,
                            null ) );
        }

        return LogicalRelAggregate.create( alg, ImmutableBitSet.of(), null, aggCalls );
    }


    /**
     * Splits out the equi-join components of a join condition, and returns what's left. For example, given the condition
     *
     * <blockquote><code>L.A = R.X AND L.B = L.C AND (L.D = 5 OR L.E = R.Y)</code></blockquote>
     *
     * returns
     *
     * <ul>
     * <li>leftKeys = {A}</li>
     * <li>rightKeys = {X}</li>
     * <li>rest = L.B = L.C AND (L.D = 5 OR L.E = R.Y)</li>
     * </ul>
     *
     * @param left left input to join
     * @param right right input to join
     * @param condition join condition
     * @param leftKeys The ordinals of the fields from the left input which are equi-join keys
     * @param rightKeys The ordinals of the fields from the right input which are equi-join keys
     * @param filterNulls List of boolean values for each join key position indicating whether the operator filters out nulls or not. Value is true if the operator is EQUALS and false if the operator is IS NOT DISTINCT FROM (or an expanded version). If <code>filterNulls</code> is null, only join conditions with EQUALS operators are considered equi-join components. Rest (including IS NOT DISTINCT FROM) are returned in remaining join condition.
     * @return remaining join filters that are not equijoins; may return a {@link RexLiteral} true, but never null
     */
    public static @Nonnull
    RexNode splitJoinCondition( AlgNode left, AlgNode right, RexNode condition, List<Integer> leftKeys, List<Integer> rightKeys, List<Boolean> filterNulls ) {
        final List<RexNode> nonEquiList = new ArrayList<>();

        splitJoinCondition(
                left.getCluster().getRexBuilder(),
                left.getTupleType().getFieldCount(),
                condition,
                leftKeys,
                rightKeys,
                filterNulls,
                nonEquiList );

        return RexUtil.composeConjunction( left.getCluster().getRexBuilder(), nonEquiList );
    }


    /**
     * Splits out the equi-join (and optionally, a single non-equi) components of a join condition, and returns what's left. Projection might be required by the caller to provide join
     * keys that are not direct field references.
     *
     * @param sysFieldList list of system fields
     * @param leftRel left join input
     * @param rightRel right join input
     * @param condition join condition
     * @param leftJoinKeys The join keys from the left input which are equi-join keys
     * @param rightJoinKeys The join keys from the right input which are equi-join keys
     * @param filterNulls The join key positions for which null values will not match. null values only match for the "is not distinct from" condition.
     * @param rangeOp if null, only locate equi-joins; otherwise, locate a single non-equi join predicate and return its operator in this list; join keys associated with the non-equi join predicate are at the end of the key lists returned
     * @return What's left, never null
     */
    public static RexNode splitJoinCondition( List<AlgDataTypeField> sysFieldList, AlgNode leftRel, AlgNode rightRel, RexNode condition, List<RexNode> leftJoinKeys, List<RexNode> rightJoinKeys, List<Integer> filterNulls, List<Operator> rangeOp ) {
        return splitJoinCondition( sysFieldList, ImmutableList.of( leftRel, rightRel ), condition, ImmutableList.of( leftJoinKeys, rightJoinKeys ), filterNulls, rangeOp );
    }


    /**
     * Splits out the equi-join (and optionally, a single non-equi) components of a join condition, and returns what's left. Projection might be required by the caller to provide join keys that
     * are not direct field references.
     *
     * @param sysFieldList list of system fields
     * @param inputs join inputs
     * @param condition join condition
     * @param joinKeys The join keys from the inputs which are equi-join keys
     * @param filterNulls The join key positions for which null values will not match. null values only match for the "is not distinct from" condition.
     * @param rangeOp if null, only locate equi-joins; otherwise, locate a single non-equi join predicate and return its operator in this list; join keys associated with the non-equi join predicate are at the end of the key lists returned
     * @return What's left, never null
     */
    public static @Nonnull
    RexNode splitJoinCondition( List<AlgDataTypeField> sysFieldList, List<AlgNode> inputs, RexNode condition, List<List<RexNode>> joinKeys, List<Integer> filterNulls, List<Operator> rangeOp ) {
        final List<RexNode> nonEquiList = new ArrayList<>();
        splitJoinCondition( sysFieldList, inputs, condition, joinKeys, filterNulls, rangeOp, nonEquiList );
        // Convert the remainders into a list that are AND'ed together.
        return RexUtil.composeConjunction( inputs.get( 0 ).getCluster().getRexBuilder(), nonEquiList );
    }


    public static RexNode splitCorrelatedFilterCondition( LogicalRelFilter filter, List<RexNode> joinKeys, List<RexNode> correlatedJoinKeys, boolean extractCorrelatedFieldAccess ) {
        final List<RexNode> nonEquiList = new ArrayList<>();
        splitCorrelatedFilterCondition( filter, filter.getCondition(), joinKeys, correlatedJoinKeys, nonEquiList, extractCorrelatedFieldAccess );
        // Convert the remainders into a list that are AND'ed together.
        return RexUtil.composeConjunction( filter.getCluster().getRexBuilder(), nonEquiList, true );
    }


    private static void splitJoinCondition( List<AlgDataTypeField> sysFieldList, List<AlgNode> inputs, RexNode condition, List<List<RexNode>> joinKeys, List<Integer> filterNulls, List<Operator> rangeOp, List<RexNode> nonEquiList ) {
        final int sysFieldCount = sysFieldList.size();
        final AlgCluster cluster = inputs.get( 0 ).getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        final AlgDataTypeFactory typeFactory = cluster.getTypeFactory();

        final ImmutableBitSet[] inputsRange = new ImmutableBitSet[inputs.size()];
        int totalFieldCount = 0;
        for ( int i = 0; i < inputs.size(); i++ ) {
            final int firstField = totalFieldCount + sysFieldCount;
            totalFieldCount = firstField + inputs.get( i ).getTupleType().getFieldCount();
            inputsRange[i] = ImmutableBitSet.range( firstField, totalFieldCount );
        }

        // adjustment array
        int[] adjustments = new int[totalFieldCount];
        for ( int i = 0; i < inputs.size(); i++ ) {
            final int adjustment = inputsRange[i].nextSetBit( 0 );
            for ( int j = adjustment; j < inputsRange[i].length(); j++ ) {
                adjustments[j] = -adjustment;
            }
        }

        if ( condition instanceof RexCall ) {
            RexCall call = (RexCall) condition;
            if ( call.getKind() == Kind.AND ) {
                for ( RexNode operand : call.getOperands() ) {
                    splitJoinCondition( sysFieldList, inputs, operand, joinKeys, filterNulls, rangeOp, nonEquiList );
                }
                return;
            }

            RexNode leftKey = null;
            RexNode rightKey = null;
            int leftInput = 0;
            int rightInput = 0;
            List<AlgDataTypeField> leftFields = null;
            List<AlgDataTypeField> rightFields = null;
            boolean reverse = false;

            call = collapseExpandedIsNotDistinctFromExpr( call, rexBuilder );
            Kind kind = call.getKind();

            // Only consider range operators if we haven't already seen one
            if ( (kind == Kind.EQUALS)
                    || (filterNulls != null
                    && kind == Kind.IS_NOT_DISTINCT_FROM)
                    || (rangeOp != null
                    && rangeOp.isEmpty()
                    && (kind == Kind.GREATER_THAN
                    || kind == Kind.GREATER_THAN_OR_EQUAL
                    || kind == Kind.LESS_THAN
                    || kind == Kind.LESS_THAN_OR_EQUAL)) ) {
                final List<RexNode> operands = call.getOperands();
                RexNode op0 = operands.get( 0 );
                RexNode op1 = operands.get( 1 );

                final ImmutableBitSet projRefs0 = InputFinder.bits( op0 );
                final ImmutableBitSet projRefs1 = InputFinder.bits( op1 );

                boolean foundBothInputs = false;
                for ( int i = 0; i < inputs.size() && !foundBothInputs; i++ ) {
                    if ( projRefs0.intersects( inputsRange[i] ) && projRefs0.union( inputsRange[i] ).equals( inputsRange[i] ) ) {
                        if ( leftKey == null ) {
                            leftKey = op0;
                            leftInput = i;
                            leftFields = inputs.get( leftInput ).getTupleType().getFields();
                        } else {
                            rightKey = op0;
                            rightInput = i;
                            rightFields = inputs.get( rightInput ).getTupleType().getFields();
                            reverse = true;
                            foundBothInputs = true;
                        }
                    } else if ( projRefs1.intersects( inputsRange[i] )
                            && projRefs1.union( inputsRange[i] ).equals( inputsRange[i] ) ) {
                        if ( leftKey == null ) {
                            leftKey = op1;
                            leftInput = i;
                            leftFields = inputs.get( leftInput ).getTupleType().getFields();
                        } else {
                            rightKey = op1;
                            rightInput = i;
                            rightFields = inputs.get( rightInput ).getTupleType().getFields();
                            foundBothInputs = true;
                        }
                    }
                }

                if ( (leftKey != null) && (rightKey != null) ) {
                    // replace right Key input ref
                    rightKey = rightKey.accept( new AlgOptUtil.RexInputConverter( rexBuilder, rightFields, rightFields, adjustments ) );

                    // left key only needs to be adjusted if there are system fields, but do it for uniformity
                    leftKey = leftKey.accept( new AlgOptUtil.RexInputConverter( rexBuilder, leftFields, leftFields, adjustments ) );

                    AlgDataType leftKeyType = leftKey.getType();
                    AlgDataType rightKeyType = rightKey.getType();

                    if ( leftKeyType != rightKeyType ) {
                        // perform casting
                        AlgDataType targetKeyType = typeFactory.leastRestrictive( ImmutableList.of( leftKeyType, rightKeyType ) );

                        if ( targetKeyType == null ) {
                            throw new AssertionError( "Cannot find common type for join keys " + leftKey + " (type " + leftKeyType + ") and " + rightKey + " (type " + rightKeyType + ")" );
                        }

                        if ( leftKeyType != targetKeyType ) {
                            leftKey = rexBuilder.makeCast( targetKeyType, leftKey );
                        }

                        if ( rightKeyType != targetKeyType ) {
                            rightKey = rexBuilder.makeCast( targetKeyType, rightKey );
                        }
                    }
                }
            }

            if ( (rangeOp == null) && ((leftKey == null) || (rightKey == null)) ) {
                // no equality join keys found yet: try transforming the condition to equality "join" conditions, e.g.
                //     f(LHS) > 0 ===> ( f(LHS) > 0 ) = TRUE,
                // and make the RHS produce TRUE, but only if we're strictly looking for equi-joins
                final ImmutableBitSet projRefs = InputFinder.bits( condition );
                leftKey = null;
                rightKey = null;

                boolean foundInput = false;
                for ( int i = 0; i < inputs.size() && !foundInput; i++ ) {
                    if ( inputsRange[i].contains( projRefs ) ) {
                        leftInput = i;
                        leftFields = inputs.get( leftInput ).getTupleType().getFields();

                        leftKey = condition.accept( new AlgOptUtil.RexInputConverter( rexBuilder, leftFields, leftFields, adjustments ) );

                        rightKey = rexBuilder.makeLiteral( true );

                        // effectively performing an equality comparison
                        kind = Kind.EQUALS;

                        foundInput = true;
                    }
                }
            }

            if ( (leftKey != null) && (rightKey != null) ) {
                // found suitable join keys add them to key list, ensuring that if there is a non-equi join predicate, it appears at the end of the key list; also mark the null filtering property
                addJoinKey(
                        joinKeys.get( leftInput ),
                        leftKey,
                        (rangeOp != null) && !rangeOp.isEmpty() );
                addJoinKey(
                        joinKeys.get( rightInput ),
                        rightKey,
                        (rangeOp != null) && !rangeOp.isEmpty() );
                if ( filterNulls != null && kind == Kind.EQUALS ) {
                    // nulls are considered not matching for equality comparison add the position of the most recently inserted key
                    filterNulls.add( joinKeys.get( leftInput ).size() - 1 );
                }
                if ( rangeOp != null && kind != Kind.EQUALS && kind != Kind.IS_DISTINCT_FROM ) {
                    if ( reverse ) {
                        kind = kind.reverse();
                    }
                    rangeOp.add( op( kind, call.getOperator() ) );
                }
                return;
            } // else fall through and add this condition as nonEqui condition
        }

        // The operator is not of RexCall type. So we fail. Fall through. Add this condition to the list of non-equi-join conditions.
        nonEquiList.add( condition );
    }


    /**
     * Builds an equi-join condition from a set of left and right keys.
     */
    public static @Nonnull
    RexNode createEquiJoinCondition( final AlgNode left, final List<Integer> leftKeys, final AlgNode right, final List<Integer> rightKeys, final RexBuilder rexBuilder ) {
        final List<AlgDataType> leftTypes = AlgOptUtil.getFieldTypeList( left.getTupleType() );
        final List<AlgDataType> rightTypes = AlgOptUtil.getFieldTypeList( right.getTupleType() );
        return RexUtil.composeConjunction(
                rexBuilder,
                new AbstractList<RexNode>() {
                    @Override
                    public RexNode get( int index ) {
                        final int leftKey = leftKeys.get( index );
                        final int rightKey = rightKeys.get( index );
                        return rexBuilder.makeCall(
                                OperatorRegistry.get( OperatorName.EQUALS ),
                                rexBuilder.makeInputRef( leftTypes.get( leftKey ), leftKey ),
                                rexBuilder.makeInputRef( rightTypes.get( rightKey ), leftTypes.size() + rightKey ) );
                    }


                    @Override
                    public int size() {
                        return leftKeys.size();
                    }
                } );
    }


    public static Operator op( Kind kind, Operator operator ) {
        switch ( kind ) {
            case EQUALS:
                return OperatorRegistry.get( OperatorName.EQUALS );
            case NOT_EQUALS:
                return OperatorRegistry.get( OperatorName.NOT_EQUALS );
            case GREATER_THAN:
                return OperatorRegistry.get( OperatorName.GREATER_THAN );
            case GREATER_THAN_OR_EQUAL:
                return OperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL );
            case LESS_THAN:
                return OperatorRegistry.get( OperatorName.LESS_THAN );
            case LESS_THAN_OR_EQUAL:
                return OperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL );
            case IS_DISTINCT_FROM:
                return OperatorRegistry.get( OperatorName.IS_DISTINCT_FROM );
            case IS_NOT_DISTINCT_FROM:
                return OperatorRegistry.get( OperatorName.IS_NOT_DISTINCT_FROM );
            default:
                return operator;
        }
    }


    private static void addJoinKey( List<RexNode> joinKeyList, RexNode key, boolean preserveLastElementInList ) {
        if ( !joinKeyList.isEmpty() && preserveLastElementInList ) {
            joinKeyList.add( joinKeyList.size() - 1, key );
        } else {
            joinKeyList.add( key );
        }
    }


    private static void splitCorrelatedFilterCondition( LogicalRelFilter filter, RexNode condition, List<RexIndexRef> joinKeys, List<RexNode> correlatedJoinKeys, List<RexNode> nonEquiList ) {
        if ( condition instanceof RexCall ) {
            RexCall call = (RexCall) condition;
            if ( call.getOperator().getKind() == Kind.AND ) {
                for ( RexNode operand : call.getOperands() ) {
                    splitCorrelatedFilterCondition( filter, operand, joinKeys, correlatedJoinKeys, nonEquiList );
                }
                return;
            }

            if ( call.getOperator().getKind() == Kind.EQUALS ) {
                final List<RexNode> operands = call.getOperands();
                RexNode op0 = operands.get( 0 );
                RexNode op1 = operands.get( 1 );

                if ( !(RexUtil.containsInputRef( op0 )) && (op1 instanceof RexIndexRef) ) {
                    correlatedJoinKeys.add( op0 );
                    joinKeys.add( (RexIndexRef) op1 );
                    return;
                } else if ( (op0 instanceof RexIndexRef) && !(RexUtil.containsInputRef( op1 )) ) {
                    joinKeys.add( (RexIndexRef) op0 );
                    correlatedJoinKeys.add( op1 );
                    return;
                }
            }
        }

        // The operator is not of RexCall type. So we fail. Fall through. Add this condition to the list of non-equi-join conditions.
        nonEquiList.add( condition );
    }


    private static void splitCorrelatedFilterCondition( LogicalRelFilter filter, RexNode condition, List<RexNode> joinKeys, List<RexNode> correlatedJoinKeys, List<RexNode> nonEquiList, boolean extractCorrelatedFieldAccess ) {
        if ( condition instanceof RexCall ) {
            RexCall call = (RexCall) condition;
            if ( call.getOperator().getKind() == Kind.AND ) {
                for ( RexNode operand : call.getOperands() ) {
                    splitCorrelatedFilterCondition( filter, operand, joinKeys, correlatedJoinKeys, nonEquiList, extractCorrelatedFieldAccess );
                }
                return;
            }

            if ( call.getOperator().getKind() == Kind.EQUALS ) {
                final List<RexNode> operands = call.getOperands();
                RexNode op0 = operands.get( 0 );
                RexNode op1 = operands.get( 1 );

                if ( extractCorrelatedFieldAccess ) {
                    if ( !RexUtil.containsFieldAccess( op0 ) && (op1 instanceof RexFieldAccess) ) {
                        joinKeys.add( op0 );
                        correlatedJoinKeys.add( op1 );
                        return;
                    } else if ( (op0 instanceof RexFieldAccess) && !RexUtil.containsFieldAccess( op1 ) ) {
                        correlatedJoinKeys.add( op0 );
                        joinKeys.add( op1 );
                        return;
                    }
                } else {
                    if ( !(RexUtil.containsInputRef( op0 )) && (op1 instanceof RexIndexRef) ) {
                        correlatedJoinKeys.add( op0 );
                        joinKeys.add( op1 );
                        return;
                    } else if ( (op0 instanceof RexIndexRef) && !(RexUtil.containsInputRef( op1 )) ) {
                        joinKeys.add( op0 );
                        correlatedJoinKeys.add( op1 );
                        return;
                    }
                }
            }
        }

        // The operator is not of RexCall type. So we fail. Fall through. Add this condition to the list of non-equi-join conditions.
        nonEquiList.add( condition );
    }


    private static void splitJoinCondition( final RexBuilder rexBuilder, final int leftFieldCount, RexNode condition, List<Integer> leftKeys, List<Integer> rightKeys, List<Boolean> filterNulls, List<RexNode> nonEquiList ) {
        if ( condition instanceof RexCall ) {
            RexCall call = (RexCall) condition;
            Kind kind = call.getKind();
            if ( kind == Kind.AND ) {
                for ( RexNode operand : call.getOperands() ) {
                    splitJoinCondition( rexBuilder, leftFieldCount, operand, leftKeys, rightKeys, filterNulls, nonEquiList );
                }
                return;
            }

            if ( filterNulls != null ) {
                call = collapseExpandedIsNotDistinctFromExpr( call, rexBuilder );
                kind = call.getKind();
            }

            // "=" and "IS NOT DISTINCT FROM" are the same except for how they treat nulls.
            if ( kind == Kind.EQUALS || (filterNulls != null && kind == Kind.IS_NOT_DISTINCT_FROM) ) {
                final List<RexNode> operands = call.getOperands();
                if ( (operands.get( 0 ) instanceof RexIndexRef) && (operands.get( 1 ) instanceof RexIndexRef) ) {
                    RexIndexRef op0 = (RexIndexRef) operands.get( 0 );
                    RexIndexRef op1 = (RexIndexRef) operands.get( 1 );

                    RexIndexRef leftField;
                    RexIndexRef rightField;
                    if ( (op0.getIndex() < leftFieldCount) && (op1.getIndex() >= leftFieldCount) ) {
                        // Arguments were of form 'op0 = op1'
                        leftField = op0;
                        rightField = op1;
                    } else if ( (op1.getIndex() < leftFieldCount) && (op0.getIndex() >= leftFieldCount) ) {
                        // Arguments were of form 'op1 = op0'
                        leftField = op1;
                        rightField = op0;
                    } else {
                        nonEquiList.add( condition );
                        return;
                    }

                    leftKeys.add( leftField.getIndex() );
                    rightKeys.add( rightField.getIndex() - leftFieldCount );
                    if ( filterNulls != null ) {
                        filterNulls.add( kind == Kind.EQUALS );
                    }
                    return;
                }
                // Arguments were not field references, one from each side, so we fail. Fall through.
            }
        }

        // Add this condition to the list of non-equi-join conditions.
        if ( !condition.isAlwaysTrue() ) {
            nonEquiList.add( condition );
        }
    }


    /**
     * Helper method for {@link #splitJoinCondition(RexBuilder, int, RexNode, List, List, List, List)} and {@link #splitJoinCondition(List, List, RexNode, List, List, List, List)}.
     *
     * If the given expr <code>call</code> is an expanded version of IS NOT DISTINCT FROM function call, collapse it and return a IS NOT DISTINCT FROM function call.
     *
     * For example: {@code t1.key IS NOT DISTINCT FROM t2.key} can rewritten in expanded form as {@code t1.key = t2.key OR (t1.key IS NULL AND t2.key IS NULL)}.
     *
     * @param call Function expression to try collapsing.
     * @param rexBuilder {@link RexBuilder} instance to create new {@link RexCall} instances.
     * @return If the given function is an expanded IS NOT DISTINCT FROM function call, return a IS NOT DISTINCT FROM function call. Otherwise return the input function call as it is.
     */
    private static RexCall collapseExpandedIsNotDistinctFromExpr( final RexCall call, final RexBuilder rexBuilder ) {
        if ( call.getKind() != Kind.OR || call.getOperands().size() != 2 ) {
            return call;
        }

        final RexNode op0 = call.getOperands().get( 0 );
        final RexNode op1 = call.getOperands().get( 1 );

        if ( !(op0 instanceof RexCall) || !(op1 instanceof RexCall) ) {
            return call;
        }

        RexCall opEqCall = (RexCall) op0;
        RexCall opNullEqCall = (RexCall) op1;

        if ( opEqCall.getKind() == Kind.AND && opNullEqCall.getKind() == Kind.EQUALS ) {
            RexCall temp = opEqCall;
            opEqCall = opNullEqCall;
            opNullEqCall = temp;
        }

        if ( opNullEqCall.getKind() != Kind.AND
                || opNullEqCall.getOperands().size() != 2
                || opEqCall.getKind() != Kind.EQUALS ) {
            return call;
        }

        final RexNode op10 = opNullEqCall.getOperands().get( 0 );
        final RexNode op11 = opNullEqCall.getOperands().get( 1 );
        if ( op10.getKind() != Kind.IS_NULL || op11.getKind() != Kind.IS_NULL ) {
            return call;
        }
        final RexNode isNullInput0 = ((RexCall) op10).getOperands().get( 0 );
        final RexNode isNullInput1 = ((RexCall) op11).getOperands().get( 0 );

        final String isNullInput0Digest = isNullInput0.toString();
        final String isNullInput1Digest = isNullInput1.toString();
        final String equalsInput0Digest = opEqCall.getOperands().get( 0 ).toString();
        final String equalsInput1Digest = opEqCall.getOperands().get( 1 ).toString();

        if ( (isNullInput0Digest.equals( equalsInput0Digest )
                && isNullInput1Digest.equals( equalsInput1Digest ))
                || (isNullInput1Digest.equals( equalsInput0Digest )
                && isNullInput0Digest.equals( equalsInput1Digest )) ) {
            return (RexCall) rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NOT_DISTINCT_FROM ), ImmutableList.of( isNullInput0, isNullInput1 ) );
        }

        return call;
    }


    @Deprecated // to be removed before 2.0
    public static void projectJoinInputs( AlgNode[] inputAlgs, List<RexNode> leftJoinKeys, List<RexNode> rightJoinKeys, int systemColCount, List<Integer> leftKeys, List<Integer> rightKeys, List<Integer> outputProj ) {
        AlgNode leftRel = inputAlgs[0];
        AlgNode rightRel = inputAlgs[1];
        final AlgCluster cluster = leftRel.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        final AlgDataTypeSystem typeSystem = cluster.getTypeFactory().getTypeSystem();

        int origLeftInputSize = leftRel.getTupleType().getFieldCount();
        int origRightInputSize = rightRel.getTupleType().getFieldCount();

        final List<RexNode> newLeftFields = new ArrayList<>();
        final List<String> newLeftFieldNames = new ArrayList<>();

        final List<RexNode> newRightFields = new ArrayList<>();
        final List<String> newRightFieldNames = new ArrayList<>();
        int leftKeyCount = leftJoinKeys.size();
        int rightKeyCount = rightJoinKeys.size();
        int i;

        for ( i = 0; i < systemColCount; i++ ) {
            outputProj.add( i );
        }

        for ( i = 0; i < origLeftInputSize; i++ ) {
            final AlgDataTypeField field = leftRel.getTupleType().getFields().get( i );
            newLeftFields.add( rexBuilder.makeInputRef( field.getType(), i ) );
            newLeftFieldNames.add( field.getName() );
            outputProj.add( systemColCount + i );
        }

        int newLeftKeyCount = 0;
        for ( i = 0; i < leftKeyCount; i++ ) {
            RexNode leftKey = leftJoinKeys.get( i );

            if ( leftKey instanceof RexIndexRef ) {
                // already added to the projected left fields only need to remember the index in the join key list
                leftKeys.add( ((RexIndexRef) leftKey).getIndex() );
            } else {
                newLeftFields.add( leftKey );
                newLeftFieldNames.add( null );
                leftKeys.add( origLeftInputSize + newLeftKeyCount );
                newLeftKeyCount++;
            }
        }

        int leftFieldCount = origLeftInputSize + newLeftKeyCount;
        for ( i = 0; i < origRightInputSize; i++ ) {
            final AlgDataTypeField field = rightRel.getTupleType().getFields().get( i );
            newRightFields.add( rexBuilder.makeInputRef( field.getType(), i ) );
            newRightFieldNames.add( field.getName() );
            outputProj.add( systemColCount + leftFieldCount + i );
        }

        int newRightKeyCount = 0;
        for ( i = 0; i < rightKeyCount; i++ ) {
            RexNode rightKey = rightJoinKeys.get( i );

            if ( rightKey instanceof RexIndexRef ) {
                // already added to the projected left fields only need to remember the index in the join key list
                rightKeys.add( ((RexIndexRef) rightKey).getIndex() );
            } else {
                newRightFields.add( rightKey );
                newRightFieldNames.add( null );
                rightKeys.add( origRightInputSize + newRightKeyCount );
                newRightKeyCount++;
            }
        }

        final AlgBuilder algBuilder = AlgFactories.LOGICAL_BUILDER.create( cluster, null );

        // added project if need to produce new keys than the original input fields
        if ( newLeftKeyCount > 0 ) {
            leftRel = algBuilder.push( leftRel )
                    .project( newLeftFields, newLeftFieldNames, true )
                    .build();
        }

        if ( newRightKeyCount > 0 ) {
            rightRel = algBuilder.push( rightRel )
                    .project( newRightFields, newRightFieldNames )
                    .build();
        }

        inputAlgs[0] = leftRel;
        inputAlgs[1] = rightRel;
    }


    @Deprecated // to be removed before 2.0
    public static AlgNode createProjectJoinRel( List<Integer> outputProj, AlgNode joinRel ) {
        int newProjectOutputSize = outputProj.size();
        List<AlgDataTypeField> joinOutputFields = joinRel.getTupleType().getFields();

        // If no projection was passed in, or the number of desired projection columns is the same as the number of columns returned from the join, then no need to create a projection
        if ( (newProjectOutputSize > 0) && (newProjectOutputSize < joinOutputFields.size()) ) {
            final List<Pair<RexNode, String>> newProjects = new ArrayList<>();
            final AlgBuilder algBuilder = AlgFactories.LOGICAL_BUILDER.create( joinRel.getCluster(), null );
            final RexBuilder rexBuilder = algBuilder.getRexBuilder();
            for ( int fieldIndex : outputProj ) {
                final AlgDataTypeField field = joinOutputFields.get( fieldIndex );
                newProjects.add(
                        Pair.of(
                                rexBuilder.makeInputRef( field.getType(), fieldIndex ),
                                field.getName() ) );
            }

            // Create a project alg on the output of the join.
            return algBuilder.push( joinRel )
                    .project( Pair.left( newProjects ), Pair.right( newProjects ), true )
                    .build();
        }

        return joinRel;
    }


    public static void registerAbstractAlgs( AlgPlanner planner ) {
        planner.addRule( AggregateProjectPullUpConstantsRule.INSTANCE2 );
        planner.addRule( UnionPullUpConstantsRule.INSTANCE );
        planner.addRule( PruneEmptyRules.UNION_INSTANCE );
        planner.addRule( PruneEmptyRules.INTERSECT_INSTANCE );
        planner.addRule( PruneEmptyRules.MINUS_INSTANCE );
        planner.addRule( PruneEmptyRules.PROJECT_INSTANCE );
        planner.addRule( PruneEmptyRules.FILTER_INSTANCE );
        planner.addRule( PruneEmptyRules.SORT_INSTANCE );
        planner.addRule( PruneEmptyRules.AGGREGATE_INSTANCE );
        planner.addRule( PruneEmptyRules.JOIN_LEFT_INSTANCE );
        planner.addRule( PruneEmptyRules.JOIN_RIGHT_INSTANCE );
        planner.addRule( PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE );
        planner.addRule( UnionMergeRule.INSTANCE );
        planner.addRule( UnionMergeRule.INTERSECT_INSTANCE );
        planner.addRule( UnionMergeRule.MINUS_INSTANCE );
        planner.addRule( ProjectToWindowRules.PROJECT );
        planner.addRule( FilterMergeRule.INSTANCE );
        planner.addRule( DateRangeRules.FILTER_INSTANCE );
        planner.addRule( IntersectToDistinctRule.INSTANCE );
        planner.addRule( MergeDocumentFilterRule.INSTANCE );
    }


    /**
     * Dumps a plan as a string.
     *
     * @param header Header to print before the plan. Ignored if the format is XML
     * @param alg Algebra expression to explain
     * @param format Output format
     * @param detailLevel Detail level
     * @return Plan
     */
    public static String dumpPlan( String header, AlgNode alg, ExplainFormat format, ExplainLevel detailLevel ) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter( sw );
        if ( !header.equals( "" ) ) {
            pw.println( header );
        }
        AlgWriter planWriter;
        switch ( format ) {
            case XML:
                planWriter = new AlgXmlWriter( pw, detailLevel );
                break;
            case JSON:
                planWriter = new AlgJsonWriter();
                alg.explain( planWriter );
                return ((AlgJsonWriter) planWriter).asString();
            default:
                planWriter = new AlgWriterImpl( pw, detailLevel, false );
        }
        alg.explain( planWriter );
        pw.flush();
        return sw.toString();
    }


    @Deprecated // to be removed before 2.0
    public static String dumpPlan( String header, AlgNode alg, boolean asXml, ExplainLevel detailLevel ) {
        return dumpPlan( header, alg, asXml ? ExplainFormat.XML : ExplainFormat.TEXT, detailLevel );
    }


    /**
     * Creates the row type descriptor for the result of a DML operation, which is a single column named ROWCOUNT of type BIGINT for INSERT; a single column named PLAN for EXPLAIN.
     *
     * @param kind Kind of node
     * @param typeFactory factory to use for creating type descriptor
     * @return created type
     */
    public static AlgDataType createDmlRowType( Kind kind, AlgDataTypeFactory typeFactory ) {
        switch ( kind ) {
            case INSERT:
            case DELETE:
            case UPDATE:
                return typeFactory.createStructType(
                        ImmutableList.of(
                                new AlgDataTypeFieldImpl( -1L, AvaticaConnection.ROWCOUNT_COLUMN_NAME, 0,
                                        typeFactory.createPolyType( PolyType.BIGINT ) ) ) );
            case EXPLAIN:
                return typeFactory.createStructType(
                        ImmutableList.of(
                                new AlgDataTypeFieldImpl( -1L, AvaticaConnection.PLAN_COLUMN_NAME, 0,
                                        typeFactory.createPolyType( PolyType.VARCHAR, AlgDataType.PRECISION_NOT_SPECIFIED ) ) ) );
            default:
                throw Util.unexpected( kind );
        }
    }


    /**
     * Returns whether two types are equal using '='.
     *
     * @param desc1 Description of first type
     * @param type1 First type
     * @param desc2 Description of second type
     * @param type2 Second type
     * @param litmus What to do if an error is detected (types are not equal)
     * @return Whether the types are equal
     */
    public static boolean eq( final String desc1, AlgDataType type1, final String desc2, AlgDataType type2, Litmus litmus ) {
        // if any one of the types is ANY return true
        if ( type1.getPolyType() == PolyType.ANY || type2.getPolyType() == PolyType.ANY ) {
            return litmus.succeed();
        }

        if ( type1.getPolyType() == PolyType.DOCUMENT || type2.getPolyType() == PolyType.DOCUMENT ) {
            return litmus.succeed();
        }

        // Due to another issue with arrays in db.sql.SqlOperator#deriveType we cannot assume
        // that two arrays that are equal are also represented by the same type object.
        // This is why we have to handle it differently here and actually compare the properties
        // of the array types.
        // This means we are comparing the component type, cardinality, and dimension.
        if ( type1.getPolyType() == PolyType.ARRAY && type2.getPolyType() == PolyType.ARRAY ) {
            ArrayType arrayType1 = (ArrayType) type1;
            ArrayType arrayType2 = (ArrayType) type2;

            if ( arrayType1.getComponentType().equals( arrayType2.getComponentType() )
                    && arrayType1.getCardinality() == arrayType2.getCardinality()
                    && arrayType1.getDimension() == arrayType2.getDimension()
            ) {
                return litmus.succeed();
            } else {
                return litmus.fail( "array type mismatch:\n{}:\n{}\n{}:\n{}", desc1, type1.getFullTypeString(), desc2, type2.getFullTypeString() );
            }
        }

        if ( (type1.getPolyType() == PolyType.JSON || type1.getPolyType() == PolyType.VARCHAR) && (type2.getPolyType() == PolyType.VARCHAR || type2.getPolyType() == PolyType.JSON) ) {
            return litmus.succeed();
        }

        if ( !type1.equals( type2 ) ) {
            return litmus.fail( "type mismatch:\n{}:\n{}\n{}:\n{}", desc1, type1.getFullTypeString(), desc2, type2.getFullTypeString() );
        }
        return litmus.succeed();
    }


    /**
     * Returns whether two types are equal using {@link #areRowTypesEqual(AlgDataType, AlgDataType, boolean)}. Both types must not be null.
     *
     * @param desc1 Description of role of first type
     * @param type1 First type
     * @param desc2 Description of role of second type
     * @param type2 Second type
     * @param litmus Whether to assert if they are not equal
     * @return Whether the types are equal
     */
    public static boolean equal( final String desc1, AlgDataType type1, final String desc2, AlgDataType type2, Litmus litmus ) {
        if ( !areRowTypesEqual( type1, type2, false ) ) {
            return litmus.fail( "Type mismatch:\n{}:\n{}\n{}:\n{}", desc1, type1.getFullTypeString(), desc2, type2.getFullTypeString() );
        }
        return litmus.succeed();
    }


    /**
     * Returns whether two relational expressions have the same row-type.
     */
    public static boolean equalType( String desc0, AlgNode alg0, String desc1, AlgNode alg1, Litmus litmus ) {
        // TODO: change 'equal' to 'eq', which is stronger.
        return equal( desc0, alg0.getTupleType(), desc1, alg1.getTupleType(), litmus );
    }


    /**
     * Returns a translation of the <code>IS DISTINCT FROM</code> (or <code>IS NOT DISTINCT FROM</code>) sql operator.
     *
     * @param neg if false, returns a translation of IS NOT DISTINCT FROM
     */
    public static RexNode isDistinctFrom( RexBuilder rexBuilder, RexNode x, RexNode y, boolean neg ) {
        RexNode ret = null;
        if ( x.getType().isStruct() ) {
            assert y.getType().isStruct();
            List<AlgDataTypeField> xFields = x.getType().getFields();
            List<AlgDataTypeField> yFields = y.getType().getFields();
            assert xFields.size() == yFields.size();
            for ( Pair<AlgDataTypeField, AlgDataTypeField> pair : Pair.zip( xFields, yFields ) ) {
                AlgDataTypeField xField = pair.left;
                AlgDataTypeField yField = pair.right;
                RexNode newX = rexBuilder.makeFieldAccess( x, xField.getIndex() );
                RexNode newY = rexBuilder.makeFieldAccess( y, yField.getIndex() );
                RexNode newCall = isDistinctFromInternal( rexBuilder, newX, newY, neg );
                if ( ret == null ) {
                    ret = newCall;
                } else {
                    ret = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.AND ), ret, newCall );
                }
            }
        } else {
            ret = isDistinctFromInternal( rexBuilder, x, y, neg );
        }

        // The result of IS DISTINCT FROM is NOT NULL because it can only return TRUE or FALSE.
        assert ret != null;
        assert !ret.getType().isNullable();

        return ret;
    }


    private static RexNode isDistinctFromInternal( RexBuilder rexBuilder, RexNode x, RexNode y, boolean neg ) {

        if ( neg ) {
            // x is not distinct from y
            // x=y IS TRUE or ((x is null) and (y is null)),
            return rexBuilder.makeCall(
                    OperatorRegistry.get( OperatorName.OR ),
                    rexBuilder.makeCall(
                            OperatorRegistry.get( OperatorName.AND ),
                            rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NULL ), x ),
                            rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NULL ), y ) ),
                    rexBuilder.makeCall(
                            OperatorRegistry.get( OperatorName.IS_TRUE ),
                            rexBuilder.makeCall( OperatorRegistry.get( OperatorName.EQUALS ), x, y ) ) );
        } else {
            // x is distinct from y
            // x=y IS NOT TRUE and ((x is not null) or (y is not null)),
            return rexBuilder.makeCall(
                    OperatorRegistry.get( OperatorName.AND ),
                    rexBuilder.makeCall(
                            OperatorRegistry.get( OperatorName.OR ),
                            rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NOT_NULL ), x ),
                            rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NOT_NULL ), y ) ),
                    rexBuilder.makeCall(
                            OperatorRegistry.get( OperatorName.IS_NOT_TRUE ),
                            rexBuilder.makeCall( OperatorRegistry.get( OperatorName.EQUALS ), x, y ) ) );
        }
    }


    /**
     * Converts a relational expression to a string, showing just basic attributes.
     */
    public static String toString( final AlgNode alg ) {
        return toString( alg, ExplainLevel.EXPPLAN_ATTRIBUTES );
    }


    /**
     * Converts a relational expression to a string.
     */
    public static String toString( final AlgNode alg, ExplainLevel detailLevel ) {
        if ( alg == null ) {
            return null;
        }
        final StringWriter sw = new StringWriter();
        final AlgWriter planWriter = new AlgWriterImpl( new PrintWriter( sw ), detailLevel, false );
        alg.explain( planWriter );
        return sw.toString();
    }


    @Deprecated // to be removed before 2.0
    public static AlgNode renameIfNecessary( AlgNode alg, AlgDataType desiredRowType ) {
        final AlgDataType rowType = alg.getTupleType();
        if ( rowType == desiredRowType ) {
            // Nothing to do.
            return alg;
        }
        assert !rowType.equals( desiredRowType );

        if ( !areRowTypesEqual( rowType, desiredRowType, false ) ) {
            // The row types are different ignoring names. Nothing we can do.
            return alg;
        }
        alg = createRename( alg, desiredRowType.getFieldNames() );
        return alg;
    }


    public static String dumpType( AlgDataType type ) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter( sw );
        final TypeDumper typeDumper = new TypeDumper( pw );
        if ( type.isStruct() ) {
            typeDumper.acceptFields( type.getFields() );
        } else {
            typeDumper.accept( type );
        }
        pw.flush();
        return sw.toString();
    }


    /**
     * Returns the set of columns with unique names, with prior columns taking precedence over columns that appear later in the list.
     */
    public static List<AlgDataTypeField> deduplicateColumns( List<AlgDataTypeField> baseColumns, List<AlgDataTypeField> extendedColumns ) {
        final Set<String> dedupedFieldNames = new HashSet<>();
        final ImmutableList.Builder<AlgDataTypeField> dedupedFields = ImmutableList.builder();
        for ( AlgDataTypeField f : Iterables.concat( baseColumns, extendedColumns ) ) {
            if ( dedupedFieldNames.add( f.getName() ) ) {
                dedupedFields.add( f );
            }
        }
        return dedupedFields.build();
    }


    /**
     * Decomposes a predicate into a list of expressions that are AND'ed together.
     *
     * @param rexPredicate predicate to be analyzed
     * @param rexList list of decomposed RexNodes
     */
    public static void decomposeConjunction( RexNode rexPredicate, List<RexNode> rexList ) {
        if ( rexPredicate == null || rexPredicate.isAlwaysTrue() ) {
            return;
        }
        if ( rexPredicate.isA( Kind.AND ) ) {
            for ( RexNode operand : ((RexCall) rexPredicate).getOperands() ) {
                decomposeConjunction( operand, rexList );
            }
        } else {
            rexList.add( rexPredicate );
        }
    }


    /**
     * Decomposes a predicate into a list of expressions that are AND'ed together, and a list of expressions that are preceded by NOT.
     *
     * For example, {@code a AND NOT b AND NOT (c and d) AND TRUE AND NOT FALSE} returns {@code rexList = [a], notList = [b, c AND d]}.
     *
     * TRUE and NOT FALSE expressions are ignored. FALSE and NOT TRUE expressions are placed on {@code rexList} and {@code notList} as other expressions.
     *
     * For example, {@code a AND TRUE AND NOT TRUE} returns {@code rexList = [a], notList = [TRUE]}.
     *
     * @param rexPredicate predicate to be analyzed
     * @param rexList list of decomposed RexNodes (except those with NOT)
     * @param notList list of decomposed RexNodes that were prefixed NOT
     */
    public static void decomposeConjunction( RexNode rexPredicate, List<RexNode> rexList, List<RexNode> notList ) {
        if ( rexPredicate == null || rexPredicate.isAlwaysTrue() ) {
            return;
        }
        switch ( rexPredicate.getKind() ) {
            case AND:
                for ( RexNode operand : ((RexCall) rexPredicate).getOperands() ) {
                    decomposeConjunction( operand, rexList, notList );
                }
                break;
            case NOT:
                final RexNode e = ((RexCall) rexPredicate).getOperands().get( 0 );
                if ( e.isAlwaysFalse() ) {
                    return;
                }
                switch ( e.getKind() ) {
                    case OR:
                        final List<RexNode> ors = new ArrayList<>();
                        decomposeDisjunction( e, ors );
                        for ( RexNode or : ors ) {
                            switch ( or.getKind() ) {
                                case NOT:
                                    rexList.add( ((RexCall) or).operands.get( 0 ) );
                                    break;
                                default:
                                    notList.add( or );
                            }
                        }
                        break;
                    default:
                        notList.add( e );
                }
                break;
            case LITERAL:
                if ( !RexLiteral.isNullLiteral( rexPredicate ) && RexLiteral.booleanValue( rexPredicate ) ) {
                    return; // ignore TRUE
                }
                // fall through
            default:
                rexList.add( rexPredicate );
                break;
        }
    }


    /**
     * Decomposes a predicate into a list of expressions that are OR'ed together.
     *
     * @param rexPredicate predicate to be analyzed
     * @param rexList list of decomposed RexNodes
     */
    public static void decomposeDisjunction( RexNode rexPredicate, List<RexNode> rexList ) {
        if ( rexPredicate == null || rexPredicate.isAlwaysFalse() ) {
            return;
        }
        if ( rexPredicate.isA( Kind.OR ) ) {
            for ( RexNode operand : ((RexCall) rexPredicate).getOperands() ) {
                decomposeDisjunction( operand, rexList );
            }
        } else {
            rexList.add( rexPredicate );
        }
    }


    /**
     * Returns a condition decomposed by AND.
     *
     * For example, {@code conjunctions(TRUE)} returns the empty list; {@code conjunctions(FALSE)} returns list {@code {FALSE}}.
     */
    public static List<RexNode> conjunctions( RexNode rexPredicate ) {
        final List<RexNode> list = new ArrayList<>();
        decomposeConjunction( rexPredicate, list );
        return list;
    }


    /**
     * Returns a condition decomposed by OR.
     *
     * For example, {@code disjunctions(FALSE)} returns the empty list.
     */
    public static List<RexNode> disjunctions( RexNode rexPredicate ) {
        final List<RexNode> list = new ArrayList<>();
        decomposeDisjunction( rexPredicate, list );
        return list;
    }


    /**
     * Ands two sets of join filters together, either of which can be null.
     *
     * @param rexBuilder rexBuilder to create AND expression
     * @param left filter on the left that the right will be AND'd to
     * @param right filter on the right
     * @return AND'd filter
     * @see RexUtil#composeConjunction
     */
    public static RexNode andJoinFilters( RexBuilder rexBuilder, RexNode left, RexNode right ) {
        // don't bother AND'ing in expressions that always evaluate to true
        if ( (left != null) && !left.isAlwaysTrue() ) {
            if ( (right != null) && !right.isAlwaysTrue() ) {
                left = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.AND ), left, right );
            }
        } else {
            left = right;
        }

        // Joins must have some filter
        if ( left == null ) {
            left = rexBuilder.makeLiteral( true );
        }
        return left;
    }


    /**
     * Decomposes the WHERE clause of a view into predicates that constraint a column to a particular value.
     *
     * This method is key to the validation of a modifiable view. Columns that are constrained to a single value can be omitted from the SELECT clause of a modifiable view.
     *
     * @param projectMap Mapping from column ordinal to the expression that populate that column, to be populated by this method
     * @param filters List of remaining filters, to be populated by this method
     * @param constraint Constraint to be analyzed
     */
    public static void inferViewPredicates( Map<Integer, RexNode> projectMap, List<RexNode> filters, RexNode constraint ) {
        for ( RexNode node : conjunctions( constraint ) ) {
            switch ( node.getKind() ) {
                case EQUALS:
                    final List<RexNode> operands = ((RexCall) node).getOperands();
                    RexNode o0 = operands.get( 0 );
                    RexNode o1 = operands.get( 1 );
                    if ( o0 instanceof RexLiteral ) {
                        o0 = operands.get( 1 );
                        o1 = operands.get( 0 );
                    }
                    if ( o0.getKind() == Kind.CAST ) {
                        o0 = ((RexCall) o0).getOperands().get( 0 );
                    }
                    if ( o0 instanceof RexIndexRef && o1 instanceof RexLiteral ) {
                        final int index = ((RexIndexRef) o0).getIndex();
                        if ( projectMap.get( index ) == null ) {
                            projectMap.put( index, o1 );
                            continue;
                        }
                    }
            }
            filters.add( node );
        }
    }


    /**
     * Adjusts key values in a list by some fixed amount.
     *
     * @param keys list of key values
     * @param adjustment the amount to adjust the key values by
     * @return modified list
     */
    public static List<Integer> adjustKeys( List<Integer> keys, int adjustment ) {
        if ( adjustment == 0 ) {
            return keys;
        }
        final List<Integer> newKeys = new ArrayList<>();
        for ( int key : keys ) {
            newKeys.add( key + adjustment );
        }
        return newKeys;
    }


    /**
     * Simplifies outer joins if filter above would reject nulls.
     *
     * @param joinRel Join
     * @param aboveFilters Filters from above
     * @param joinType Join type, can not be inner join
     */
    public static JoinAlgType simplifyJoin( AlgNode joinRel, ImmutableList<RexNode> aboveFilters, JoinAlgType joinType ) {
        final int nTotalFields = joinRel.getTupleType().getFieldCount();
        final int nSysFields = 0;
        final int nFieldsLeft = joinRel.getInputs().get( 0 ).getTupleType().getFieldCount();
        final int nFieldsRight = joinRel.getInputs().get( 1 ).getTupleType().getFieldCount();
        assert nTotalFields == nSysFields + nFieldsLeft + nFieldsRight;

        // set the reference bitmaps for the left and right children
        ImmutableBitSet leftBitmap = ImmutableBitSet.range( nSysFields, nSysFields + nFieldsLeft );
        ImmutableBitSet rightBitmap = ImmutableBitSet.range( nSysFields + nFieldsLeft, nTotalFields );

        for ( RexNode filter : aboveFilters ) {
            if ( joinType.generatesNullsOnLeft() && Strong.isNotTrue( filter, leftBitmap ) ) {
                joinType = joinType.cancelNullsOnLeft();
            }
            if ( joinType.generatesNullsOnRight() && Strong.isNotTrue( filter, rightBitmap ) ) {
                joinType = joinType.cancelNullsOnRight();
            }
            if ( joinType == JoinAlgType.INNER ) {
                break;
            }
        }
        return joinType;
    }


    /**
     * Classifies filters according to where they should be processed. They either stay where they are, are pushed to the join (if they originated from above the join),
     * or are pushed to one of the children. Filters that are pushed are added to list passed in as input parameters.
     *
     * @param joinRel join node
     * @param filters filters to be classified
     * @param joinType join type
     * @param pushInto whether filters can be pushed into the ON clause
     * @param pushLeft true if filters can be pushed to the left
     * @param pushRight true if filters can be pushed to the right
     * @param joinFilters list of filters to push to the join
     * @param leftFilters list of filters to push to the left child
     * @param rightFilters list of filters to push to the right child
     * @return whether at least one filter was pushed
     */
    public static boolean classifyFilters( AlgNode joinRel, List<RexNode> filters, JoinAlgType joinType, boolean pushInto, boolean pushLeft, boolean pushRight, List<RexNode> joinFilters, List<RexNode> leftFilters, List<RexNode> rightFilters ) {
        RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
        List<AlgDataTypeField> joinFields = joinRel.getTupleType().getFields();
        final int nTotalFields = joinFields.size();
        final int nSysFields = 0; // joinRel.getSystemFieldList().size();
        final List<AlgDataTypeField> leftFields = joinRel.getInputs().get( 0 ).getTupleType().getFields();
        final int nFieldsLeft = leftFields.size();
        final List<AlgDataTypeField> rightFields = joinRel.getInputs().get( 1 ).getTupleType().getFields();
        final int nFieldsRight = rightFields.size();
        assert nTotalFields ==
                (joinRel instanceof SemiJoin
                        ? nSysFields + nFieldsLeft
                        : nSysFields + nFieldsLeft + nFieldsRight);

        // set the reference bitmaps for the left and right children
        ImmutableBitSet leftBitmap = ImmutableBitSet.range( nSysFields, nSysFields + nFieldsLeft );
        ImmutableBitSet rightBitmap = ImmutableBitSet.range( nSysFields + nFieldsLeft, nTotalFields );

        final List<RexNode> filtersToRemove = new ArrayList<>();
        for ( RexNode filter : filters ) {
            final InputFinder inputFinder = InputFinder.analyze( filter );
            final ImmutableBitSet inputBits = inputFinder.inputBitSet.build();

            // REVIEW - are there any expressions that need special handling and therefore cannot be pushed?

            // filters can be pushed to the left child if the left child does not generate NULLs and the only columns referenced in the filter originate from the left child
            if ( pushLeft && leftBitmap.contains( inputBits ) ) {
                // ignore filters that always evaluate to true
                if ( !filter.isAlwaysTrue() ) {
                    // adjust the field references in the filter to reflect that fields in the left now shift over by the number of system fields
                    final RexNode shiftedFilter = shiftFilter( nSysFields, nSysFields + nFieldsLeft, -nSysFields, rexBuilder, joinFields, nTotalFields, leftFields, filter );

                    leftFilters.add( shiftedFilter );
                }
                filtersToRemove.add( filter );

                // filters can be pushed to the right child if the right child does not generate NULLs and the only columns referenced in the filter originate from the right child
            } else if ( pushRight && rightBitmap.contains( inputBits ) ) {
                if ( !filter.isAlwaysTrue() ) {
                    // adjust the field references in the filter to reflect that fields in the right now shift over to the left; since we never push filters to a NULL generating child,
                    // the types of the source should match the dest so we don't need to explicitly pass the destination fields to RexInputConverter
                    final RexNode shiftedFilter = shiftFilter( nSysFields + nFieldsLeft, nTotalFields, -(nSysFields + nFieldsLeft), rexBuilder, joinFields, nTotalFields, rightFields, filter );
                    rightFilters.add( shiftedFilter );
                }
                filtersToRemove.add( filter );

            } else {
                // If the filter can't be pushed to either child and the join is an inner join, push them to the join if they originated from above the join
                if ( joinType == JoinAlgType.INNER && pushInto ) {
                    if ( !joinFilters.contains( filter ) ) {
                        joinFilters.add( filter );
                    }
                    filtersToRemove.add( filter );
                }
            }
        }

        // Remove filters after the loop, to prevent concurrent modification.
        if ( !filtersToRemove.isEmpty() ) {
            filters.removeAll( filtersToRemove );
        }

        // Did anything change?
        return !filtersToRemove.isEmpty();
    }


    private static RexNode shiftFilter( int start, int end, int offset, RexBuilder rexBuilder, List<AlgDataTypeField> joinFields, int nTotalFields, List<AlgDataTypeField> rightFields, RexNode filter ) {
        int[] adjustments = new int[nTotalFields];
        for ( int i = start; i < end; i++ ) {
            adjustments[i] = offset;
        }
        return filter.accept( new RexInputConverter( rexBuilder, joinFields, rightFields, adjustments ) );
    }


    /**
     * Splits a filter into two lists, depending on whether or not the filter only references its child input
     *
     * @param childBitmap Fields in the child
     * @param predicate filters that will be split
     * @param pushable returns the list of filters that can be pushed to the child input
     * @param notPushable returns the list of filters that cannot be pushed to the child input
     */
    public static void splitFilters( ImmutableBitSet childBitmap, RexNode predicate, List<RexNode> pushable, List<RexNode> notPushable ) {
        // for each filter, if the filter only references the child inputs, then it can be pushed
        for ( RexNode filter : conjunctions( predicate ) ) {
            ImmutableBitSet filterRefs = InputFinder.bits( filter );
            if ( childBitmap.contains( filterRefs ) ) {
                pushable.add( filter );
            } else {
                notPushable.add( filter );
            }
        }
    }


    @Deprecated // to be removed before 2.0
    public static boolean checkProjAndChildInputs( Project project, boolean checkNames ) {
        int n = project.getProjects().size();
        AlgDataType inputType = project.getInput().getTupleType();
        if ( inputType.getFields().size() != n ) {
            return false;
        }
        List<AlgDataTypeField> projFields = project.getTupleType().getFields();
        List<AlgDataTypeField> inputFields = inputType.getFields();
        boolean namesDifferent = false;
        for ( int i = 0; i < n; ++i ) {
            RexNode exp = project.getProjects().get( i );
            if ( !(exp instanceof RexIndexRef) ) {
                return false;
            }
            RexIndexRef fieldAccess = (RexIndexRef) exp;
            if ( i != fieldAccess.getIndex() ) {
                // can't support reorder yet
                return false;
            }
            if ( checkNames ) {
                String inputFieldName = inputFields.get( i ).getName();
                String projFieldName = projFields.get( i ).getName();
                if ( !projFieldName.equals( inputFieldName ) ) {
                    namesDifferent = true;
                }
            }
        }

        // inputs are the same; return value depends on the checkNames parameter
        return !checkNames || namesDifferent;
    }


    /**
     * Creates projection expressions reflecting the swapping of a join's input.
     *
     * @param newJoin the {@link AlgNode} corresponding to the join with its inputs swapped
     * @param origJoin original LogicalJoin
     * @param origOrder if true, create the projection expressions to reflect the original (pre-swapped) join projection; otherwise, create the projection to reflect the order of the swapped projection
     * @return array of expression representing the swapped join inputs
     */
    public static List<RexNode> createSwappedJoinExprs( AlgNode newJoin, Join origJoin, boolean origOrder ) {
        final List<AlgDataTypeField> newJoinFields = newJoin.getTupleType().getFields();
        final RexBuilder rexBuilder = newJoin.getCluster().getRexBuilder();
        final List<RexNode> exps = new ArrayList<>();
        final int nFields =
                origOrder
                        ? origJoin.getRight().getTupleType().getFieldCount()
                        : origJoin.getLeft().getTupleType().getFieldCount();
        for ( int i = 0; i < newJoinFields.size(); i++ ) {
            final int source = (i + nFields) % newJoinFields.size();
            AlgDataTypeField field = origOrder ? newJoinFields.get( source ) : newJoinFields.get( i );
            exps.add( rexBuilder.makeInputRef( field.getType(), source ) );
        }
        return exps;
    }


    @Deprecated // to be removed before 2.0
    public static RexNode pushFilterPastProject( RexNode filter, final Project projRel ) {
        return pushPastProject( filter, projRel );
    }


    /**
     * Converts an expression that is based on the output fields of a {@link Project} to an equivalent expression on the Project's input fields.
     *
     * @param node The expression to be converted
     * @param project Project underneath the expression
     * @return converted expression
     */
    public static RexNode pushPastProject( RexNode node, Project project ) {
        return node.accept( pushShuttle( project ) );
    }


    /**
     * Converts a list of expressions that are based on the output fields of a {@link Project} to equivalent expressions on the Project's input fields.
     *
     * @param nodes The expressions to be converted
     * @param project Project underneath the expression
     * @return converted expressions
     */
    public static List<RexNode> pushPastProject( List<? extends RexNode> nodes, Project project ) {
        final List<RexNode> list = new ArrayList<>();
        pushShuttle( project ).visitList( nodes, list );
        return list;
    }


    private static RexShuttle pushShuttle( final Project project ) {
        return new RexShuttle() {
            @Override
            public RexNode visitIndexRef( RexIndexRef ref ) {
                return project.getProjects().get( ref.getIndex() );
            }
        };
    }


    /**
     * Creates a new {@link MultiJoin} to reflect projection references from a {@link LogicalRelProject}
     * that is on top of the {@link MultiJoin}.
     *
     * @param multiJoin the original MultiJoin
     * @param project the LogicalProject on top of the MultiJoin
     * @return the new MultiJoin
     */
    public static MultiJoin projectMultiJoin( MultiJoin multiJoin, LogicalRelProject project ) {
        // Locate all input references in the projection expressions as well the post-join filter.  Since the filter effectively sits in between the LogicalProject and the MultiJoin,
        // the projection needs to include those filter references.
        ImmutableBitSet inputRefs = InputFinder.bits( project.getProjects(), multiJoin.getPostJoinFilter() );

        // create new copies of the bitmaps
        List<AlgNode> multiJoinInputs = multiJoin.getInputs();
        List<BitSet> newProjFields = new ArrayList<>();
        for ( AlgNode multiJoinInput : multiJoinInputs ) {
            newProjFields.add( new BitSet( multiJoinInput.getTupleType().getFieldCount() ) );
        }

        // set the bits found in the expressions
        int currInput = -1;
        int startField = 0;
        int nFields = 0;
        for ( int bit : inputRefs ) {
            while ( bit >= (startField + nFields) ) {
                startField += nFields;
                currInput++;
                assert currInput < multiJoinInputs.size();
                nFields = multiJoinInputs.get( currInput ).getTupleType().getFieldCount();
            }
            newProjFields.get( currInput ).set( bit - startField );
        }

        // create a new MultiJoin containing the new field bitmaps for each input
        return new MultiJoin(
                multiJoin.getCluster(),
                multiJoin.getInputs(),
                multiJoin.getJoinFilter(),
                multiJoin.getTupleType(),
                multiJoin.isFullOuterJoin(),
                multiJoin.getOuterJoinConditions(),
                multiJoin.getJoinTypes(),
                Lists.transform( newProjFields, ImmutableBitSet::fromBitSet ),
                multiJoin.getJoinFieldRefCountsMap(),
                multiJoin.getPostJoinFilter() );
    }


    public static <T extends AlgNode> T addTrait( T alg, AlgTrait trait ) {
        //noinspection unchecked
        return (T) alg.copy( alg.getTraitSet().replace( trait ), alg.getInputs() );
    }


    /**
     * Returns a shallow copy of a relational expression with a particular input replaced.
     */
    public static AlgNode replaceInput( AlgNode parent, int ordinal, AlgNode newInput ) {
        final List<AlgNode> inputs = new ArrayList<>( parent.getInputs() );
        if ( inputs.get( ordinal ) == newInput ) {
            return parent;
        }
        inputs.set( ordinal, newInput );
        return parent.copy( parent.getTraitSet(), inputs );
    }


    /**
     * Creates a {@link LogicalRelProject} that projects particular fields of its input, according to a mapping.
     */
    public static AlgNode createProject( AlgNode child, Mappings.TargetMapping mapping ) {
        return createProject( child, Mappings.asList( mapping.inverse() ) );
    }


    public static AlgNode createProject( AlgNode child, Mappings.TargetMapping mapping, AlgFactories.ProjectFactory projectFactory ) {
        return createProject( projectFactory, child, Mappings.asList( mapping.inverse() ) );
    }


    /**
     * Returns whether relational expression {@code target} occurs within a relational expression {@code ancestor}.
     */
    public static boolean contains( AlgNode ancestor, final AlgNode target ) {
        if ( ancestor == target ) {
            // Short-cut common case.
            return true;
        }
        try {
            new AlgVisitor() {
                @Override
                public void visit( AlgNode node, int ordinal, AlgNode parent ) {
                    if ( node == target ) {
                        throw Util.FoundOne.NULL;
                    }
                    super.visit( node, ordinal, parent );
                }
                // CHECKSTYLE: IGNORE 1
            }.go( ancestor );
            return false;
        } catch ( Util.FoundOne e ) {
            return true;
        }
    }


    /**
     * Within a relational expression {@code query}, replaces occurrences of {@code find} with {@code replace}.
     */
    public static AlgNode replace( AlgNode query, AlgNode find, AlgNode replace ) {
        if ( find == replace ) {
            // Short-cut common case.
            return query;
        }
        assert equalType( "find", find, "replace", replace, Litmus.THROW );
        if ( query == find ) {
            // Short-cut another common case.
            return replace;
        }
        return replaceRecurse( query, find, replace );
    }


    /**
     * Helper for {@link #replace}.
     */
    private static AlgNode replaceRecurse( AlgNode query, AlgNode find, AlgNode replace ) {
        if ( query == find ) {
            return replace;
        }
        final List<AlgNode> inputs = query.getInputs();
        if ( !inputs.isEmpty() ) {
            final List<AlgNode> newInputs = new ArrayList<>();
            for ( AlgNode input : inputs ) {
                newInputs.add( replaceRecurse( input, find, replace ) );
            }
            if ( !newInputs.equals( inputs ) ) {
                return query.copy( query.getTraitSet(), newInputs );
            }
        }
        return query;
    }


    /**
     * Returns the number of {@link Join} nodes in a tree.
     */
    public static int countJoins( AlgNode rootAlg ) {
        // Visitor that counts join nodes.
        class JoinCounter extends AlgVisitor {

            int joinCount;


            @Override
            public void visit( AlgNode node, int ordinal, AlgNode parent ) {
                if ( node instanceof Join ) {
                    ++joinCount;
                }
                super.visit( node, ordinal, parent );
            }


            int run( AlgNode node ) {
                go( node );
                return joinCount;
            }

        }

        return new JoinCounter().run( rootAlg );
    }


    /**
     * Permutes a record type according to a mapping.
     */
    public static AlgDataType permute( AlgDataTypeFactory typeFactory, AlgDataType rowType, Mapping mapping ) {
        return typeFactory.createStructType( Mappings.apply3( mapping, rowType.getFields() ) );
    }


    @Deprecated // to be removed before 2.0
    public static AlgNode createProject( AlgNode child, List<? extends RexNode> exprList, List<String> fieldNameList ) {
        final AlgBuilder algBuilder = AlgFactories.LOGICAL_BUILDER.create( child.getCluster(), null );
        return algBuilder.push( child )
                .project( exprList, fieldNameList, true )
                .build();
    }


    @Deprecated // to be removed before 2.0
    public static AlgNode createProject( AlgNode child, List<Pair<RexNode, String>> projectList, boolean optimize ) {
        final AlgBuilder algBuilder = AlgFactories.LOGICAL_BUILDER.create( child.getCluster(), null );
        return algBuilder.push( child )
                .projectNamed( Pair.left( projectList ), Pair.right( projectList ), !optimize )
                .build();
    }


    /**
     * Creates a relational expression that projects the given fields of the input.
     *
     * Optimizes if the fields are the identity projection.
     *
     * @param child Input relational expression
     * @param posList Source of each projected field
     * @return Relational expression that projects given fields
     */
    public static AlgNode createProject( final AlgNode child, final List<Integer> posList ) {
        return createProject( AlgFactories.DEFAULT_PROJECT_FACTORY, child, posList );
    }


    @Deprecated // to be removed before 2.0
    public static AlgNode createRename( AlgNode alg, List<String> fieldNames ) {
        final List<AlgDataTypeField> fields = alg.getTupleType().getFields();
        assert fieldNames.size() == fields.size();
        final List<RexNode> refs =
                new AbstractList<RexNode>() {
                    @Override
                    public int size() {
                        return fields.size();
                    }


                    @Override
                    public RexNode get( int index ) {
                        return RexIndexRef.of( index, fields );
                    }
                };
        final AlgBuilder algBuilder = AlgFactories.LOGICAL_BUILDER.create( alg.getCluster(), null );
        return algBuilder.push( alg )
                .projectNamed( refs, fieldNames, false )
                .build();
    }


    /**
     * Creates a relational expression which permutes the output fields of a relational expression according to a permutation.
     *
     * Optimizations:
     *
     * <ul>
     * <li>If the relational expression is a {@link LogicalCalc} or {@link LogicalRelProject} that is already acting as a permutation, combines the new permutation with the old;</li>
     * <li>If the permutation is the identity, returns the original relational expression.</li>
     * </ul>
     *
     * If a permutation is combined with its inverse, these optimizations would combine to remove them both.
     *
     * @param alg Relational expression
     * @param permutation Permutation to apply to fields
     * @param fieldNames Field names; if null, or if a particular entry is null, the name of the permuted field is used
     * @return relational expression which permutes its input fields
     */
    public static AlgNode permute( AlgNode alg, Permutation permutation, List<String> fieldNames ) {
        if ( permutation.isIdentity() ) {
            return alg;
        }
        if ( alg instanceof LogicalCalc ) {
            LogicalCalc calc = (LogicalCalc) alg;
            Permutation permutation1 = calc.getProgram().getPermutation();
            if ( permutation1 != null ) {
                Permutation permutation2 = permutation.product( permutation1 );
                return permute( alg, permutation2, null );
            }
        }
        if ( alg instanceof LogicalRelProject ) {
            Permutation permutation1 = ((LogicalRelProject) alg).getPermutation();
            if ( permutation1 != null ) {
                Permutation permutation2 = permutation.product( permutation1 );
                return permute( alg, permutation2, null );
            }
        }
        final List<AlgDataType> outputTypeList = new ArrayList<>();
        final List<String> outputNameList = new ArrayList<>();
        final List<RexNode> exprList = new ArrayList<>();
        final List<RexLocalRef> projectRefList = new ArrayList<>();
        final List<AlgDataTypeField> fields = alg.getTupleType().getFields();
        final AlgCluster cluster = alg.getCluster();
        for ( int i = 0; i < permutation.getTargetCount(); i++ ) {
            int target = permutation.getTarget( i );
            final AlgDataTypeField targetField = fields.get( target );
            outputTypeList.add( targetField.getType() );
            outputNameList.add(
                    ((fieldNames == null) || (fieldNames.size() <= i) || (fieldNames.get( i ) == null))
                            ? targetField.getName()
                            : fieldNames.get( i ) );
            exprList.add( cluster.getRexBuilder().makeInputRef( fields.get( i ).getType(), i ) );
            final int source = permutation.getSource( i );
            projectRefList.add( new RexLocalRef( source, fields.get( source ).getType() ) );
        }
        final AlgDataTypeFactory typeFactory = cluster.getTypeFactory();
        final RexProgram program =
                new RexProgram(
                        alg.getTupleType(),
                        exprList,
                        projectRefList,
                        null,
                        typeFactory.createStructType( null, outputTypeList, outputNameList ) );
        return LogicalCalc.create( alg, program );
    }


    /**
     * Creates a relational expression that projects the given fields of the input.
     *
     * Optimizes if the fields are the identity projection.
     *
     * @param factory ProjectFactory
     * @param child Input relational expression
     * @param posList Source of each projected field
     * @return Relational expression that projects given fields
     */
    public static AlgNode createProject( final AlgFactories.ProjectFactory factory, final AlgNode child, final List<Integer> posList ) {
        AlgDataType rowType = child.getTupleType();
        final List<String> fieldNames = rowType.getFieldNames();
        final AlgBuilder algBuilder = AlgBuilder.proto( factory ).create( child.getCluster(), null );
        final List<RexNode> exprs = new AbstractList<RexNode>() {
            @Override
            public int size() {
                return posList.size();
            }


            @Override
            public RexNode get( int index ) {
                final int pos = posList.get( index );
                return algBuilder.getRexBuilder().makeInputRef( child, pos );
            }
        };
        final List<String> names = new AbstractList<String>() {
            @Override
            public int size() {
                return posList.size();
            }


            @Override
            public String get( int index ) {
                final int pos = posList.get( index );
                return fieldNames.get( pos );
            }
        };
        return algBuilder
                .push( child )
                .projectNamed( exprs, names, false )
                .build();
    }


    @Deprecated // to be removed before 2.0
    public static AlgNode projectMapping( AlgNode alg, Mapping mapping, List<String> fieldNames, AlgFactories.ProjectFactory projectFactory ) {
        assert mapping.getMappingType().isSingleSource();
        assert mapping.getMappingType().isMandatorySource();
        if ( mapping.isIdentity() ) {
            return alg;
        }
        final List<String> outputNameList = new ArrayList<>();
        final List<RexNode> exprList = new ArrayList<>();
        final List<AlgDataTypeField> fields = alg.getTupleType().getFields();
        final RexBuilder rexBuilder = alg.getCluster().getRexBuilder();
        for ( int i = 0; i < mapping.getTargetCount(); i++ ) {
            final int source = mapping.getSource( i );
            final AlgDataTypeField sourceField = fields.get( source );
            outputNameList.add(
                    ((fieldNames == null) || (fieldNames.size() <= i) || (fieldNames.get( i ) == null))
                            ? sourceField.getName()
                            : fieldNames.get( i ) );
            exprList.add( rexBuilder.makeInputRef( alg, source ) );
        }
        return projectFactory.createProject( alg, exprList, outputNameList );
    }


    /**
     * Predicate for whether a {@link Calc} contains multisets or windowed aggregates.
     */
    public static boolean containsMultisetOrWindowedAgg( Calc calc ) {
        return !(B && RexMultisetUtil.containsMultiset( calc.getProgram() ) || calc.getProgram().containsAggs());
    }


    /**
     * Predicate for whether a {@link Filter} contains multisets or windowed aggregates.
     */
    public static boolean containsMultisetOrWindowedAgg( Filter filter ) {
        return !(B && RexMultisetUtil.containsMultiset( filter.getCondition(), true ) || RexOver.containsOver( filter.getCondition() ));
    }


    /**
     * Predicate for whether a {@link Project} contains multisets or windowed aggregates.
     */
    public static boolean containsMultisetOrWindowedAgg( Project project ) {
        return !(B && RexMultisetUtil.containsMultiset( project.getProjects(), true ) || RexOver.containsOver( project.getProjects(), null ));
    }


    /**
     * Policies for handling two- and three-valued boolean logic.
     */
    public enum Logic {
        /**
         * Three-valued boolean logic.
         */
        TRUE_FALSE_UNKNOWN,

        /**
         * Nulls are not possible.
         */
        TRUE_FALSE,

        /**
         * Two-valued logic where UNKNOWN is treated as FALSE.
         *
         * "x IS TRUE" produces the same result, and "WHERE x", "JOIN ... ON x" and "HAVING x" have the same effect.
         */
        UNKNOWN_AS_FALSE,

        /**
         * Two-valued logic where UNKNOWN is treated as TRUE.
         *
         * "x IS FALSE" produces the same result, as does "WHERE NOT x", etc.
         *
         * In particular, this is the mode used by "WHERE k NOT IN q". If "k IN q" produces TRUE or UNKNOWN, "NOT k IN q" produces FALSE or UNKNOWN and the row is eliminated;
         * if "k IN q" it returns FALSE, the row is retained by the WHERE clause.
         */
        UNKNOWN_AS_TRUE,

        /**
         * A semi-join will have been applied, so that only rows for which the value is TRUE will have been returned.
         */
        TRUE,

        /**
         * An anti-semi-join will have been applied, so that only rows for which the value is FALSE will have been returned.
         *
         * Currently only used within {@link LogicVisitor}, to ensure that 'NOT (NOT EXISTS (q))' behaves the same as 'EXISTS (q)')
         */
        FALSE;


        public Logic negate() {
            switch ( this ) {
                case UNKNOWN_AS_FALSE:
                case TRUE:
                    return UNKNOWN_AS_TRUE;
                case UNKNOWN_AS_TRUE:
                    return UNKNOWN_AS_FALSE;
                default:
                    return this;
            }
        }


        /**
         * Variant of {@link #negate()} to be used within {@link LogicVisitor}, where FALSE values may exist.
         */
        public Logic negate2() {
            switch ( this ) {
                case FALSE:
                    return TRUE;
                case TRUE:
                    return FALSE;
                case UNKNOWN_AS_FALSE:
                    return UNKNOWN_AS_TRUE;
                case UNKNOWN_AS_TRUE:
                    return UNKNOWN_AS_FALSE;
                default:
                    return this;
            }
        }
    }


    /**
     * Pushes down expressions in "equal" join condition.
     *
     * For example, given "emp JOIN dept ON emp.deptno + 1 = dept.deptno", adds a project above "emp" that computes the expression
     * "emp.deptno + 1". The resulting join condition is a simple combination of AND, equals, and input fields, plus the remaining non-equal conditions.
     *
     * @param originalJoin Join whose condition is to be pushed down
     * @param algBuilder Factory to create project operator
     */
    public static AlgNode pushDownJoinConditions( Join originalJoin, AlgBuilder algBuilder ) {
        RexNode joinCond = originalJoin.getCondition();
        final JoinAlgType joinType = originalJoin.getJoinType();

        final List<RexNode> extraLeftExprs = new ArrayList<>();
        final List<RexNode> extraRightExprs = new ArrayList<>();
        final int leftCount = originalJoin.getLeft().getTupleType().getFieldCount();
        final int rightCount = originalJoin.getRight().getTupleType().getFieldCount();

        // You cannot push a 'get' because field names might change.
        //
        // Pushing sub-queries is OK in principle (if they don't reference both sides of the join via correlating variables) but we'd rather not do it yet.
        if ( !containsGet( joinCond ) && RexUtil.SubQueryFinder.find( joinCond ) == null ) {
            joinCond = pushDownEqualJoinConditions( joinCond, leftCount, rightCount, extraLeftExprs, extraRightExprs );
        }

        algBuilder.push( originalJoin.getLeft() );
        if ( !extraLeftExprs.isEmpty() ) {
            final List<AlgDataTypeField> fields = algBuilder.peek().getTupleType().getFields();
            final List<Pair<RexNode, String>> pairs =
                    new AbstractList<Pair<RexNode, String>>() {
                        @Override
                        public int size() {
                            return leftCount + extraLeftExprs.size();
                        }


                        @Override
                        public Pair<RexNode, String> get( int index ) {
                            if ( index < leftCount ) {
                                AlgDataTypeField field = fields.get( index );
                                return Pair.of( new RexIndexRef( index, field.getType() ), field.getName() );
                            } else {
                                return Pair.of( extraLeftExprs.get( index - leftCount ), null );
                            }
                        }
                    };
            algBuilder.project( Pair.left( pairs ), Pair.right( pairs ) );
        }

        algBuilder.push( originalJoin.getRight() );
        if ( !extraRightExprs.isEmpty() ) {
            final List<AlgDataTypeField> fields = algBuilder.peek().getTupleType().getFields();
            final int newLeftCount = leftCount + extraLeftExprs.size();
            final List<Pair<RexNode, String>> pairs =
                    new AbstractList<Pair<RexNode, String>>() {
                        @Override
                        public int size() {
                            return rightCount + extraRightExprs.size();
                        }


                        @Override
                        public Pair<RexNode, String> get( int index ) {
                            if ( index < rightCount ) {
                                AlgDataTypeField field = fields.get( index );
                                return Pair.of(
                                        new RexIndexRef( index, field.getType() ),
                                        field.getName() );
                            } else {
                                return Pair.of(
                                        RexUtil.shift( extraRightExprs.get( index - rightCount ), -newLeftCount ),
                                        null );
                            }
                        }
                    };
            algBuilder.project( Pair.left( pairs ), Pair.right( pairs ) );
        }

        final AlgNode right = algBuilder.build();
        final AlgNode left = algBuilder.build();
        algBuilder.push( originalJoin.copy( originalJoin.getTraitSet(), joinCond, left, right, joinType, originalJoin.isSemiJoinDone() ) );

        if ( !extraLeftExprs.isEmpty() || !extraRightExprs.isEmpty() ) {
            Mappings.TargetMapping mapping =
                    Mappings.createShiftMapping(
                            leftCount + extraLeftExprs.size() + rightCount + extraRightExprs.size(),
                            0, 0, leftCount,
                            leftCount, leftCount + extraLeftExprs.size(), rightCount );
            algBuilder.project( algBuilder.fields( mapping.inverse() ) );
        }
        return algBuilder.build();
    }


    private static AlgNode pushDownJoinConditions( Join originalJoin, AlgBuilderFactory algBuilderFactory ) {
        return pushDownJoinConditions( originalJoin, algBuilderFactory.create( originalJoin.getCluster(), null ) );
    }


    private static boolean containsGet( RexNode node ) {
        try {
            node.accept(
                    new RexVisitorImpl<Void>( true ) {
                        @Override
                        public Void visitCall( RexCall call ) {
                            if ( call.getOperator().equals( RexBuilder.GET_OPERATOR ) ) {
                                throw Util.FoundOne.NULL;
                            }
                            return super.visitCall( call );
                        }
                    } );
            return false;
        } catch ( Util.FoundOne e ) {
            return true;
        }
    }


    /**
     * Pushes down parts of a join condition.
     *
     * For example, given
     * "emp JOIN dept ON emp.deptno + 1 = dept.deptno", adds a project above
     * "emp" that computes the expression
     * "emp.deptno + 1". The resulting join condition is a simple combination
     * of AND, equals, and input fields.
     */
    private static RexNode pushDownEqualJoinConditions( RexNode node, int leftCount, int rightCount, List<RexNode> extraLeftExprs, List<RexNode> extraRightExprs ) {
        switch ( node.getKind() ) {
            case AND:
            case EQUALS:
                final RexCall call = (RexCall) node;
                final List<RexNode> list = new ArrayList<>();
                List<RexNode> operands = Lists.newArrayList( call.getOperands() );
                for ( int i = 0; i < operands.size(); i++ ) {
                    RexNode operand = operands.get( i );
                    final int left2 = leftCount + extraLeftExprs.size();
                    final int right2 = rightCount + extraRightExprs.size();
                    final RexNode e = pushDownEqualJoinConditions( operand, leftCount, rightCount, extraLeftExprs, extraRightExprs );
                    final List<RexNode> remainingOperands = Util.skip( operands, i + 1 );
                    final int left3 = leftCount + extraLeftExprs.size();
                    fix( remainingOperands, left2, left3 );
                    fix( list, left2, left3 );
                    list.add( e );
                }
                if ( !list.equals( call.getOperands() ) ) {
                    return call.clone( call.getType(), list );
                }
                return call;
            case OR:
            case INPUT_REF:
            case LITERAL:
            case NOT:
                return node;
            default:
                final ImmutableBitSet bits = AlgOptUtil.InputFinder.bits( node );
                final int mid = leftCount + extraLeftExprs.size();
                switch ( Side.of( bits, mid ) ) {
                    case LEFT:
                        fix( extraRightExprs, mid, mid + 1 );
                        extraLeftExprs.add( node );
                        return new RexIndexRef( mid, node.getType() );
                    case RIGHT:
                        final int index2 = mid + rightCount + extraRightExprs.size();
                        extraRightExprs.add( node );
                        return new RexIndexRef( index2, node.getType() );
                    case BOTH:
                    case EMPTY:
                    default:
                        return node;
                }
        }
    }


    private static void fix( List<RexNode> operands, int before, int after ) {
        if ( before == after ) {
            return;
        }
        for ( int i = 0; i < operands.size(); i++ ) {
            RexNode node = operands.get( i );
            operands.set( i, RexUtil.shift( node, before, after - before ) );
        }
    }


    /**
     * Determines whether any of the fields in a given relational expression may contain null values, taking into account constraints on the field types and also deduced predicates.
     *
     * The method is cautious: It may sometimes return {@code true} when the actual answer is {@code false}. In particular, it does this when there is no executor, or the executor is not a sub-class of {@link RexExecutorImpl}.
     */
    private static boolean containsNullableFields( AlgNode r ) {
        final RexBuilder rexBuilder = r.getCluster().getRexBuilder();
        final AlgDataType rowType = r.getTupleType();
        final List<RexNode> list = new ArrayList<>();
        final AlgMetadataQuery mq = r.getCluster().getMetadataQuery();
        for ( AlgDataTypeField field : rowType.getFields() ) {
            if ( field.getType().isNullable() ) {
                list.add( rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NOT_NULL ), rexBuilder.makeInputRef( field.getType(), field.getIndex() ) ) );
            }
        }
        if ( list.isEmpty() ) {
            // All columns are declared NOT NULL.
            return false;
        }
        final AlgOptPredicateList predicates = mq.getPulledUpPredicates( r );
        if ( predicates.pulledUpPredicates.isEmpty() ) {
            // We have no predicates, so cannot deduce that any of the fields declared NULL are really NOT NULL.
            return true;
        }
        final RexExecutor executor = r.getCluster().getPlanner().getExecutor();
        if ( !(executor instanceof RexExecutorImpl) ) {
            // Cannot proceed without an executor.
            return true;
        }
        final RexImplicationChecker checker = new RexImplicationChecker( rexBuilder, (RexExecutorImpl) executor, rowType );
        final RexNode first = RexUtil.composeConjunction( rexBuilder, predicates.pulledUpPredicates );
        final RexNode second = RexUtil.composeConjunction( rexBuilder, list );
        // Suppose we have EMP(empno INT NOT NULL, mgr INT), and predicates [empno > 0, mgr > 0].
        // We make first: "empno > 0 AND mgr > 0" and second: "mgr IS NOT NULL" and ask whether first implies second.
        // It does, so we have no nullable columns.
        return !checker.implies( first, second );
    }


    /**
     * Visitor that finds all variables used but not stopped in an expression.
     */
    private static class VariableSetVisitor extends AlgVisitor {

        final Set<CorrelationId> variables = new HashSet<>();


        // implement RelVisitor
        @Override
        public void visit( AlgNode p, int ordinal, AlgNode parent ) {
            super.visit( p, ordinal, parent );
            p.collectVariablesUsed( variables );
            // Important! Remove stopped variables AFTER we visit children (which what super.visit() does)
            variables.removeAll( p.getVariablesSet() );
        }

    }


    /**
     * Visitor that finds all variables used in an expression.
     */
    public static class VariableUsedVisitor extends RexShuttle {

        public final Set<CorrelationId> variables = new LinkedHashSet<>();
        public final Multimap<CorrelationId, Integer> variableFields = LinkedHashMultimap.create();
        private final AlgShuttle algShuttle;


        public VariableUsedVisitor( AlgShuttle algShuttle ) {
            this.algShuttle = algShuttle;
        }


        @Override
        public RexNode visitCorrelVariable( RexCorrelVariable p ) {
            variables.add( p.id );
            variableFields.put( p.id, -1 );
            return p;
        }


        @Override
        public RexNode visitFieldAccess( RexFieldAccess fieldAccess ) {
            if ( fieldAccess.getReferenceExpr() instanceof RexCorrelVariable ) {
                final RexCorrelVariable v = (RexCorrelVariable) fieldAccess.getReferenceExpr();
                variableFields.put( v.id, fieldAccess.getField().getIndex() );
            }
            return super.visitFieldAccess( fieldAccess );
        }


        @Override
        public RexNode visitSubQuery( RexSubQuery subQuery ) {
            if ( algShuttle != null ) {
                subQuery.alg.accept( algShuttle ); // look inside sub-queries
            }
            return super.visitSubQuery( subQuery );
        }

    }


    /**
     * Shuttle that finds the set of inputs that are used.
     */
    public static class InputReferencedVisitor extends RexShuttle {

        public final SortedSet<Integer> inputPosReferenced = new TreeSet<>();


        @Override
        public RexNode visitIndexRef( RexIndexRef inputRef ) {
            inputPosReferenced.add( inputRef.getIndex() );
            return inputRef;
        }

    }


    /**
     * Converts types to descriptive strings.
     */
    public static class TypeDumper {

        private final String extraIndent = "  ";
        private String indent;
        private final PrintWriter pw;


        TypeDumper( PrintWriter pw ) {
            this.pw = pw;
            this.indent = "";
        }


        void accept( AlgDataType type ) {
            if ( type.isStruct() ) {
                final List<AlgDataTypeField> fields = type.getFields();

                // RECORD (
                //   I INTEGER NOT NULL,
                //   J VARCHAR(240))
                pw.println( "RECORD (" );
                String prevIndent = indent;
                this.indent = indent + extraIndent;
                acceptFields( fields );
                this.indent = prevIndent;
                pw.print( ")" );
                if ( !type.isNullable() ) {
                    pw.print( " NOT NULL" );
                }
            } else if ( type instanceof MultisetPolyType ) {
                // E.g. "INTEGER NOT NULL MULTISET NOT NULL"
                accept( type.getComponentType() );
                pw.print( " MULTISET" );
                if ( !type.isNullable() ) {
                    pw.print( " NOT NULL" );
                }
            } else {
                // E.g. "INTEGER" E.g. "VARCHAR(240) CHARACTER SET "ISO-8859-1" COLLATE "ISO-8859-1$en_US$primary" NOT NULL"
                pw.print( type.getFullTypeString() );
            }
        }


        private void acceptFields( final List<AlgDataTypeField> fields ) {
            for ( int i = 0; i < fields.size(); i++ ) {
                AlgDataTypeField field = fields.get( i );
                if ( i > 0 ) {
                    pw.println( "," );
                }
                pw.print( indent );
                pw.print( field.getName() );
                pw.print( " " );
                accept( field.getType() );
            }
        }

    }


    /**
     * Visitor which builds a bitmap of the inputs used by an expression.
     */
    public static class InputFinder extends RexVisitorImpl<Void> {

        public final ImmutableBitSet.Builder inputBitSet;
        private final Set<AlgDataTypeField> extraFields;


        public InputFinder() {
            this( null );
        }


        public InputFinder( Set<AlgDataTypeField> extraFields ) {
            super( true );
            this.inputBitSet = ImmutableBitSet.builder();
            this.extraFields = extraFields;
        }


        /**
         * Returns an input finder that has analyzed a given expression.
         */
        public static InputFinder analyze( RexNode node ) {
            final InputFinder inputFinder = new InputFinder();
            node.accept( inputFinder );
            return inputFinder;
        }


        /**
         * Returns a bit set describing the inputs used by an expression.
         */
        public static ImmutableBitSet bits( RexNode node ) {
            return analyze( node ).inputBitSet.build();
        }


        /**
         * Returns a bit set describing the inputs used by a collection of project expressions and an optional condition.
         */
        public static ImmutableBitSet bits( List<RexNode> exprs, RexNode expr ) {
            final InputFinder inputFinder = new InputFinder();
            RexUtil.apply( inputFinder, exprs, expr );
            return inputFinder.inputBitSet.build();
        }


        @Override
        public Void visitIndexRef( RexIndexRef inputRef ) {
            inputBitSet.set( inputRef.getIndex() );
            return null;
        }


        @Override
        public Void visitCall( RexCall call ) {
            if ( call.getOperator().equals( RexBuilder.GET_OPERATOR ) ) {
                RexLiteral literal = (RexLiteral) call.getOperands().get( 1 );
                extraFields.add( new AlgDataTypeFieldImpl( -1L, literal.getValue().asString().value, -1, call.getType() ) );
            }
            return super.visitCall( call );
        }

    }


    /**
     * Walks an expression tree, converting the index of RexInputRefs based on some adjustment factor.
     */
    public static class RexInputConverter extends RexShuttle {

        protected final RexBuilder rexBuilder;
        private final List<AlgDataTypeField> srcFields;
        protected final List<AlgDataTypeField> destFields;
        private final List<AlgDataTypeField> leftDestFields;
        private final List<AlgDataTypeField> rightDestFields;
        private final int nLeftDestFields;
        private final int[] adjustments;


        /**
         * @param rexBuilder builder for creating new RexInputRefs
         * @param srcFields fields where the RexInputRefs originated from; if null, a new RexInputRef is always created, referencing the input from destFields corresponding to its current index value
         * @param destFields fields that the new RexInputRefs will be referencing; if null, use the type information from the source field when creating the new RexInputRef
         * @param leftDestFields in the case where the destination is a join, these are the fields from the left join input
         * @param rightDestFields in the case where the destination is a join, these are the fields from the right join input
         * @param adjustments the amount to adjust each field by
         */
        private RexInputConverter( RexBuilder rexBuilder, List<AlgDataTypeField> srcFields, List<AlgDataTypeField> destFields, List<AlgDataTypeField> leftDestFields, List<AlgDataTypeField> rightDestFields, int[] adjustments ) {
            this.rexBuilder = rexBuilder;
            this.srcFields = srcFields;
            this.destFields = destFields;
            this.adjustments = adjustments;
            this.leftDestFields = leftDestFields;
            this.rightDestFields = rightDestFields;
            if ( leftDestFields == null ) {
                nLeftDestFields = 0;
            } else {
                assert destFields == null;
                nLeftDestFields = leftDestFields.size();
            }
        }


        public RexInputConverter( RexBuilder rexBuilder, List<AlgDataTypeField> srcFields, List<AlgDataTypeField> leftDestFields, List<AlgDataTypeField> rightDestFields, int[] adjustments ) {
            this( rexBuilder, srcFields, null, leftDestFields, rightDestFields, adjustments );
        }


        public RexInputConverter( RexBuilder rexBuilder, List<AlgDataTypeField> srcFields, List<AlgDataTypeField> destFields, int[] adjustments ) {
            this( rexBuilder, srcFields, destFields, null, null, adjustments );
        }


        public RexInputConverter( RexBuilder rexBuilder, List<AlgDataTypeField> srcFields, int[] adjustments ) {
            this( rexBuilder, srcFields, null, null, null, adjustments );
        }


        @Override
        public RexNode visitIndexRef( RexIndexRef var ) {
            int srcIndex = var.getIndex();
            int destIndex = srcIndex + adjustments[srcIndex];

            AlgDataType type;
            if ( destFields != null ) {
                type = destFields.get( destIndex ).getType();
            } else if ( leftDestFields != null ) {
                if ( destIndex < nLeftDestFields ) {
                    type = leftDestFields.get( destIndex ).getType();
                } else {
                    type = rightDestFields.get( destIndex - nLeftDestFields ).getType();
                }
            } else {
                type = srcFields.get( srcIndex ).getType();
            }
            if ( (adjustments[srcIndex] != 0) || (srcFields == null) || (type != srcFields.get( srcIndex ).getType()) ) {
                return rexBuilder.makeInputRef( type, destIndex );
            } else {
                return var;
            }
        }

    }


    /**
     * What kind of sub-query.
     */
    public enum SubQueryType {
        EXISTS,
        IN,
        SCALAR
    }


    /**
     * Categorizes whether a bit set contains bits left and right of a line.
     */
    enum Side {
        LEFT, RIGHT, BOTH, EMPTY;


        static Side of( ImmutableBitSet bitSet, int middle ) {
            final int firstBit = bitSet.nextSetBit( 0 );
            if ( firstBit < 0 ) {
                return EMPTY;
            }
            if ( firstBit >= middle ) {
                return RIGHT;
            }
            if ( bitSet.nextSetBit( middle ) < 0 ) {
                return LEFT;
            }
            return BOTH;
        }
    }


    /**
     * Shuttle that finds correlation variables inside a given relational expression, including those that are inside {@link RexSubQuery sub-queries}.
     */
    private static class CorrelationCollector extends AlgHomogeneousShuttle {

        private final VariableUsedVisitor vuv = new VariableUsedVisitor( this );


        @Override
        public AlgNode visit( AlgNode other ) {
            other.collectVariablesUsed( vuv.variables );
            other.accept( vuv );
            AlgNode result = super.visit( other );
            // Important! Remove stopped variables AFTER we visit children. (which what super.visit() does)
            vuv.variables.removeAll( other.getVariablesSet() );
            return result;
        }

    }


    /**
     * Result of calling {@link AlgOptUtil#createExistsPlan}
     */
    public static class Exists {

        public final AlgNode r;
        public final boolean indicator;
        public final boolean outerJoin;


        private Exists( AlgNode r, boolean indicator, boolean outerJoin ) {
            this.r = r;
            this.indicator = indicator;
            this.outerJoin = outerJoin;
        }

    }

}

