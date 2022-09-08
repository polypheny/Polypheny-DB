/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.tools;


import static org.polypheny.db.util.Static.RESOURCE;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.math.BigDecimal;
import java.util.AbstractList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Experimental;
import org.bson.BsonValue;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgDistribution;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.SemiJoinType;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.AlgFactories.ScanFactory;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Intersect;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.Match;
import org.polypheny.db.algebra.core.Minus;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Scan;
import org.polypheny.db.algebra.core.SemiJoin;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.logical.lpg.LogicalGraph;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgMatch;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgProject;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgScan;
import org.polypheny.db.algebra.logical.relational.LogicalFilter;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptPredicateList;
import org.polypheny.db.plan.AlgOptSchema;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.Context;
import org.polypheny.db.plan.Contexts;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexCorrelVariable;
import org.polypheny.db.rex.RexExecutor;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.rex.RexSimplify;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.runtime.Hook;
import org.polypheny.db.runtime.PolyCollections.PolyDictionary;
import org.polypheny.db.schema.ModelTrait;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.graph.PolyNode;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.Holder;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.ImmutableIntList;
import org.polypheny.db.util.ImmutableNullableList;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.NlsString;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.TimeString;
import org.polypheny.db.util.TimestampString;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.ValidatorUtil;
import org.polypheny.db.util.mapping.Mapping;
import org.polypheny.db.util.mapping.Mappings;


/**
 * Builder for relational expressions.
 * <p>
 * {@code AlgBuilder} does not make possible anything that you could not also accomplish by calling the factory methods of
 * the particular relational expression. But it makes common tasks more straightforward and concise.
 * <p>
 * {@code AlgBuilder} uses factories to create relational expressions.
 * By default, it uses the default factories, which create logical relational expressions ({@link LogicalFilter},
 * {@link LogicalProject} and so forth). But you could override those factories so that, say, {@code filter} creates
 * instead a {@code HiveFilter}.
 * <p>
 * It is not thread-safe.
 */
public class AlgBuilder {

    @Getter
    protected final AlgOptCluster cluster;
    protected final AlgOptSchema algOptSchema;
    private final AlgFactories.FilterFactory filterFactory;
    private final AlgFactories.ProjectFactory projectFactory;
    private final AlgFactories.AggregateFactory aggregateFactory;
    private final AlgFactories.SortFactory sortFactory;
    private final AlgFactories.ExchangeFactory exchangeFactory;
    private final AlgFactories.SortExchangeFactory sortExchangeFactory;
    private final AlgFactories.SetOpFactory setOpFactory;
    private final AlgFactories.JoinFactory joinFactory;
    private final AlgFactories.SemiJoinFactory semiJoinFactory;
    private final AlgFactories.CorrelateFactory correlateFactory;
    private final AlgFactories.ValuesFactory valuesFactory;
    private final ScanFactory scanFactory;
    private final AlgFactories.MatchFactory matchFactory;
    private final AlgFactories.DocumentsFactory documentsFactory;
    private final Deque<Frame> stack = new ArrayDeque<>();
    private final boolean simplify;
    private final RexSimplify simplifier;


    protected AlgBuilder( Context context, AlgOptCluster cluster, AlgOptSchema algOptSchema ) {
        this.cluster = cluster;
        this.algOptSchema = algOptSchema;
        if ( context == null ) {
            context = Contexts.EMPTY_CONTEXT;
        }
        this.simplify = Hook.REL_BUILDER_SIMPLIFY.get( true );
        this.aggregateFactory =
                Util.first(
                        context.unwrap( AlgFactories.AggregateFactory.class ),
                        AlgFactories.DEFAULT_AGGREGATE_FACTORY );
        this.filterFactory =
                Util.first(
                        context.unwrap( AlgFactories.FilterFactory.class ),
                        AlgFactories.DEFAULT_FILTER_FACTORY );
        this.projectFactory =
                Util.first(
                        context.unwrap( AlgFactories.ProjectFactory.class ),
                        AlgFactories.DEFAULT_PROJECT_FACTORY );
        this.sortFactory =
                Util.first(
                        context.unwrap( AlgFactories.SortFactory.class ),
                        AlgFactories.DEFAULT_SORT_FACTORY );
        this.exchangeFactory =
                Util.first(
                        context.unwrap( AlgFactories.ExchangeFactory.class ),
                        AlgFactories.DEFAULT_EXCHANGE_FACTORY );
        this.sortExchangeFactory =
                Util.first(
                        context.unwrap( AlgFactories.SortExchangeFactory.class ),
                        AlgFactories.DEFAULT_SORT_EXCHANGE_FACTORY );
        this.setOpFactory =
                Util.first(
                        context.unwrap( AlgFactories.SetOpFactory.class ),
                        AlgFactories.DEFAULT_SET_OP_FACTORY );
        this.joinFactory =
                Util.first(
                        context.unwrap( AlgFactories.JoinFactory.class ),
                        AlgFactories.DEFAULT_JOIN_FACTORY );
        this.semiJoinFactory =
                Util.first(
                        context.unwrap( AlgFactories.SemiJoinFactory.class ),
                        AlgFactories.DEFAULT_SEMI_JOIN_FACTORY );
        this.correlateFactory =
                Util.first(
                        context.unwrap( AlgFactories.CorrelateFactory.class ),
                        AlgFactories.DEFAULT_CORRELATE_FACTORY );
        this.valuesFactory =
                Util.first(
                        context.unwrap( AlgFactories.ValuesFactory.class ),
                        AlgFactories.DEFAULT_VALUES_FACTORY );
        this.scanFactory =
                Util.first(
                        context.unwrap( ScanFactory.class ),
                        AlgFactories.DEFAULT_TABLE_SCAN_FACTORY );
        this.matchFactory =
                Util.first(
                        context.unwrap( AlgFactories.MatchFactory.class ),
                        AlgFactories.DEFAULT_MATCH_FACTORY );
        this.documentsFactory =
                Util.first(
                        context.unwrap( AlgFactories.DocumentsFactory.class ),
                        AlgFactories.DEFAULT_DOCUMENTS_FACTORY );

        final RexExecutor executor =
                Util.first(
                        context.unwrap( RexExecutor.class ),
                        Util.first(
                                cluster.getPlanner().getExecutor(),
                                RexUtil.EXECUTOR ) );
        final AlgOptPredicateList predicates = AlgOptPredicateList.EMPTY;
        this.simplifier = new RexSimplify( cluster.getRexBuilder(), predicates, executor );

    }


    /**
     * @return the stack size of the current builder.
     */
    public int stackSize() {
        return this.stack.size();
    }


    /**
     * Creates a AlgBuilder.
     */
    public static AlgBuilder create( FrameworkConfig config ) {
        final AlgOptCluster[] clusters = { null };
        final AlgOptSchema[] algOptSchemas = { null };
        Frameworks.withPrepare(
                new Frameworks.PrepareAction<Void>( config ) {
                    @Override
                    public Void apply( AlgOptCluster cluster, AlgOptSchema algOptSchema, SchemaPlus rootSchema ) {
                        clusters[0] = cluster;
                        algOptSchemas[0] = algOptSchema;
                        return null;
                    }
                } );
        return new AlgBuilder( config.getContext(), clusters[0], algOptSchemas[0] );
    }


    public static AlgBuilder create( Statement statement ) {
        final RexBuilder rexBuilder = new RexBuilder( statement.getTransaction().getTypeFactory() );
        final AlgOptCluster cluster = AlgOptCluster.create( statement.getQueryProcessor().getPlanner(), rexBuilder );
        return create( statement, cluster );
    }


    public static AlgBuilder create( Statement statement, AlgOptCluster cluster ) {
        return new AlgBuilder( Contexts.EMPTY_CONTEXT, cluster, statement.getTransaction().getCatalogReader() );
    }


    /**
     * Converts this{@link AlgBuilder}  to a string. The string is the string representation of all of the RelNodes on the stack.
     */
    @Override
    public String toString() {
        return stack.stream()
                .map( frame -> AlgOptUtil.toString( frame.alg ) )
                .collect( Collectors.joining( "" ) );
    }


    /**
     * Returns the type factory.
     */
    public AlgDataTypeFactory getTypeFactory() {
        return cluster.getTypeFactory();
    }


    /**
     * Returns the builder for {@link RexNode} expressions.
     */
    public RexBuilder getRexBuilder() {
        return cluster.getRexBuilder();
    }


    /**
     * Creates a {@link AlgBuilderFactory}, a partially-created AlgBuilder.
     * Just add a {@link AlgOptCluster} and a {@link AlgOptSchema}
     */
    public static AlgBuilderFactory proto( final Context context ) {
        return ( cluster, schema ) -> new AlgBuilder( context, cluster, schema );
    }


    /**
     * Creates a {@link AlgBuilderFactory} that uses a given set of factories.
     */
    public static AlgBuilderFactory proto( Object... factories ) {
        return proto( Contexts.of( factories ) );
    }

    // Methods for manipulating the stack


    /**
     * Adds a relational expression to be the input to the next relational expression constructed.
     *
     * This method is usual when you want to weave in relational expressions that are not supported by the builder. If, while
     * creating such expressions, you need to use previously built expressions as inputs, call
     * {@link #build()} to pop those inputs.
     */
    public AlgBuilder push( AlgNode node ) {
        stack.push( new Frame( node ) );
        return this;
    }


    /**
     * Adds a alg node to the top of the stack while preserving the field names and aliases.
     */
    public void replaceTop( AlgNode node ) {
        final Frame frame = stack.pop();
        stack.push( new Frame( node, frame.fields ) );
    }


    /**
     * Adds an alg node to the top of the stack while preserving the field names and aliases.
     */
    public void replaceTop( AlgNode node, int amount ) {
        final Frame frame = stack.pop();
        for ( int i = 0; i < amount - 1; i++ ) {
            stack.pop();
        }
        stack.push( new Frame( node, frame.fields ) );
    }


    /**
     * Pushes a collection of relational expressions.
     */
    public AlgBuilder pushAll( Iterable<? extends AlgNode> nodes ) {
        for ( AlgNode node : nodes ) {
            push( node );
        }
        return this;
    }


    /**
     * Returns the final relational expression.
     *
     * Throws if the stack is empty.
     */
    public AlgNode build() {
        return stack.pop().alg;
    }


    /**
     * Returns the relational expression at the top of the stack, but does not remove it.
     */
    public AlgNode peek() {
        return peek_().alg;
    }


    private Frame peek_() {
        return stack.peek();
    }


    /**
     * Returns the relational expression {@code n} positions from the top of the stack, but does not remove it.
     */
    public AlgNode peek( int n ) {
        return peek_( n ).alg;
    }


    private Frame peek_( int n ) {
        return Iterables.get( stack, n );
    }


    /**
     * Returns the relational expression {@code n} positions from the top of the stack, but does not remove it.
     */
    public AlgNode peek( int inputCount, int inputOrdinal ) {
        return peek_( inputCount, inputOrdinal ).alg;
    }


    private Frame peek_( int inputCount, int inputOrdinal ) {
        return peek_( inputCount - 1 - inputOrdinal );
    }


    /**
     * Returns the number of fields in all inputs before (to the left of) the given input.
     *
     * @param inputCount Number of inputs
     * @param inputOrdinal Input ordinal
     */
    private int inputOffset( int inputCount, int inputOrdinal ) {
        int offset = 0;
        for ( int i = 0; i < inputOrdinal; i++ ) {
            offset += peek( inputCount, i ).getRowType().getFieldCount();
        }
        return offset;
    }

    // Methods that return scalar expressions


    /**
     * Creates a literal (constant expression).
     */
    public RexNode literal( Object value ) {
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        if ( value == null ) {
            return rexBuilder.constantNull();
        } else if ( value instanceof Boolean ) {
            return rexBuilder.makeLiteral( (Boolean) value );
        } else if ( value instanceof BigDecimal ) {
            return rexBuilder.makeExactLiteral( (BigDecimal) value );
        } else if ( value instanceof Float || value instanceof Double ) {
            return rexBuilder.makeApproxLiteral( BigDecimal.valueOf( ((Number) value).doubleValue() ) );
        } else if ( value instanceof Number ) {
            return rexBuilder.makeExactLiteral( BigDecimal.valueOf( ((Number) value).longValue() ) );
        } else if ( value instanceof String ) {
            return rexBuilder.makeLiteral( (String) value );
        } else if ( value instanceof byte[] ) {
            // multimedia stream
            return rexBuilder.makeFileLiteral( (byte[]) value );
        } else if ( value instanceof DateString ) {
            return rexBuilder.makeDateLiteral( (DateString) value );
        } else if ( value instanceof TimeString ) {
            return rexBuilder.makeTimeLiteral( (TimeString) value, value.toString().length() );
        } else if ( value instanceof TimestampString ) {
            return rexBuilder.makeTimestampLiteral( (TimestampString) value, value.toString().length() );
        } else {
            throw new IllegalArgumentException( "cannot convert " + value + " (" + value.getClass() + ") to a constant" );
        }
    }


    /**
     * Creates a correlation variable for the current input, and writes it into a Holder.
     */
    public AlgBuilder variable( Holder<RexCorrelVariable> v ) {
        v.set( (RexCorrelVariable) getRexBuilder().makeCorrel( peek().getRowType(), cluster.createCorrel() ) );
        return this;
    }


    /**
     * Creates a reference to a field by name.
     *
     * Equivalent to {@code field(1, 0, fieldName)}.
     *
     * @param fieldName Field name
     */
    public RexInputRef field( String fieldName ) {
        return field( 1, 0, fieldName );
    }


    /**
     * Creates a reference to a field of given input relational expression by name.
     *
     * @param inputCount Number of inputs
     * @param inputOrdinal Input ordinal
     * @param fieldName Field name
     */
    public RexInputRef field( int inputCount, int inputOrdinal, String fieldName ) {
        final Frame frame = peek_( inputCount, inputOrdinal );
        final List<String> fieldNames = Pair.left( frame.fields() );
        int i = fieldNames.indexOf( fieldName );
        if ( i >= 0 ) {
            return field( inputCount, inputOrdinal, i );
        } else {
            throw new IllegalArgumentException( "field [" + fieldName + "] not found; input fields are: " + fieldNames );
        }
    }


    /**
     * Creates a reference to an input field by ordinal.
     *
     * Equivalent to {@code field(1, 0, ordinal)}.
     *
     * @param fieldOrdinal Field ordinal
     */
    public RexInputRef field( int fieldOrdinal ) {
        return (RexInputRef) field( 1, 0, fieldOrdinal, false );
    }


    /**
     * Creates a reference to a field of a given input relational expression by ordinal.
     *
     * @param inputCount Number of inputs
     * @param inputOrdinal Input ordinal
     * @param fieldOrdinal Field ordinal within input
     */
    public RexInputRef field( int inputCount, int inputOrdinal, int fieldOrdinal ) {
        return (RexInputRef) field( inputCount, inputOrdinal, fieldOrdinal, false );
    }


    /**
     * As {@link #field(int, int, int)}, but if {@code alias} is true, the method may apply an alias to make sure that the
     * field has the same name as in the input frame. If no alias is applied the expression is definitely a
     * {@link RexInputRef}.
     */
    private RexNode field( int inputCount, int inputOrdinal, int fieldOrdinal, boolean alias ) {
        final Frame frame = peek_( inputCount, inputOrdinal );
        final AlgNode input = frame.alg;
        final AlgDataType rowType = input.getRowType();
        if ( fieldOrdinal < 0 || fieldOrdinal > rowType.getFieldCount() ) {
            throw new IllegalArgumentException( "field ordinal [" + fieldOrdinal + "] out of range; input fields are: " + rowType.getFieldNames() );
        }
        final AlgDataTypeField field = rowType.getFieldList().get( fieldOrdinal );
        final int offset = inputOffset( inputCount, inputOrdinal );
        final RexInputRef ref = cluster.getRexBuilder().makeInputRef( field.getType(), offset + fieldOrdinal );
        final AlgDataTypeField aliasField = frame.fields().get( fieldOrdinal );
        if ( !alias || field.getName().equals( aliasField.getName() ) ) {
            return ref;
        } else {
            return alias( ref, aliasField.getName() );
        }
    }


    /**
     * Creates a reference to a field of the current record which originated in a relation with a given alias.
     */
    public RexNode field( String alias, String fieldName ) {
        return field( 1, alias, fieldName );
    }


    /**
     * Creates a reference to a field which originated in a relation with the given alias. Searches for the relation starting
     * at the top of the stack.
     */
    public RexNode field( int inputCount, String alias, String fieldName ) {
        Objects.requireNonNull( alias );
        Objects.requireNonNull( fieldName );
        final List<String> fields = new ArrayList<>();
        for ( int inputOrdinal = 0; inputOrdinal < inputCount; ++inputOrdinal ) {
            final Frame frame = peek_( inputOrdinal );
            for ( Ord<Field> p : Ord.zip( frame.fields ) ) {
                // If alias and field name match, reference that field.
                if ( p.e.left.contains( alias ) && p.e.right.getName().equals( fieldName ) ) {
                    return field( inputCount, inputCount - 1 - inputOrdinal, p.i );
                }
                fields.add( String.format( Locale.ROOT, "{aliases=%s,fieldName=%s}", p.e.left, p.e.right.getName() ) );
            }
        }
        throw new IllegalArgumentException( "no aliased field found; fields are: " + fields );
    }


    /**
     * Returns a reference to a given field of a record-valued expression.
     */
    public RexNode field( RexNode e, String name ) {
        return getRexBuilder().makeFieldAccess( e, name, false );
    }


    /**
     * Returns references to the fields of the top input.
     */
    public ImmutableList<RexNode> fields() {
        return fields( 1, 0 );
    }


    /**
     * Returns references to the fields of a given input.
     */
    public ImmutableList<RexNode> fields( int inputCount, int inputOrdinal ) {
        final AlgNode input = peek( inputCount, inputOrdinal );
        final AlgDataType rowType = input.getRowType();
        final ImmutableList.Builder<RexNode> nodes = ImmutableList.builder();
        for ( int fieldOrdinal : Util.range( rowType.getFieldCount() ) ) {
            nodes.add( field( inputCount, inputOrdinal, fieldOrdinal ) );
        }
        return nodes.build();
    }


    /**
     * Returns references to fields for a given collation.
     */
    public ImmutableList<RexNode> fields( AlgCollation collation ) {
        final ImmutableList.Builder<RexNode> nodes = ImmutableList.builder();
        for ( AlgFieldCollation fieldCollation : collation.getFieldCollations() ) {
            RexNode node = field( fieldCollation.getFieldIndex() );
            switch ( fieldCollation.direction ) {
                case DESCENDING:
                    node = desc( node );
            }
            switch ( fieldCollation.nullDirection ) {
                case FIRST:
                    node = nullsFirst( node );
                    break;
                case LAST:
                    node = nullsLast( node );
                    break;
            }
            nodes.add( node );
        }
        return nodes.build();
    }


    /**
     * Returns references to fields for a given list of input ordinals.
     */
    public ImmutableList<RexNode> fields( List<? extends Number> ordinals ) {
        final ImmutableList.Builder<RexNode> nodes = ImmutableList.builder();
        for ( Number ordinal : ordinals ) {
            RexNode node = field( 1, 0, ordinal.intValue(), false );
            nodes.add( node );
        }
        return nodes.build();
    }


    /**
     * Returns references to fields identified by name.
     */
    public ImmutableList<RexNode> fields( Iterable<String> fieldNames ) {
        final ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
        for ( String fieldName : fieldNames ) {
            builder.add( field( fieldName ) );
        }
        return builder.build();
    }


    /**
     * Returns references to fields identified by a mapping.
     */
    public ImmutableList<RexNode> fields( Mappings.TargetMapping mapping ) {
        return fields( Mappings.asList( mapping ) );
    }


    /**
     * Creates an access to a field by name.
     */
    public RexNode dot( RexNode node, String fieldName ) {
        final RexBuilder builder = cluster.getRexBuilder();
        return builder.makeFieldAccess( node, fieldName, true );
    }


    /**
     * Creates an access to a field by ordinal.
     */
    public RexNode dot( RexNode node, int fieldOrdinal ) {
        final RexBuilder builder = cluster.getRexBuilder();
        return builder.makeFieldAccess( node, fieldOrdinal );
    }


    /**
     * Creates a call to a scalar operator.
     */
    public RexNode call( Operator operator, RexNode... operands ) {
        return call( operator, ImmutableList.copyOf( operands ) );
    }


    /**
     * Creates a call to a scalar operator.
     */
    private RexNode call( Operator operator, List<RexNode> operandList ) {
        final RexBuilder builder = cluster.getRexBuilder();
        final AlgDataType type = builder.deriveReturnType( operator, operandList );
        return builder.makeCall( type, operator, operandList );
    }


    /**
     * Creates a call to a scalar operator.
     */
    public RexNode call( Operator operator, Iterable<? extends RexNode> operands ) {
        return call( operator, ImmutableList.copyOf( operands ) );
    }


    /**
     * Creates an AND.
     */
    public RexNode and( RexNode... operands ) {
        return and( ImmutableList.copyOf( operands ) );
    }


    /**
     * Creates an AND.
     *
     * Simplifies the expression a little:
     * {@code e AND TRUE} becomes {@code e};
     * {@code e AND e2 AND NOT e} becomes {@code e2}.
     */
    public RexNode and( Iterable<? extends RexNode> operands ) {
        return simplifier.simplifyAnds( operands );
    }


    /**
     * Creates an OR.
     */
    public RexNode or( RexNode... operands ) {
        return or( ImmutableList.copyOf( operands ) );
    }


    /**
     * Creates an OR.
     */
    public RexNode or( Iterable<? extends RexNode> operands ) {
        return RexUtil.composeDisjunction( cluster.getRexBuilder(), operands );
    }


    /**
     * Creates a NOT.
     */
    public RexNode not( RexNode operand ) {
        return call( OperatorRegistry.get( OperatorName.NOT ), operand );
    }


    /**
     * Creates an {@code =}.
     */
    public RexNode equals( RexNode operand0, RexNode operand1 ) {
        return call( OperatorRegistry.get( OperatorName.EQUALS ), operand0, operand1 );
    }


    /**
     * Creates a {@code <>}.
     */
    public RexNode notEquals( RexNode operand0, RexNode operand1 ) {
        return call( OperatorRegistry.get( OperatorName.NOT_EQUALS ), operand0, operand1 );
    }


    /**
     * Creates a IS NULL.
     */
    public RexNode isNull( RexNode operand ) {
        return call( OperatorRegistry.get( OperatorName.IS_NULL ), operand );
    }


    /**
     * Creates a IS NOT NULL.
     */
    public RexNode isNotNull( RexNode operand ) {
        return call( OperatorRegistry.get( OperatorName.IS_NOT_NULL ), operand );
    }


    /**
     * Creates an expression that casts an expression to a given type.
     */
    public RexNode cast( RexNode expr, PolyType typeName ) {
        final AlgDataType type = cluster.getTypeFactory().createPolyType( typeName );
        return cluster.getRexBuilder().makeCast( type, expr );
    }


    /**
     * Creates an expression that casts an expression to a type with a given name and precision or length.
     */
    public RexNode cast( RexNode expr, PolyType typeName, int precision ) {
        final AlgDataType type = cluster.getTypeFactory().createPolyType( typeName, precision );
        return cluster.getRexBuilder().makeCast( type, expr );
    }


    /**
     * Creates an expression that casts an expression to a type with a given name, precision and scale.
     */
    public RexNode cast( RexNode expr, PolyType typeName, int precision, int scale ) {
        final AlgDataType type = cluster.getTypeFactory().createPolyType( typeName, precision, scale );
        return cluster.getRexBuilder().makeCast( type, expr );
    }


    /**
     * Returns an expression wrapped in an alias.
     *
     * @see #project
     */
    public RexNode alias( RexNode expr, String alias ) {
        return call( OperatorRegistry.get( OperatorName.AS ), expr, literal( alias ) );
    }


    /**
     * Converts a sort expression to descending.
     */
    public RexNode desc( RexNode node ) {
        return call( OperatorRegistry.get( OperatorName.DESC ), node );
    }


    /**
     * Converts a sort expression to nulls last.
     */
    public RexNode nullsLast( RexNode node ) {
        return call( OperatorRegistry.get( OperatorName.NULLS_LAST ), node );
    }


    /**
     * Converts a sort expression to nulls first.
     */
    public RexNode nullsFirst( RexNode node ) {
        return call( OperatorRegistry.get( OperatorName.NULLS_FIRST ), node );
    }

    // Methods that create group keys and aggregate calls


    /**
     * Creates an empty group key.
     */
    public GroupKey groupKey() {
        return groupKey( ImmutableList.of() );
    }


    /**
     * Creates a group key.
     */
    public GroupKey groupKey( RexNode... nodes ) {
        return groupKey( ImmutableList.copyOf( nodes ) );
    }


    /**
     * Creates a group key.
     */
    public GroupKey groupKey( Iterable<? extends RexNode> nodes ) {
        return new GroupKeyImpl( ImmutableList.copyOf( nodes ), false, null, null );
    }


    /**
     * Creates a group key with grouping sets.
     */
    public GroupKey groupKey( Iterable<? extends RexNode> nodes, Iterable<? extends Iterable<? extends RexNode>> nodeLists ) {
        return groupKey_( nodes, false, nodeLists );
    }


    /**
     * @deprecated Now that indicator is deprecated, use {@link #groupKey(Iterable, Iterable)}, which has the same behavior
     * as calling this method with {@code indicator = false}.
     */
    @Deprecated // to be removed before 2.0
    public GroupKey groupKey( Iterable<? extends RexNode> nodes, boolean indicator, Iterable<? extends Iterable<? extends RexNode>> nodeLists ) {
        return groupKey_( nodes, indicator, nodeLists );
    }


    private GroupKey groupKey_( Iterable<? extends RexNode> nodes, boolean indicator, Iterable<? extends Iterable<? extends RexNode>> nodeLists ) {
        final ImmutableList.Builder<ImmutableList<RexNode>> builder = ImmutableList.builder();
        for ( Iterable<? extends RexNode> nodeList : nodeLists ) {
            builder.add( ImmutableList.copyOf( nodeList ) );
        }
        return new GroupKeyImpl( ImmutableList.copyOf( nodes ), indicator, builder.build(), null );
    }


    /**
     * Creates a group key of fields identified by ordinal.
     */
    public GroupKey groupKey( int... fieldOrdinals ) {
        return groupKey( fields( ImmutableIntList.of( fieldOrdinals ) ) );
    }


    /**
     * Creates a group key of fields identified by name.
     */
    public GroupKey groupKey( String... fieldNames ) {
        return groupKey( fields( ImmutableList.copyOf( fieldNames ) ) );
    }


    /**
     * Creates a group key, identified by field positions in the underlying relational expression.
     *
     * This method of creating a group key does not allow you to group on new expressions, only column projections, but is
     * efficient, especially when you are coming from an existing {@link Aggregate}.
     */
    public GroupKey groupKey( @Nonnull ImmutableBitSet groupSet ) {
        return groupKey( groupSet, ImmutableList.of( groupSet ) );
    }


    /**
     * Creates a group key with grouping sets, both identified by field positions in the underlying relational expression.
     *
     * This method of creating a group key does not allow you to group on new expressions, only column projections, but is
     * efficient, especially when you are coming from an existing {@link Aggregate}.
     */
    public GroupKey groupKey( ImmutableBitSet groupSet, @Nonnull Iterable<? extends ImmutableBitSet> groupSets ) {
        return groupKey_( groupSet, false, ImmutableList.copyOf( groupSets ) );
    }


    /**
     * As {@link #groupKey(ImmutableBitSet, Iterable)}.
     */
    // deprecated, to be removed before 2.0
    public GroupKey groupKey( ImmutableBitSet groupSet, ImmutableList<ImmutableBitSet> groupSets ) {
        return groupKey_(
                groupSet,
                false,
                groupSets == null
                        ? ImmutableList.of( groupSet )
                        : ImmutableList.copyOf( groupSets ) );
    }


    /**
     * @deprecated Use {@link #groupKey(ImmutableBitSet, Iterable)}.
     */
    @Deprecated // to be removed before 2.0
    public GroupKey groupKey( ImmutableBitSet groupSet, boolean indicator, ImmutableList<ImmutableBitSet> groupSets ) {
        return groupKey_(
                groupSet,
                indicator,
                groupSets == null
                        ? ImmutableList.of( groupSet )
                        : ImmutableList.copyOf( groupSets ) );
    }


    private GroupKey groupKey_( ImmutableBitSet groupSet, boolean indicator, @Nonnull ImmutableList<ImmutableBitSet> groupSets ) {
        if ( groupSet.length() > peek().getRowType().getFieldCount() ) {
            throw new IllegalArgumentException( "out of bounds: " + groupSet );
        }
        Objects.requireNonNull( groupSets );
        final ImmutableList<RexNode> nodes = fields( ImmutableIntList.of( groupSet.toArray() ) );
        final List<ImmutableList<RexNode>> nodeLists = Util.transform( groupSets, bitSet -> fields( ImmutableIntList.of( bitSet.toArray() ) ) );
        return groupKey_( nodes, indicator, nodeLists );
    }


    /**
     * Creates a call to an aggregate function.
     *
     * To add other operands, apply
     * {@link AggCall#distinct()},
     * {@link AggCall#approximate(boolean)},
     * {@link AggCall#filter(RexNode...)},
     * {@link AggCall#sort},
     * {@link AggCall#as} to the result.
     */
    public AggCall aggregateCall( AggFunction aggFunction, Iterable<? extends RexNode> operands ) {
        return aggregateCall(
                aggFunction,
                false,
                false,
                null,
                ImmutableList.of(),
                null,
                ImmutableList.copyOf( operands ) );
    }


    /**
     * Creates a call to an aggregate function.
     *
     * To add other operands, apply
     * {@link AggCall#distinct()},
     * {@link AggCall#approximate(boolean)},
     * {@link AggCall#filter(RexNode...)},
     * {@link AggCall#sort},
     * {@link AggCall#as} to the result.
     */
    public AggCall aggregateCall( AggFunction aggFunction, RexNode... operands ) {
        return aggregateCall(
                aggFunction,
                false,
                false,
                null,
                ImmutableList.of(),
                null,
                ImmutableList.copyOf( operands ) );
    }


    /**
     * Creates a call to an aggregate function with all applicable operands.
     */
    protected AggCall aggregateCall(
            AggFunction aggFunction,
            boolean distinct,
            boolean approximate,
            RexNode filter,
            ImmutableList<RexNode> orderKeys,
            String alias,
            ImmutableList<RexNode> operands ) {
        return new AggCallImpl(
                aggFunction,
                distinct,
                approximate,
                filter,
                alias,
                operands,
                orderKeys );
    }


    /**
     * Creates a call to the {@code COUNT} aggregate function.
     */
    public AggCall count( RexNode... operands ) {
        return count( false, null, operands );
    }


    /**
     * Creates a call to the {@code COUNT} aggregate function.
     */
    public AggCall count( Iterable<? extends RexNode> operands ) {
        return count( false, null, operands );
    }


    /**
     * Creates a call to the {@code COUNT} aggregate function, optionally distinct and with an alias.
     */
    public AggCall count( boolean distinct, String alias, RexNode... operands ) {
        return aggregateCall(
                OperatorRegistry.getAgg( OperatorName.COUNT ),
                distinct,
                false,
                null,
                ImmutableList.of(),
                alias,
                ImmutableList.copyOf( operands ) );
    }


    /**
     * Creates a call to the {@code COUNT} aggregate function, optionally distinct and with an alias.
     */
    public AggCall count( boolean distinct, String alias, Iterable<? extends RexNode> operands ) {
        return aggregateCall(
                OperatorRegistry.getAgg( OperatorName.COUNT ),
                distinct,
                false,
                null,
                ImmutableList.of(),
                alias,
                ImmutableList.copyOf( operands ) );
    }


    /**
     * Creates a call to the {@code COUNT(*)} aggregate function.
     */
    public AggCall countStar( String alias ) {
        return count( false, alias );
    }


    /**
     * Creates a call to the {@code SUM} aggregate function.
     */
    public AggCall sum( RexNode operand ) {
        return sum( false, null, operand );
    }


    /**
     * Creates a call to the {@code SUM} aggregate function, optionally distinct and with an alias.
     */
    public AggCall sum( boolean distinct, String alias, RexNode operand ) {
        return aggregateCall(
                OperatorRegistry.getAgg( OperatorName.SUM ),
                distinct,
                false,
                null,
                ImmutableList.of(),
                alias,
                ImmutableList.of( operand ) );
    }


    /**
     * Creates a call to the {@code AVG} aggregate function.
     */
    public AggCall avg( RexNode operand ) {
        return avg( false, null, operand );
    }


    /**
     * Creates a call to the {@code AVG} aggregate function, optionally distinct and with an alias.
     */
    public AggCall avg( boolean distinct, String alias, RexNode operand ) {
        return aggregateCall(
                OperatorRegistry.getAgg( OperatorName.AVG ),
                distinct,
                false,
                null,
                ImmutableList.of(),
                alias,
                ImmutableList.of( operand ) );
    }


    /**
     * Creates a call to the {@code MIN} aggregate function.
     */
    public AggCall min( RexNode operand ) {
        return min( null, operand );
    }


    /**
     * Creates a call to the {@code MIN} aggregate function, optionally with an alias.
     */
    public AggCall min( String alias, RexNode operand ) {
        return aggregateCall(
                OperatorRegistry.getAgg( OperatorName.MIN ),
                false,
                false,
                null,
                ImmutableList.of(),
                alias,
                ImmutableList.of( operand ) );
    }


    /**
     * Creates a call to the {@code MAX} aggregate function, optionally with an alias.
     */
    public AggCall max( RexNode operand ) {
        return max( null, operand );
    }


    /**
     * Creates a call to the {@code MAX} aggregate function.
     */
    public AggCall max( String alias, RexNode operand ) {
        return aggregateCall(
                OperatorRegistry.getAgg( OperatorName.MAX ),
                false,
                false,
                null,
                ImmutableList.of(),
                alias,
                ImmutableList.of( operand ) );
    }

    // Methods for patterns


    /**
     * Creates a reference to a given field of the pattern.
     *
     * @param alpha the pattern name
     * @param type Type of field
     * @param i Ordinal of field
     * @return Reference to field of pattern
     */
    public RexNode patternField( String alpha, AlgDataType type, int i ) {
        return getRexBuilder().makePatternFieldRef( alpha, type, i );
    }


    /**
     * Creates a call that concatenates patterns; for use in {@link #match}.
     */
    public RexNode patternConcat( Iterable<? extends RexNode> nodes ) {
        final ImmutableList<RexNode> list = ImmutableList.copyOf( nodes );
        if ( list.size() > 2 ) {
            // Convert into binary calls
            return patternConcat( patternConcat( Util.skipLast( list ) ), Util.last( list ) );
        }
        final AlgDataType t = getTypeFactory().createPolyType( PolyType.NULL );
        return getRexBuilder().makeCall( t, OperatorRegistry.get( OperatorName.PATTERN_CONCAT ), list );
    }


    /**
     * Creates a call that concatenates patterns; for use in {@link #match}.
     */
    public RexNode patternConcat( RexNode... nodes ) {
        return patternConcat( ImmutableList.copyOf( nodes ) );
    }


    /**
     * Creates a call that creates alternate patterns; for use in {@link #match}.
     */
    public RexNode patternAlter( Iterable<? extends RexNode> nodes ) {
        final AlgDataType t = getTypeFactory().createPolyType( PolyType.NULL );
        return getRexBuilder().makeCall( t, OperatorRegistry.get( OperatorName.PATTERN_ALTER ), ImmutableList.copyOf( nodes ) );
    }


    /**
     * Creates a call that creates alternate patterns; for use in {@link #match}.
     */
    public RexNode patternAlter( RexNode... nodes ) {
        return patternAlter( ImmutableList.copyOf( nodes ) );
    }


    /**
     * Creates a call that creates quantify patterns; for use in {@link #match}.
     */
    public RexNode patternQuantify( Iterable<? extends RexNode> nodes ) {
        final AlgDataType t = getTypeFactory().createPolyType( PolyType.NULL );
        return getRexBuilder().makeCall( t, OperatorRegistry.get( OperatorName.PATTERN_QUANTIFIER ), ImmutableList.copyOf( nodes ) );
    }


    /**
     * Creates a call that creates quantify patterns; for use in {@link #match}.
     */
    public RexNode patternQuantify( RexNode... nodes ) {
        return patternQuantify( ImmutableList.copyOf( nodes ) );
    }


    /**
     * Creates a call that creates permute patterns; for use in {@link #match}.
     */
    public RexNode patternPermute( Iterable<? extends RexNode> nodes ) {
        final AlgDataType t = getTypeFactory().createPolyType( PolyType.NULL );
        return getRexBuilder().makeCall( t, OperatorRegistry.get( OperatorName.PATTERN_PERMUTE ), ImmutableList.copyOf( nodes ) );
    }


    /**
     * Creates a call that creates permute patterns; for use in {@link #match}.
     */
    public RexNode patternPermute( RexNode... nodes ) {
        return patternPermute( ImmutableList.copyOf( nodes ) );
    }


    /**
     * Creates a call that creates an exclude pattern; for use in {@link #match}.
     */
    public RexNode patternExclude( RexNode node ) {
        final AlgDataType t = getTypeFactory().createPolyType( PolyType.NULL );
        return getRexBuilder().makeCall( t, OperatorRegistry.get( OperatorName.PATTERN_EXCLUDE ), ImmutableList.of( node ) );
    }

    // Methods that create relational expressions


    /**
     * Creates a {@link Scan} of the table with a given name.
     *
     * Throws if the table does not exist.
     *
     * Returns this builder.
     *
     * @param tableNames Name of table (can optionally be qualified)
     */
    public AlgBuilder scan( Iterable<String> tableNames ) {
        final List<String> names = ImmutableList.copyOf( tableNames );
        final AlgOptTable algOptTable = algOptSchema.getTableForMember( names );
        if ( algOptTable == null ) {
            throw RESOURCE.tableNotFound( String.join( ".", names ) ).ex();
        }
        final AlgNode scan = scanFactory.createScan( cluster, algOptTable );
        push( scan );
        rename( algOptTable.getRowType().getFieldNames() );
        return this;
    }


    public AlgBuilder scan( @Nonnull AlgOptTable algOptTable ) {
        final AlgNode scan = scanFactory.createScan( cluster, algOptTable );
        push( scan );
        rename( algOptTable.getRowType().getFieldNames() );
        return this;
    }


    /**
     * Creates a {@link Scan} of the table with a given name.
     *
     * Throws if the table does not exist.
     *
     * Returns this builder.
     *
     * @param tableNames Name of table (can optionally be qualified)
     */
    public AlgBuilder scan( String... tableNames ) {
        return scan( ImmutableList.copyOf( tableNames ) );
    }


    public AlgBuilder documentScan( AlgOptTable collection ) {
        stack.add( new Frame( new LogicalDocumentScan( cluster, cluster.traitSet().replace( ModelTrait.DOCUMENT ), collection ) ) );
        return this;
    }


    public AlgBuilder documentProject( List<? extends RexNode> projects, List<String> names ) {
        stack.add( new Frame( LogicalDocumentProject.create( build(), projects, names ) ) );
        return this;
    }


    public AlgBuilder lpgScan( long id ) {
        LogicalGraph graph = new LogicalGraph( id );
        stack.add( new Frame( new LogicalLpgScan( cluster, cluster.traitSet().replace( ModelTrait.GRAPH ), graph, graph.getRowType() ) ) );
        return this;
    }


    public AlgBuilder lpgMatch( List<RexCall> matches, List<String> names ) {
        stack.add( new Frame( new LogicalLpgMatch( cluster, cluster.traitSet().replace( ModelTrait.GRAPH ), build(), matches, names ) ) );
        return this;
    }


    public AlgBuilder lpgProject( List<? extends RexNode> projects, List<String> names ) {
        stack.add( new Frame( new LogicalLpgProject( cluster, cluster.traitSet().replace( ModelTrait.GRAPH ), build(), projects, names ) ) );
        return this;
    }


    public RexCall lpgNodeMatch( List<String> labels ) {
        RexBuilder rexBuilder = getRexBuilder();
        Operator op = OperatorRegistry.get( QueryLanguage.CYPHER, OperatorName.CYPHER_NODE_MATCH );
        AlgDataType nodeType = getTypeFactory().createPolyType( PolyType.NODE );
        return (RexCall) rexBuilder.makeCall( nodeType, op, List.of( rexBuilder.makeInputRef( peek().getRowType().getFieldList().get( 0 ).getType(), 0 ), new RexLiteral( new PolyNode( new PolyDictionary(), labels, null ), nodeType, PolyType.NODE ) ) );
    }


    /**
     * Creates a {@link Filter} of an array of predicates.
     *
     * The predicates are combined using AND, and optimized in a similar way to the {@link #and} method.
     * If the result is TRUE no filter is created.
     */
    public AlgBuilder filter( RexNode... predicates ) {
        return filter( ImmutableList.copyOf( predicates ) );
    }


    /**
     * Creates a {@link Filter} of a list of predicates.
     *
     * The predicates are combined using AND, and optimized in a similar way to the {@link #and} method.
     * If the result is TRUE no filter is created.
     */
    public AlgBuilder filter( Iterable<? extends RexNode> predicates ) {
        final RexNode simplifiedPredicates = simplifier.simplifyFilterPredicates( predicates );
        if ( simplifiedPredicates == null ) {
            return empty();
        }

        if ( !simplifiedPredicates.isAlwaysTrue() ) {
            final Frame frame = stack.pop();
            final AlgNode filter = filterFactory.createFilter( frame.alg, simplifiedPredicates );
            stack.push( new Frame( filter, frame.fields ) );
        }
        return this;
    }


    /**
     * Creates a {@link Project} of the given expressions.
     */
    public AlgBuilder project( RexNode... nodes ) {
        return project( ImmutableList.copyOf( nodes ) );
    }


    /**
     * Creates a {@link Project} of the given list of expressions.
     *
     * Infers names as would {@link #project(Iterable, Iterable)} if all suggested names were null.
     *
     * @param nodes Expressions
     */
    public AlgBuilder project( Iterable<? extends RexNode> nodes ) {
        return project( nodes, ImmutableList.of() );
    }


    /**
     * Creates a {@link Project} of the given list of expressions and field names.
     *
     * @param nodes Expressions
     * @param fieldNames field names for expressions
     */
    public AlgBuilder project( Iterable<? extends RexNode> nodes, Iterable<String> fieldNames ) {
        return project( nodes, fieldNames, false );
    }


    /**
     * Creates a {@link Project} of all original fields, plus the given expressions.
     */
    public AlgBuilder projectPlus( RexNode... nodes ) {
        return projectPlus( ImmutableList.copyOf( nodes ) );
    }


    /**
     * Creates a {@link Project} of all original fields, plus the given list of expressions.
     */
    public AlgBuilder projectPlus( Iterable<RexNode> nodes ) {
        final ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
        return project( builder.addAll( fields() ).addAll( nodes ).build() );
    }


    /**
     * Creates a {@link Project} of the given list of expressions, using the given names.
     *
     * Names are deduced as follows:
     * <ul>
     * <li>If the length of {@code fieldNames} is greater than the index of the current entry in {@code nodes}, and the entry in {@code fieldNames} is not null, uses it; otherwise</li>
     * <li>If an expression projects an input field, or is a cast an input field, uses the input field name; otherwise</li>
     * <li>If an expression is a call to {@link OperatorRegistry #AS} (see {@link #alias}), removes the call but uses the intended alias.</li>
     * </ul>
     *
     * After the field names have been inferred, makes the field names unique by appending numeric suffixes.
     *
     * @param nodes Expressions
     * @param fieldNames Suggested field names
     * @param force create project even if it is identity
     */
    public AlgBuilder project( Iterable<? extends RexNode> nodes, Iterable<String> fieldNames, boolean force ) {
        final Frame frame = stack.peek();
        final AlgDataType inputRowType = frame.alg.getRowType();
        final List<RexNode> nodeList = Lists.newArrayList( nodes );

        // Perform a quick check for identity. We'll do a deeper check later when we've derived column names.
        if ( !force && Iterables.isEmpty( fieldNames ) && RexUtil.isIdentity( nodeList, inputRowType ) ) {
            return this;
        }

        final List<String> fieldNameList = Lists.newArrayList( fieldNames );
        while ( fieldNameList.size() < nodeList.size() ) {
            fieldNameList.add( null );
        }

        if ( frame.alg instanceof Project && shouldMergeProject() ) {
            final Project project = (Project) frame.alg;
            // Populate field names. If the upper expression is an input ref and does not have a recommended name, use the
            // name of the underlying field.
            for ( int i = 0; i < fieldNameList.size(); i++ ) {
                if ( fieldNameList.get( i ) == null ) {
                    final RexNode node = nodeList.get( i );
                    if ( node instanceof RexInputRef ) {
                        final RexInputRef ref = (RexInputRef) node;
                        fieldNameList.set( i, project.getRowType().getFieldNames().get( ref.getIndex() ) );
                    }
                }
            }
            final List<RexNode> newNodes = AlgOptUtil.pushPastProject( nodeList, project );

            // Carefully build a list of fields, so that table aliases from the input can be seen for fields that are based on a RexInputRef.
            final Frame frame1 = stack.pop();
            final List<Field> fields = new ArrayList<>();
            for ( AlgDataTypeField f : project.getInput().getRowType().getFieldList() ) {
                fields.add( new Field( ImmutableSet.of(), f ) );
            }
            for ( Pair<RexNode, Field> pair : Pair.zip( project.getProjects(), frame1.fields ) ) {
                switch ( pair.left.getKind() ) {
                    case INPUT_REF:
                        final int i = ((RexInputRef) pair.left).getIndex();
                        final Field field = fields.get( i );
                        final ImmutableSet<String> aliases = pair.right.left;
                        fields.set( i, new Field( aliases, field.right ) );
                        break;
                }
            }
            stack.push( new Frame( project.getInput(), ImmutableList.copyOf( fields ) ) );
            return project( newNodes, fieldNameList, force );
        }

        // Simplify expressions.
        if ( simplify ) {
            for ( int i = 0; i < nodeList.size(); i++ ) {
                nodeList.set( i, simplifier.simplifyPreservingType( nodeList.get( i ) ) );
            }
        }

        // Replace null names with generated aliases.
        for ( int i = 0; i < fieldNameList.size(); i++ ) {
            if ( fieldNameList.get( i ) == null ) {
                fieldNameList.set( i, inferAlias( nodeList, nodeList.get( i ), i ) );
            }
        }

        final ImmutableList.Builder<Field> fields = ImmutableList.builder();
        final Set<String> uniqueNameList =
                getTypeFactory().getTypeSystem().isSchemaCaseSensitive()
                        ? new HashSet<>()
                        : new TreeSet<>( String.CASE_INSENSITIVE_ORDER );
        // calculate final names and build field list
        for ( int i = 0; i < fieldNameList.size(); ++i ) {
            final RexNode node = nodeList.get( i );
            String name = fieldNameList.get( i );
            Field field;
            if ( name == null || uniqueNameList.contains( name ) ) {
                int j = 0;
                if ( name == null ) {
                    j = i;
                }
                do {
                    name = ValidatorUtil.F_SUGGESTER.apply( name, j, j++ );
                } while ( uniqueNameList.contains( name ) );
                fieldNameList.set( i, name );
            }
            AlgDataTypeField fieldType = new AlgDataTypeFieldImpl( name, i, node.getType() );
            switch ( node.getKind() ) {
                case INPUT_REF:
                    // preserve alg aliases for INPUT_REF fields
                    final int index = ((RexInputRef) node).getIndex();
                    field = new Field( frame.fields.get( index ).left, fieldType );
                    break;
                default:
                    field = new Field( ImmutableSet.of(), fieldType );
                    break;
            }
            uniqueNameList.add( name );
            fields.add( field );
        }
        if ( !force && RexUtil.isIdentity( nodeList, inputRowType ) ) {
            if ( fieldNameList.equals( inputRowType.getFieldNames() ) ) {
                // Do not create an identity project if it does not rename any fields
                return this;
            } else {
                // create "virtual" row type for project only rename fields
                stack.pop();
                stack.push( new Frame( frame.alg, fields.build() ) );
                return this;
            }
        }
        final AlgNode project = projectFactory.createProject( frame.alg, ImmutableList.copyOf( nodeList ), fieldNameList );
        stack.pop();
        stack.push( new Frame( project, fields.build() ) );
        return this;
    }


    /**
     * Whether to attempt to merge consecutive {@link Project} operators.
     *
     * The default implementation returns {@code true}; sub-classes may disable merge by overriding to return {@code false}.
     */
    @Experimental
    protected boolean shouldMergeProject() {
        return true;
    }


    /**
     * Creates a {@link Project} of the given expressions and field names, and optionally optimizing.
     *
     * If {@code fieldNames} is null, or if a particular entry in {@code fieldNames} is null, derives field names from
     * the input expressions.
     *
     * If {@code force} is false, and the input is a {@code Project}, and the expressions  make the trivial
     * projection ($0, $1, ...), modifies the input.
     *
     * @param nodes Expressions
     * @param fieldNames Suggested field names, or null to generate
     * @param force Whether to create a renaming Project if the projections are trivial
     */
    public AlgBuilder projectNamed( Iterable<? extends RexNode> nodes, Iterable<String> fieldNames, boolean force ) {
        @SuppressWarnings("unchecked") final List<? extends RexNode> nodeList =
                nodes instanceof List
                        ? (List) nodes
                        : ImmutableList.copyOf( nodes );
        final List<String> fieldNameList =
                fieldNames == null
                        ? null
                        : fieldNames instanceof List
                                ? (List<String>) fieldNames
                                : ImmutableNullableList.copyOf( fieldNames );
        final AlgNode input = peek();
        final AlgDataType rowType =
                RexUtil.createStructType(
                        cluster.getTypeFactory(),
                        nodeList,
                        fieldNameList,
                        ValidatorUtil.F_SUGGESTER );
        if ( !force && RexUtil.isIdentity( nodeList, input.getRowType() ) ) {
            if ( input instanceof Project && fieldNames != null ) {
                // Rename columns of child projection if desired field names are given.
                final Frame frame = stack.pop();
                final Project childProject = (Project) frame.alg;
                final Project newInput = childProject.copy(
                        childProject.getTraitSet(),
                        childProject.getInput(),
                        childProject.getProjects(),
                        rowType );
                stack.push( new Frame( newInput, frame.fields ) );
            }
        } else {
            project( nodeList, rowType.getFieldNames(), force );
        }
        return this;
    }


    /**
     * Ensures that the field names match those given.
     *
     * If all fields have the same name, adds nothing; if any fields do not have the same name, adds a {@link Project}.
     *
     * Note that the names can be short-lived. Other {@code AlgBuilder} operations make no guarantees about the field names
     * of the rows they produce.
     *
     * @param fieldNames List of desired field names; may contain null values or have fewer fields than the current row type
     */
    public AlgBuilder rename( List<String> fieldNames ) {
        final List<String> oldFieldNames = peek().getRowType().getFieldNames();
        Preconditions.checkArgument( fieldNames.size() <= oldFieldNames.size(), "More names than fields" );
        final List<String> newFieldNames = new ArrayList<>( oldFieldNames );
        for ( int i = 0; i < fieldNames.size(); i++ ) {
            final String s = fieldNames.get( i );
            if ( s != null ) {
                newFieldNames.set( i, s );
            }
        }
        if ( oldFieldNames.equals( newFieldNames ) ) {
            return this;
        }
        if ( peek() instanceof Values ) {
            // Special treatment for VALUES. Re-build it rather than add a project.
            final Values v = (Values) build();
            final AlgDataTypeFactory.Builder b = getTypeFactory().builder();
            for ( Pair<String, AlgDataTypeField> p : Pair.zip( newFieldNames, v.getRowType().getFieldList() ) ) {
                b.add( p.left, null, p.right.getType() );
            }
            return values( v.tuples, b.build() );
        }

        return project( fields(), newFieldNames, true );
    }


    /**
     * Infers the alias of an expression.
     *
     * If the expression was created by {@link #alias}, replaces the expression in the project list.
     */
    private String inferAlias( List<RexNode> exprList, RexNode expr, int i ) {
        switch ( expr.getKind() ) {
            case INPUT_REF:
                final RexInputRef ref = (RexInputRef) expr;
                return stack.peek().fields.get( ref.getIndex() ).getValue().getName();
            case CAST:
                return inferAlias( exprList, ((RexCall) expr).getOperands().get( 0 ), -1 );
            case AS:
                final RexCall call = (RexCall) expr;
                if ( i >= 0 ) {
                    exprList.set( i, call.getOperands().get( 0 ) );
                }
                return ((NlsString) ((RexLiteral) call.getOperands().get( 1 )).getValue()).getValue();
            default:
                return null;
        }
    }


    /**
     * Creates an {@link Aggregate} that makes the relational expression distinct on all fields.
     */
    public AlgBuilder distinct() {
        return aggregate( groupKey( fields() ) );
    }


    /**
     * Creates an {@link Aggregate} with an array of calls.
     */
    public AlgBuilder aggregate( GroupKey groupKey, AggCall... aggCalls ) {
        return aggregate( groupKey, ImmutableList.copyOf( aggCalls ) );
    }


    /**
     * Creates an {@link Aggregate} with a list of calls.
     */
    public AlgBuilder aggregate( GroupKey groupKey, Iterable<AggCall> aggCalls ) {
        final Registrar registrar = new Registrar();
        registrar.extraNodes.addAll( fields() );
        registrar.names.addAll( peek().getRowType().getFieldNames() );
        final GroupKeyImpl groupKey_ = (GroupKeyImpl) groupKey;
        final ImmutableBitSet groupSet = ImmutableBitSet.of( registrar.registerExpressions( groupKey_.nodes ) );
        label:
        if ( Iterables.isEmpty( aggCalls ) && !groupKey_.indicator ) {
            final AlgMetadataQuery mq = peek().getCluster().getMetadataQuery();
            if ( groupSet.isEmpty() ) {
                final Double minRowCount = mq.getMinRowCount( peek() );
                if ( minRowCount == null || minRowCount < 1D ) {
                    // We can't remove "GROUP BY ()" if there's a chance the alg could be empty.
                    break label;
                }
            }
            if ( registrar.extraNodes.size() == fields().size() ) {
                final Boolean unique = mq.areColumnsUnique( peek(), groupSet );
                if ( unique != null && unique ) {
                    // Rel is already unique.
                    return project( fields( groupSet.asList() ) );
                }
            }
            final Double maxRowCount = mq.getMaxRowCount( peek() );
            if ( maxRowCount != null && maxRowCount <= 1D ) {
                // If there is at most one row, alg is already unique.
                return this;
            }
        }
        final ImmutableList<ImmutableBitSet> groupSets;
        if ( groupKey_.nodeLists != null ) {
            final int sizeBefore = registrar.extraNodes.size();
            final SortedSet<ImmutableBitSet> groupSetSet = new TreeSet<>( ImmutableBitSet.ORDERING );
            for ( ImmutableList<RexNode> nodeList : groupKey_.nodeLists ) {
                final ImmutableBitSet groupSet2 = ImmutableBitSet.of( registrar.registerExpressions( nodeList ) );
                if ( !groupSet.contains( groupSet2 ) ) {
                    throw new IllegalArgumentException( "group set element " + nodeList + " must be a subset of group key" );
                }
                groupSetSet.add( groupSet2 );
            }
            groupSets = ImmutableList.copyOf( groupSetSet );
            if ( registrar.extraNodes.size() > sizeBefore ) {
                throw new IllegalArgumentException( "group sets contained expressions not in group key: " + registrar.extraNodes.subList( sizeBefore, registrar.extraNodes.size() ) );
            }
        } else {
            groupSets = ImmutableList.of( groupSet );
        }
        for ( AggCall aggCall : aggCalls ) {
            if ( aggCall instanceof AggCallImpl ) {
                final AggCallImpl aggCall1 = (AggCallImpl) aggCall;
                registrar.registerExpressions( aggCall1.operands );
                if ( aggCall1.filter != null ) {
                    registrar.registerExpression( aggCall1.filter );
                }
            }
        }
        project( registrar.extraNodes );
        rename( registrar.names );
        final Frame frame = stack.pop();
        final AlgNode r = frame.alg;
        final List<AggregateCall> aggregateCalls = new ArrayList<>();
        for ( AggCall aggCall : aggCalls ) {
            final AggregateCall aggregateCall;
            if ( aggCall instanceof AggCallImpl ) {
                final AggCallImpl aggCall1 = (AggCallImpl) aggCall;
                final List<Integer> args = registrar.registerExpressions( aggCall1.operands );
                final int filterArg =
                        aggCall1.filter == null
                                ? -1
                                : registrar.registerExpression( aggCall1.filter );
                if ( aggCall1.distinct && !aggCall1.aggFunction.isQuantifierAllowed() ) {
                    throw new IllegalArgumentException( "DISTINCT not allowed" );
                }
                if ( aggCall1.filter != null && !aggCall1.aggFunction.allowsFilter() ) {
                    throw new IllegalArgumentException( "FILTER not allowed" );
                }
                AlgCollation collation =
                        AlgCollations.of( aggCall1.orderKeys
                                .stream()
                                .map( orderKey -> collation(
                                        orderKey,
                                        AlgFieldCollation.Direction.ASCENDING,
                                        null,
                                        Collections.emptyList() ) )
                                .collect( Collectors.toList() ) );
                aggregateCall =
                        AggregateCall.create(
                                (Operator & AggFunction) aggCall1.aggFunction,
                                aggCall1.distinct,
                                aggCall1.approximate,
                                args,
                                filterArg,
                                collation,
                                groupSet.cardinality(),
                                r,
                                null,
                                aggCall1.alias );
            } else {
                aggregateCall = ((AggCallImpl2) aggCall).aggregateCall;
            }
            aggregateCalls.add( aggregateCall );
        }

        assert ImmutableBitSet.ORDERING.isStrictlyOrdered( groupSets ) : groupSets;
        for ( ImmutableBitSet set : groupSets ) {
            assert groupSet.contains( set );
        }
        AlgNode aggregate = aggregateFactory.createAggregate( r, groupKey_.indicator, groupSet, groupSets, aggregateCalls );

        // build field list
        final ImmutableList.Builder<Field> fields = ImmutableList.builder();
        final List<AlgDataTypeField> aggregateFields = aggregate.getRowType().getFieldList();
        int i = 0;
        // first, group fields
        for ( Integer groupField : groupSet.asList() ) {
            RexNode node = registrar.extraNodes.get( groupField );
            final Kind kind = node.getKind();
            switch ( kind ) {
                case INPUT_REF:
                    fields.add( frame.fields.get( ((RexInputRef) node).getIndex() ) );
                    break;
                default:
                    String name = aggregateFields.get( i ).getName();
                    AlgDataTypeField fieldType = new AlgDataTypeFieldImpl( name, i, node.getType() );
                    fields.add( new Field( ImmutableSet.of(), fieldType ) );
                    break;
            }
            i++;
        }
        // second, indicator fields (copy from aggregate alg type)
        if ( groupKey_.indicator ) {
            for ( int j = 0; j < groupSet.cardinality(); ++j ) {
                final AlgDataTypeField field = aggregateFields.get( i );
                final AlgDataTypeField fieldType = new AlgDataTypeFieldImpl( field.getName(), i, field.getType() );
                fields.add( new Field( ImmutableSet.of(), fieldType ) );
                i++;
            }
        }
        // third, aggregate fields. retain `i' as field index
        for ( int j = 0; j < aggregateCalls.size(); ++j ) {
            final AggregateCall call = aggregateCalls.get( j );
            final AlgDataTypeField fieldType = new AlgDataTypeFieldImpl( aggregateFields.get( i + j ).getName(), i + j, call.getType() );
            fields.add( new Field( ImmutableSet.of(), fieldType ) );
        }
        stack.push( new Frame( aggregate, fields.build() ) );
        return this;
    }


    private AlgBuilder setOp( boolean all, Kind kind, int n ) {
        List<AlgNode> inputs = new LinkedList<>();
        for ( int i = 0; i < n; i++ ) {
            inputs.add( 0, build() );
        }
        switch ( kind ) {
            case UNION:
            case INTERSECT:
            case EXCEPT:
                if ( n < 1 ) {
                    throw new IllegalArgumentException( "bad INTERSECT/UNION/EXCEPT input count" );
                }
                break;
            default:
                throw new AssertionError( "bad setOp " + kind );
        }
        switch ( n ) {
            case 1:
                return push( inputs.get( 0 ) );
            default:
                return push( setOpFactory.createSetOp( kind, inputs, all ) );
        }
    }


    /**
     * Creates a {@link Union} of the two most recent relational expressions on the stack.
     *
     * @param all Whether to create UNION ALL
     */
    public AlgBuilder union( boolean all ) {
        return union( all, 2 );
    }


    /**
     * Creates a {@link Union} of the {@code n} most recent relational expressions on the stack.
     *
     * @param all Whether to create UNION ALL
     * @param n Number of inputs to the UNION operator
     */
    public AlgBuilder union( boolean all, int n ) {
        return setOp( all, Kind.UNION, n );
    }


    /**
     * Creates an {@link Intersect} of the two most recent relational expressions on the stack.
     *
     * @param all Whether to create INTERSECT ALL
     */
    public AlgBuilder intersect( boolean all ) {
        return intersect( all, 2 );
    }


    /**
     * Creates an {@link Intersect} of the {@code n} most recent relational expressions on the stack.
     *
     * @param all Whether to create INTERSECT ALL
     * @param n Number of inputs to the INTERSECT operator
     */
    public AlgBuilder intersect( boolean all, int n ) {
        return setOp( all, Kind.INTERSECT, n );
    }


    /**
     * Creates a {@link Minus} of the two most recent relational expressions on the stack.
     *
     * @param all Whether to create EXCEPT ALL
     */
    public AlgBuilder minus( boolean all ) {
        return minus( all, 2 );
    }


    /**
     * Creates a {@link Minus} of the {@code n} most recent relational expressions on the stack.
     *
     * @param all Whether to create EXCEPT ALL
     */
    public AlgBuilder minus( boolean all, int n ) {
        return setOp( all, Kind.EXCEPT, n );
    }


    /**
     * Creates a {@link Join}.
     */
    public AlgBuilder join( JoinAlgType joinType, RexNode condition0, RexNode... conditions ) {
        return join( joinType, Lists.asList( condition0, conditions ) );
    }


    /**
     * Creates a {@link Join} with multiple conditions.
     */
    public AlgBuilder join( JoinAlgType joinType, Iterable<? extends RexNode> conditions ) {
        return join( joinType, and( conditions ), ImmutableSet.of() );
    }


    public AlgBuilder join( JoinAlgType joinType, RexNode condition ) {
        return join( joinType, condition, ImmutableSet.of() );
    }


    /**
     * Creates a {@link Join} with correlating variables.
     */
    public AlgBuilder join( JoinAlgType joinType, RexNode condition, Set<CorrelationId> variablesSet ) {
        Frame right = stack.pop();
        final Frame left = stack.pop();
        final AlgNode join;
        final boolean correlate = variablesSet.size() == 1;
        RexNode postCondition = literal( true );
        if ( correlate ) {
            final CorrelationId id = Iterables.getOnlyElement( variablesSet );
            final ImmutableBitSet requiredColumns = AlgOptUtil.correlationColumns( id, right.alg );
            if ( !AlgOptUtil.notContainsCorrelation( left.alg, id, Litmus.IGNORE ) ) {
                throw new IllegalArgumentException( "variable " + id + " must not be used by left input to correlation" );
            }
            switch ( joinType ) {
                case LEFT:
                    // Correlate does not have an ON clause.
                    // For a LEFT correlate, predicate must be evaluated first.
                    // For INNER, we can defer.
                    stack.push( right );
                    filter( condition.accept( new Shifter( left.alg, id, right.alg ) ) );
                    right = stack.pop();
                    break;
                default:
                    postCondition = condition;
            }
            join = correlateFactory.createCorrelate( left.alg, right.alg, id, requiredColumns, SemiJoinType.of( joinType ) );
        } else {
            join = joinFactory.createJoin( left.alg, right.alg, condition, variablesSet, joinType, false );
        }
        final ImmutableList.Builder<Field> fields = ImmutableList.builder();
        fields.addAll( left.fields );
        fields.addAll( right.fields );
        stack.push( new Frame( join, fields.build() ) );
        filter( postCondition );
        return this;
    }


    /**
     * Creates a {@link Join} using USING syntax.
     *
     * For each of the field names, both left and right inputs must have a field of that name. Constructs a join condition
     * that the left and right fields are equal.
     *
     * @param joinType Join type
     * @param fieldNames Field names
     */
    public AlgBuilder join( JoinAlgType joinType, String... fieldNames ) {
        final List<RexNode> conditions = new ArrayList<>();
        for ( String fieldName : fieldNames ) {
            conditions.add(
                    call(
                            OperatorRegistry.get( OperatorName.EQUALS ),
                            field( 2, 0, fieldName ),
                            field( 2, 1, fieldName ) ) );
        }
        return join( joinType, conditions );
    }


    /**
     * Creates a {@link SemiJoin}.
     */
    public AlgBuilder semiJoin( Iterable<? extends RexNode> conditions ) {
        final Frame right = stack.pop();
        final AlgNode semiJoin = semiJoinFactory.createSemiJoin( peek(), right.alg, and( conditions ) );
        replaceTop( semiJoin );
        return this;
    }


    /**
     * Creates a {@link SemiJoin}.
     */
    public AlgBuilder semiJoin( RexNode... conditions ) {
        return semiJoin( ImmutableList.copyOf( conditions ) );
    }


    /**
     * Assigns a table alias to the top entry on the stack.
     */
    public AlgBuilder as( final String alias ) {
        final Frame pair = stack.pop();
        List<Field> newFields = Util.transform( pair.fields, field -> field.addAlias( alias ) );
        stack.push( new Frame( pair.alg, ImmutableList.copyOf( newFields ) ) );
        return this;
    }


    /**
     * Creates a {@link Values}.
     *
     * The {@code values} array must have the same number of entries as {@code fieldNames}, or an integer multiple if you
     * wish to create multiple rows.
     *
     * If there are zero rows, or if all values of a any column are null, this method cannot deduce the type of columns.
     * For these cases, call {@link #values(Iterable, AlgDataType)}.
     *
     * @param fieldNames Field names
     * @param values Values
     */
    public AlgBuilder values( String[] fieldNames, Object... values ) {
        if ( fieldNames == null
                || fieldNames.length == 0
                || values.length % fieldNames.length != 0
                || values.length < fieldNames.length ) {
            throw new IllegalArgumentException( "Value count must be a positive multiple of field count" );
        }
        final int rowCount = values.length / fieldNames.length;
        for ( Ord<String> fieldName : Ord.zip( fieldNames ) ) {
            if ( allNull( values, fieldName.i, fieldNames.length ) ) {
                throw new IllegalArgumentException( "All values of field '" + fieldName.e + "' are null; cannot deduce type" );
            }
        }
        final ImmutableList<ImmutableList<RexLiteral>> tupleList = tupleList( fieldNames.length, values );
        final AlgDataTypeFactory typeFactory = cluster.getTypeFactory();
        final AlgDataTypeFactory.Builder builder = typeFactory.builder();
        for ( final Ord<String> fieldName : Ord.zip( fieldNames ) ) {
            final String name =
                    fieldName.e != null
                            ? fieldName.e
                            : "expr$" + fieldName.i;
            final AlgDataType type = typeFactory.leastRestrictive(
                    new AbstractList<AlgDataType>() {
                        @Override
                        public AlgDataType get( int index ) {
                            return tupleList.get( index ).get( fieldName.i ).getType();
                        }


                        @Override
                        public int size() {
                            return rowCount;
                        }
                    } );
            builder.add( name, null, type );
        }
        final AlgDataType rowType = builder.build();
        return values( tupleList, rowType );
    }


    private ImmutableList<ImmutableList<RexLiteral>> tupleList( int columnCount, Object[] values ) {
        final ImmutableList.Builder<ImmutableList<RexLiteral>> listBuilder = ImmutableList.builder();
        final List<RexLiteral> valueList = new ArrayList<>();
        for ( int i = 0; i < values.length; i++ ) {
            Object value = values[i];
            valueList.add( (RexLiteral) literal( value ) );
            if ( (i + 1) % columnCount == 0 ) {
                listBuilder.add( ImmutableList.copyOf( valueList ) );
                valueList.clear();
            }
        }
        return listBuilder.build();
    }


    /**
     * Returns whether all values for a given column are null.
     */
    private boolean allNull( Object[] values, int column, int columnCount ) {
        for ( int i = column; i < values.length; i += columnCount ) {
            if ( values[i] != null ) {
                return false;
            }
        }
        return true;
    }


    /**
     * Creates a relational expression that reads from an input and throws all of the rows away.
     *
     * Note that this method always pops one relational expression from the stack. {@code values}, in contrast, does not
     * pop any relational expressions, and always produces a leaf.
     *
     * The default implementation creates a {@link Values} with the same specified row type as the input, and ignores the
     * input entirely. But schema-on-query systems such as Drill might override this method to create a relation expression
     * that retains the input, just to read its schema.
     */
    public AlgBuilder empty() {
        final Frame frame = stack.pop();
        return values( frame.alg.getRowType() );
    }


    /**
     * Creates a {@link Values} with a specified row type.
     *
     * This method can handle cases that {@link #values(String[], Object...)} cannot, such as all values of a column being
     * null, or there being zero rows.
     *
     * @param rowType Row type
     * @param columnValues Values
     */
    public AlgBuilder values( AlgDataType rowType, Object... columnValues ) {
        final ImmutableList<ImmutableList<RexLiteral>> tupleList = tupleList( rowType.getFieldCount(), columnValues );
        AlgNode values = valuesFactory.createValues( cluster, rowType, ImmutableList.copyOf( tupleList ) );
        push( values );
        return this;
    }


    /**
     * Creates a {@link Values} with a specified row type.
     *
     * This method can handle cases that {@link #values(String[], Object...)} cannot, such as all values of a column being
     * null, or there being zero rows.
     *
     * @param tupleList Tuple list
     * @param rowType Row type
     */
    public AlgBuilder values( Iterable<? extends List<RexLiteral>> tupleList, AlgDataType rowType ) {
        AlgNode values = valuesFactory.createValues( cluster, rowType, copy( tupleList ) );
        push( values );
        return this;
    }


    public AlgBuilder documents( ImmutableList<BsonValue> tuples, AlgDataType rowType ) {
        AlgNode documents = documentsFactory.createDocuments( cluster, tuples, rowType );
        push( documents );
        return this;
    }


    /**
     * Creates a {@link Values} with a specified row type and zero rows.
     *
     * @param rowType Row type
     */
    public AlgBuilder values( AlgDataType rowType ) {
        return values( ImmutableList.<ImmutableList<RexLiteral>>of(), rowType );
    }


    /**
     * Converts an iterable of lists into an immutable list of immutable lists with the same contents. Returns the same
     * object if possible.
     */
    private static <E> ImmutableList<ImmutableList<E>> copy( Iterable<? extends List<E>> tupleList ) {
        final ImmutableList.Builder<ImmutableList<E>> builder = ImmutableList.builder();
        int changeCount = 0;
        for ( List<E> literals : tupleList ) {
            final ImmutableList<E> literals2 = ImmutableList.copyOf( literals );
            builder.add( literals2 );
            if ( literals != literals2 ) {
                ++changeCount;
            }
        }
        if ( changeCount == 0 ) {
            // don't make a copy if we don't have to
            //noinspection unchecked
            return (ImmutableList<ImmutableList<E>>) tupleList;
        }
        return builder.build();
    }


    /**
     * Creates a limit without a sort.
     */
    public AlgBuilder limit( int offset, int fetch ) {
        return sortLimit( offset, fetch, ImmutableList.of() );
    }


    /**
     * Creates an Exchange by distribution.
     */
    public AlgBuilder exchange( AlgDistribution distribution ) {
        AlgNode exchange = exchangeFactory.createExchange( peek(), distribution );
        replaceTop( exchange );
        return this;
    }


    /**
     * Creates a SortExchange by distribution and collation.
     */
    public AlgBuilder sortExchange( AlgDistribution distribution, AlgCollation collation ) {
        AlgNode exchange = sortExchangeFactory.createSortExchange( peek(), distribution, collation );
        replaceTop( exchange );
        return this;
    }


    /**
     * Creates a {@link Sort} by field ordinals.
     *
     * Negative fields mean descending: -1 means field(0) descending, -2 means field(1) descending, etc.
     */
    public AlgBuilder sort( int... fields ) {
        final ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
        for ( int field : fields ) {
            builder.add( field < 0 ? desc( field( -field - 1 ) ) : field( field ) );
        }
        return sortLimit( -1, -1, builder.build() );
    }


    /**
     * Creates a {@link Sort} by expressions.
     */
    public AlgBuilder sort( RexNode... nodes ) {
        return sortLimit( -1, -1, ImmutableList.copyOf( nodes ) );
    }


    /**
     * Creates a {@link Sort} by expressions.
     */
    public AlgBuilder sort( Iterable<? extends RexNode> nodes ) {
        return sortLimit( -1, -1, nodes );
    }


    /**
     * Creates a {@link Sort} by expressions, with limit and offset.
     */
    public AlgBuilder sortLimit( int offset, int fetch, RexNode... nodes ) {
        return sortLimit( offset, fetch, ImmutableList.copyOf( nodes ) );
    }


    /**
     * Creates a {@link Sort} by a list of expressions, with limit and offset.
     *
     * @param offset Number of rows to skip; non-positive means don't skip any
     * @param fetch Maximum number of rows to fetch; negative means no limit
     * @param nodes Sort expressions
     */
    public AlgBuilder sortLimit( int offset, int fetch, Iterable<? extends RexNode> nodes ) {
        final List<AlgFieldCollation> fieldCollations = new ArrayList<>();
        final List<RexNode> originalExtraNodes = fields();
        final List<RexNode> extraNodes = new ArrayList<>( originalExtraNodes );
        for ( RexNode node : nodes ) {
            final AlgFieldCollation collation = collation( node, AlgFieldCollation.Direction.ASCENDING, null, extraNodes );
            if ( !AlgCollations.ordinals( fieldCollations ).contains( collation.getFieldIndex() ) ) {
                fieldCollations.add( collation );
            }
        }
        final RexNode offsetNode = offset <= 0 ? null : literal( offset );
        final RexNode fetchNode = fetch < 0 ? null : literal( fetch );
        if ( offsetNode == null && fetch == 0 ) {
            return empty();
        }
        if ( offsetNode == null && fetchNode == null && fieldCollations.isEmpty() ) {
            return this; // sort is trivial
        }

        final boolean addedFields = extraNodes.size() > originalExtraNodes.size();
        if ( fieldCollations.isEmpty() ) {
            assert !addedFields;
            AlgNode top = peek();
            if ( top instanceof Sort ) {
                final Sort sort2 = (Sort) top;
                if ( sort2.offset == null && sort2.fetch == null ) {
                    replaceTop( sort2.getInput() );
                    final AlgNode sort = sortFactory.createSort( peek(), sort2.collation, offsetNode, fetchNode );
                    replaceTop( sort );
                    return this;
                }
            }
            if ( top instanceof Project ) {
                final Project project = (Project) top;
                if ( project.getInput() instanceof Sort ) {
                    final Sort sort2 = (Sort) project.getInput();
                    if ( sort2.offset == null && sort2.fetch == null ) {
                        final AlgNode sort = sortFactory.createSort( sort2.getInput(), sort2.collation, offsetNode, fetchNode );
                        replaceTop( projectFactory.createProject( sort, project.getProjects(), Pair.right( project.getNamedProjects() ) ) );
                        return this;
                    }
                }
            }
        }
        if ( addedFields ) {
            project( extraNodes );
        }
        final AlgNode sort = sortFactory.createSort( peek(), AlgCollations.of( fieldCollations ), offsetNode, fetchNode );
        replaceTop( sort );
        if ( addedFields ) {
            project( originalExtraNodes );
        }
        return this;
    }


    private static AlgFieldCollation collation(
            RexNode node,
            AlgFieldCollation.Direction direction,
            AlgFieldCollation.NullDirection nullDirection,
            List<RexNode> extraNodes ) {
        switch ( node.getKind() ) {
            case INPUT_REF:
                return new AlgFieldCollation(
                        ((RexInputRef) node).getIndex(),
                        direction,
                        Util.first( nullDirection, direction.defaultNullDirection() ) );
            case DESCENDING:
                return collation(
                        ((RexCall) node).getOperands().get( 0 ),
                        AlgFieldCollation.Direction.DESCENDING,
                        nullDirection,
                        extraNodes );
            case NULLS_FIRST:
                return collation(
                        ((RexCall) node).getOperands().get( 0 ),
                        direction,
                        AlgFieldCollation.NullDirection.FIRST,
                        extraNodes );
            case NULLS_LAST:
                return collation(
                        ((RexCall) node).getOperands().get( 0 ),
                        direction,
                        AlgFieldCollation.NullDirection.LAST,
                        extraNodes );
            default:
                final int fieldIndex = extraNodes.size();
                extraNodes.add( node );
                return new AlgFieldCollation(
                        fieldIndex,
                        direction,
                        Util.first( nullDirection, direction.defaultNullDirection() ) );
        }
    }


    /**
     * Creates a projection that converts the current relational expression's output to a desired row type.
     *
     * @param castRowType row type after cast
     * @param rename if true, use field names from castRowType; if false, preserve field names from rel
     */
    public AlgBuilder convert( AlgDataType castRowType, boolean rename ) {
        final AlgNode r = build();
        final AlgNode r2 = AlgOptUtil.createCastAlg( r, castRowType, rename, projectFactory );
        push( r2 );
        return this;
    }


    public AlgBuilder permute( Mapping mapping ) {
        assert mapping.getMappingType().isSingleSource();
        assert mapping.getMappingType().isMandatorySource();
        if ( mapping.isIdentity() ) {
            return this;
        }
        final List<RexNode> exprList = new ArrayList<>();
        for ( int i = 0; i < mapping.getTargetCount(); i++ ) {
            exprList.add( field( mapping.getSource( i ) ) );
        }
        return project( exprList );
    }


    public AlgBuilder aggregate( GroupKey groupKey, List<AggregateCall> aggregateCalls ) {
        return aggregate( groupKey, Lists.transform( aggregateCalls, AggCallImpl2::new ) );
    }


    /**
     * Creates a {@link Match}.
     */
    public AlgBuilder match(
            RexNode pattern,
            boolean strictStart,
            boolean strictEnd,
            Map<String, RexNode> patternDefinitions,
            Iterable<? extends RexNode> measureList,
            RexNode after,
            Map<String, ? extends SortedSet<String>> subsets,
            boolean allRows,
            Iterable<? extends RexNode> partitionKeys,
            Iterable<? extends RexNode> orderKeys,
            RexNode interval ) {
        final List<AlgFieldCollation> fieldCollations = new ArrayList<>();
        for ( RexNode orderKey : orderKeys ) {
            final AlgFieldCollation.Direction direction;
            switch ( orderKey.getKind() ) {
                case DESCENDING:
                    direction = AlgFieldCollation.Direction.DESCENDING;
                    orderKey = ((RexCall) orderKey).getOperands().get( 0 );
                    break;
                case NULLS_FIRST:
                case NULLS_LAST:
                    throw new AssertionError();
                default:
                    direction = AlgFieldCollation.Direction.ASCENDING;
                    break;
            }
            final AlgFieldCollation.NullDirection nullDirection = direction.defaultNullDirection();
            final RexInputRef ref = (RexInputRef) orderKey;
            fieldCollations.add( new AlgFieldCollation( ref.getIndex(), direction, nullDirection ) );
        }

        final AlgDataTypeFactory.Builder typeBuilder = cluster.getTypeFactory().builder();
        for ( RexNode partitionKey : partitionKeys ) {
            typeBuilder.add( partitionKey.toString(), null, partitionKey.getType() );
        }
        if ( allRows ) {
            for ( RexNode orderKey : orderKeys ) {
                if ( !typeBuilder.nameExists( orderKey.toString() ) ) {
                    typeBuilder.add( orderKey.toString(), null, orderKey.getType() );
                }
            }

            final AlgDataType inputRowType = peek().getRowType();
            for ( AlgDataTypeField fs : inputRowType.getFieldList() ) {
                if ( !typeBuilder.nameExists( fs.getName() ) ) {
                    typeBuilder.add( fs );
                }
            }
        }

        final ImmutableMap.Builder<String, RexNode> measures = ImmutableMap.builder();
        for ( RexNode measure : measureList ) {
            List<RexNode> operands = ((RexCall) measure).getOperands();
            String alias = operands.get( 1 ).toString();
            typeBuilder.add( alias, null, operands.get( 0 ).getType() );
            measures.put( alias, operands.get( 0 ) );
        }

        final AlgNode match = matchFactory.createMatch(
                peek(),
                pattern,
                typeBuilder.build(),
                strictStart,
                strictEnd,
                patternDefinitions,
                measures.build(),
                after,
                subsets,
                allRows,
                ImmutableList.copyOf( partitionKeys ),
                AlgCollations.of( fieldCollations ),
                interval );
        stack.push( new Frame( match ) );
        return this;
    }


    /**
     * Clears the stack.
     *
     * The builder's state is now the same as when it was created.
     */
    public void clear() {
        stack.clear();
    }


    /**
     * Information necessary to create a call to an aggregate function.
     *
     * @see AlgBuilder#aggregateCall
     */
    public interface AggCall {

        /**
         * Returns a copy of this AggCall that applies a filter before aggregating values.
         */
        AggCall filter( RexNode condition );

        /**
         * Returns a copy of this AggCall that sorts its input values by {@code orderKeys} before aggregating, as in SQL's
         * {@code WITHIN GROUP} clause.
         */
        AggCall sort( Iterable<RexNode> orderKeys );

        /**
         * Returns a copy of this AggCall that sorts its input values by {@code orderKeys} before aggregating, as in SQL's
         * {@code WITHIN GROUP} clause.
         */
        AggCall sort( RexNode... orderKeys );

        /**
         * Returns a copy of this AggCall that may return approximate results if {@code approximate} is true.
         */
        AggCall approximate( boolean approximate );

        /**
         * Returns a copy of this AggCall with a given alias.
         */
        AggCall as( String alias );

        /**
         * Returns a copy of this AggCall that is optionally distinct.
         */
        AggCall distinct( boolean distinct );

        /**
         * Returns a copy of this AggCall that is distinct.
         */
        AggCall distinct();

    }


    /**
     * Information necessary to create the GROUP BY clause of an Aggregate.
     *
     * @see AlgBuilder#groupKey
     */
    public interface GroupKey {

        /**
         * Assigns an alias to this group key.
         *
         * Used to assign field names in the {@code group} operation.
         */
        GroupKey alias( String alias );

    }


    /**
     * Implementation of {@link GroupKey}.
     */
    protected static class GroupKeyImpl implements GroupKey {

        final ImmutableList<RexNode> nodes;
        final boolean indicator;
        final ImmutableList<ImmutableList<RexNode>> nodeLists;
        final String alias;


        GroupKeyImpl( ImmutableList<RexNode> nodes, boolean indicator, ImmutableList<ImmutableList<RexNode>> nodeLists, String alias ) {
            this.nodes = Objects.requireNonNull( nodes );
            assert !indicator;
            this.indicator = indicator;
            this.nodeLists = nodeLists;
            this.alias = alias;
        }


        @Override
        public String toString() {
            return alias == null ? nodes.toString() : nodes + " as " + alias;
        }


        @Override
        public GroupKey alias( String alias ) {
            return Objects.equals( this.alias, alias )
                    ? this
                    : new GroupKeyImpl( nodes, indicator, nodeLists, alias );
        }

    }


    /**
     * Implementation of {@link AggCall}.
     */
    private class AggCallImpl implements AggCall {

        private final AggFunction aggFunction;
        private final boolean distinct;
        private final boolean approximate;
        private final RexNode filter; // may be null
        private final String alias; // may be null
        private final ImmutableList<RexNode> operands; // may be empty, never null
        private final ImmutableList<RexNode> orderKeys; // may be empty, never null


        AggCallImpl(
                AggFunction aggFunction,
                boolean distinct,
                boolean approximate,
                RexNode filter,
                String alias,
                ImmutableList<RexNode> operands,
                ImmutableList<RexNode> orderKeys ) {
            this.aggFunction = Objects.requireNonNull( aggFunction );
            this.distinct = distinct;
            this.approximate = approximate;
            this.alias = alias;
            this.operands = Objects.requireNonNull( operands );
            this.orderKeys = Objects.requireNonNull( orderKeys );
            if ( filter != null ) {
                if ( filter.getType().getPolyType() != PolyType.BOOLEAN ) {
                    throw RESOURCE.filterMustBeBoolean().ex();
                }
                if ( filter.getType().isNullable() ) {
                    filter = call( OperatorRegistry.get( OperatorName.IS_TRUE ), filter );
                }
            }
            this.filter = filter;
        }


        @Override
        public AggCall sort( Iterable<RexNode> orderKeys ) {
            final ImmutableList<RexNode> orderKeyList = ImmutableList.copyOf( orderKeys );
            return orderKeyList.equals( this.orderKeys )
                    ? this
                    : new AggCallImpl( aggFunction, distinct, approximate, filter, alias, operands, orderKeyList );
        }


        @Override
        public AggCall sort( RexNode... orderKeys ) {
            return sort( ImmutableList.copyOf( orderKeys ) );
        }


        @Override
        public AggCall approximate( boolean approximate ) {
            return approximate == this.approximate
                    ? this
                    : new AggCallImpl( aggFunction, distinct, approximate, filter, alias, operands, orderKeys );
        }


        @Override
        public AggCall filter( RexNode condition ) {
            return Objects.equals( condition, this.filter )
                    ? this
                    : new AggCallImpl( aggFunction, distinct, approximate, condition, alias, operands, orderKeys );
        }


        @Override
        public AggCall as( String alias ) {
            return Objects.equals( alias, this.alias )
                    ? this
                    : new AggCallImpl( aggFunction, distinct, approximate, filter, alias, operands, orderKeys );
        }


        @Override
        public AggCall distinct( boolean distinct ) {
            return distinct == this.distinct
                    ? this
                    : new AggCallImpl( aggFunction, distinct, approximate, filter, alias, operands, orderKeys );
        }


        @Override
        public AggCall distinct() {
            return distinct( true );
        }

    }


    /**
     * Implementation of {@link AggCall} that wraps an {@link AggregateCall}.
     */
    private static class AggCallImpl2 implements AggCall {

        private final AggregateCall aggregateCall;


        AggCallImpl2( AggregateCall aggregateCall ) {
            this.aggregateCall = Objects.requireNonNull( aggregateCall );
        }


        @Override
        public AggCall sort( Iterable<RexNode> orderKeys ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public AggCall sort( RexNode... orderKeys ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public AggCall approximate( boolean approximate ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public AggCall filter( RexNode condition ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public AggCall as( String alias ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public AggCall distinct( boolean distinct ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public AggCall distinct() {
            throw new UnsupportedOperationException();
        }

    }


    /**
     * Collects the extra expressions needed for {@link #aggregate}.
     *
     * The extra expressions come from the group key and as arguments to aggregate calls, and later there will be
     * a {@link #project} or a {@link #rename(List)} if necessary.
     */
    private static class Registrar {

        final List<RexNode> extraNodes = new ArrayList<>();
        final List<String> names = new ArrayList<>();


        int registerExpression( RexNode node ) {
            switch ( node.getKind() ) {
                case AS:
                    final List<RexNode> operands = ((RexCall) node).operands;
                    int i = registerExpression( operands.get( 0 ) );
                    names.set( i, RexLiteral.stringValue( operands.get( 1 ) ) );
                    return i;
            }
            int i = extraNodes.indexOf( node );
            if ( i < 0 ) {
                i = extraNodes.size();
                extraNodes.add( node );
                names.add( null );
            }
            return i;
        }


        List<Integer> registerExpressions( Iterable<? extends RexNode> nodes ) {
            final List<Integer> builder = new ArrayList<>();
            for ( RexNode node : nodes ) {
                builder.add( registerExpression( node ) );
            }
            return builder;
        }

    }


    /**
     * Builder stack frame.
     *
     * Describes a previously created relational expression and information about how table aliases map into its row type.
     */
    private static class Frame {

        final AlgNode alg;
        final ImmutableList<Field> fields;


        private Frame( AlgNode alg, ImmutableList<Field> fields ) {
            this.alg = alg;
            this.fields = fields;
        }


        private Frame( AlgNode alg ) {
            String tableAlias = deriveAlias( alg );
            ImmutableList.Builder<Field> builder = ImmutableList.builder();
            ImmutableSet<String> aliases =
                    tableAlias == null
                            ? ImmutableSet.of()
                            : ImmutableSet.of( tableAlias );
            for ( AlgDataTypeField field : alg.getRowType().getFieldList() ) {
                builder.add( new Field( aliases, field ) );
            }
            this.alg = alg;
            this.fields = builder.build();
        }


        private static String deriveAlias( AlgNode alg ) {
            if ( alg instanceof Scan ) {
                final List<String> names = alg.getTable().getQualifiedName();
                if ( !names.isEmpty() ) {
                    return Util.last( names );
                }
            }
            return null;
        }


        List<AlgDataTypeField> fields() {
            return Pair.right( fields );
        }

    }


    /**
     * A field that belongs to a stack {@link Frame}.
     */
    private static class Field extends Pair<ImmutableSet<String>, AlgDataTypeField> {

        Field( ImmutableSet<String> left, AlgDataTypeField right ) {
            super( left, right );
        }


        Field addAlias( String alias ) {
            if ( left.contains( alias ) ) {
                return this;
            }
            final ImmutableSet<String> aliasList = ImmutableSet.<String>builder().addAll( left ).add( alias ).build();
            return new Field( aliasList, right );
        }

    }


    /**
     * Shuttle that shifts a predicate's inputs to the left, replacing early ones with references to a {@link RexCorrelVariable}.
     */
    private class Shifter extends RexShuttle {

        private final AlgNode left;
        private final CorrelationId id;
        private final AlgNode right;


        Shifter( AlgNode left, CorrelationId id, AlgNode right ) {
            this.left = left;
            this.id = id;
            this.right = right;
        }


        @Override
        public RexNode visitInputRef( RexInputRef inputRef ) {
            final AlgDataType leftRowType = left.getRowType();
            final RexBuilder rexBuilder = getRexBuilder();
            final int leftCount = leftRowType.getFieldCount();
            if ( inputRef.getIndex() < leftCount ) {
                final RexNode v = rexBuilder.makeCorrel( leftRowType, id );
                return rexBuilder.makeFieldAccess( v, inputRef.getIndex() );
            } else {
                return rexBuilder.makeInputRef( right, inputRef.getIndex() - leftCount );
            }
        }

    }

}
