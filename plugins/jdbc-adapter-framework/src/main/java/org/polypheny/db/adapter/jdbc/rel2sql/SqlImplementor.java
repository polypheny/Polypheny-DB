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

package org.polypheny.db.adapter.jdbc.rel2sql;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.jdbc.JdbcScan;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.JoinType;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.LangFunctionOperator;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexCorrelVariable;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexFieldAccess;
import org.polypheny.db.rex.RexFieldCollation;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexLocalRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexOver;
import org.polypheny.db.rex.RexPatternFieldRef;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.rex.RexSubQuery;
import org.polypheny.db.rex.RexWindow;
import org.polypheny.db.rex.RexWindowBound;
import org.polypheny.db.sql.language.SqlAggFunction;
import org.polypheny.db.sql.language.SqlBasicCall;
import org.polypheny.db.sql.language.SqlBinaryOperator;
import org.polypheny.db.sql.language.SqlBinaryStringLiteral;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlDateLiteral;
import org.polypheny.db.sql.language.SqlDialect;
import org.polypheny.db.sql.language.SqlDialect.IntervalParameterStrategy;
import org.polypheny.db.sql.language.SqlDynamicParam;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlIntervalQualifier;
import org.polypheny.db.sql.language.SqlJoin;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlMatchRecognize;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlOperator;
import org.polypheny.db.sql.language.SqlSelect;
import org.polypheny.db.sql.language.SqlSelectKeyword;
import org.polypheny.db.sql.language.SqlSetOperator;
import org.polypheny.db.sql.language.SqlWindow;
import org.polypheny.db.sql.language.fun.SqlCase;
import org.polypheny.db.sql.language.fun.SqlSumEmptyIsZeroAggFunction;
import org.polypheny.db.sql.language.validate.SqlValidatorUtil;
import org.polypheny.db.type.IntervalPolyType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.entity.PolyInterval;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.ValidatorUtil;


/**
 * State for generating a SQL statement.
 */
public abstract class SqlImplementor {

    public static final ParserPos POS = ParserPos.ZERO;

    public final SqlDialect dialect;
    protected final Set<String> aliasSet = new LinkedHashSet<>();
    protected final Map<String, SqlNode> ordinalMap = new HashMap<>();

    protected final Map<CorrelationId, Context> correlTableMap = new HashMap<>();


    protected SqlImplementor( SqlDialect dialect ) {
        this.dialect = Objects.requireNonNull( dialect );
    }


    public abstract Result visitChild( int i, AlgNode e );


    public void addSelect( List<SqlNode> selectList, SqlNode node, AlgDataType rowType ) {
        String name = rowType.getFieldNames().get( selectList.size() );
        String alias = SqlValidatorUtil.getAlias( node, -1 );
        if ( alias == null || !alias.equals( name ) ) {
            node = (SqlNode) OperatorRegistry.get( OperatorName.AS ).createCall( POS, node, new SqlIdentifier( name, POS ) );
        }
        selectList.add( node );
    }


    /**
     * Returns whether a list of expressions projects all fields, in order, from the input, with the same names.
     */
    public static boolean isStar( List<RexNode> exps, AlgDataType inputRowType, AlgDataType projectRowType ) {
        return false;
    }


    public static boolean isStar( RexProgram program ) {
        int i = 0;
        for ( RexLocalRef ref : program.getProjectList() ) {
            if ( ref.getIndex() != i++ ) {
                return false;
            }
        }
        return i == program.getInputRowType().getFieldCount();
    }


    public Result setOpToSql( SqlSetOperator operator, AlgNode alg ) {
        SqlNode node = null;
        for ( Ord<AlgNode> input : Ord.zip( alg.getInputs() ) ) {
            final Result result = visitChild( input.i, input.e );
            if ( node == null ) {
                node = input.getValue().unwrap( JdbcScan.class ).map( i -> result.asSelect( i.getEntity().getNodeList() ) )
                        .orElse( result.asSelect() );
            } else {
                if ( input.getValue().unwrap( JdbcScan.class ).isPresent() ) {
                    node = (SqlNode) operator.createCall( POS, node, result.asSelect( input.getValue().unwrap( JdbcScan.class ).get().getEntity().getNodeList() ) );
                } else {
                    node = (SqlNode) operator.createCall( POS, node, result.asSelect() );
                }
            }
        }
        final List<Clause> clauses = Expressions.list( Clause.SET_OP );
        return result( node, clauses, alg, null );
    }


    /**
     * Converts a {@link RexNode} condition into a {@link SqlNode}.
     *
     * @param node Join condition
     * @param leftContext Left context
     * @param rightContext Right context
     * @param leftFieldCount Number of fields on left result
     * @return SqlNode that represents the condition
     */
    public static SqlNode convertConditionToSqlNode( RexNode node, Context leftContext, Context rightContext, int leftFieldCount ) {
        if ( node.isAlwaysTrue() ) {
            return SqlLiteral.createBoolean( true, POS );
        }
        if ( node.isAlwaysFalse() ) {
            return SqlLiteral.createBoolean( false, POS );
        }
        if ( node instanceof RexIndexRef ) {
            Context joinContext = leftContext.implementor().joinContext( leftContext, rightContext );
            return joinContext.toSql( null, node );
        }
        if ( !(node instanceof RexCall) ) {
            throw new AssertionError( node );
        }
        final List<RexNode> operands;
        final SqlOperator op;
        final Context joinContext;
        switch ( node.getKind() ) {
            case AND:
            case OR:
                operands = ((RexCall) node).getOperands();
                op = (SqlOperator) ((RexCall) node).getOperator();
                SqlNode sqlCondition = null;
                for ( RexNode operand : operands ) {
                    SqlNode x = convertConditionToSqlNode( operand, leftContext, rightContext, leftFieldCount );
                    if ( sqlCondition == null ) {
                        sqlCondition = x;
                    } else {
                        sqlCondition = (SqlNode) op.createCall( POS, sqlCondition, x );
                    }
                }
                return sqlCondition;

            case EQUALS:
            case IS_NOT_DISTINCT_FROM:
            case NOT_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                node = stripCastFromString( node );
                operands = node.unwrap( RexCall.class ).orElseThrow().getOperands();
                op = (SqlOperator) node.unwrap( RexCall.class ).orElseThrow().getOperator();
                if ( operands.size() == 2
                        && operands.get( 0 ) instanceof RexIndexRef op0
                        && operands.get( 1 ) instanceof RexIndexRef op1 ) {

                    if ( op0.getIndex() < leftFieldCount && op1.getIndex() >= leftFieldCount ) {
                        // Arguments were of form 'op0 = op1'
                        return (SqlNode) op.createCall(
                                POS,
                                leftContext.field( op0.getIndex() ),
                                rightContext.field( op1.getIndex() - leftFieldCount ) );
                    }
                    if ( op1.getIndex() < leftFieldCount && op0.getIndex() >= leftFieldCount ) {
                        // Arguments were of form 'op1 = op0'
                        return (SqlNode) reverseOperatorDirection( op ).createCall(
                                POS,
                                leftContext.field( op1.getIndex() ),
                                rightContext.field( op0.getIndex() - leftFieldCount ) );
                    }
                }
                joinContext = leftContext.implementor().joinContext( leftContext, rightContext );
                return joinContext.toSql( null, node );
            case IS_NULL:
            case IS_NOT_NULL:
                operands = ((RexCall) node).getOperands();
                if ( operands.size() == 1 && operands.get( 0 ) instanceof RexIndexRef op0 ) {
                    op = (SqlOperator) ((RexCall) node).getOperator();
                    if ( op0.getIndex() < leftFieldCount ) {
                        return (SqlNode) op.createCall( POS, leftContext.field( op0.getIndex() ) );
                    } else {
                        return (SqlNode) op.createCall( POS, rightContext.field( op0.getIndex() - leftFieldCount ) );
                    }
                }
                joinContext = leftContext.implementor().joinContext( leftContext, rightContext );
                return joinContext.toSql( null, node );
            default:
                throw new AssertionError( node );
        }
    }


    /**
     * Removes cast from string.
     * <p>
     * For example, {@code x > CAST('2015-01-07' AS DATE)} becomes {@code x > '2015-01-07'}.
     */
    private static RexNode stripCastFromString( RexNode node ) {
        switch ( node.getKind() ) {
            case EQUALS:
            case IS_NOT_DISTINCT_FROM:
            case NOT_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                final RexCall call = (RexCall) node;
                final RexNode o0 = call.operands.get( 0 );
                final RexNode o1 = call.operands.get( 1 );
                if ( o0.getKind() == Kind.CAST && o1.getKind() != Kind.CAST ) {
                    final RexNode o0b = ((RexCall) o0).getOperands().get( 0 );
                    switch ( o0b.getType().getPolyType() ) {
                        case CHAR:
                        case JSON:
                        case VARCHAR:
                            return call.clone( call.getType(), ImmutableList.of( o0b, o1 ) );
                    }
                }
                if ( o1.getKind() == Kind.CAST && o0.getKind() != Kind.CAST ) {
                    final RexNode o1b = ((RexCall) o1).getOperands().get( 0 );
                    switch ( o1b.getType().getPolyType() ) {
                        case CHAR:
                        case JSON:
                        case VARCHAR:
                            return call.clone( call.getType(), ImmutableList.of( o0, o1b ) );
                    }
                }
        }
        return node;
    }


    private static SqlOperator reverseOperatorDirection( SqlOperator op ) {
        return switch ( op.kind ) {
            case GREATER_THAN -> (SqlOperator) OperatorRegistry.get( OperatorName.LESS_THAN );
            case GREATER_THAN_OR_EQUAL -> (SqlOperator) OperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL );
            case LESS_THAN -> (SqlOperator) OperatorRegistry.get( OperatorName.GREATER_THAN );
            case LESS_THAN_OR_EQUAL -> (SqlOperator) OperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL );
            case EQUALS, IS_NOT_DISTINCT_FROM, NOT_EQUALS -> op;
            default -> throw new AssertionError( op );
        };
    }


    public static JoinType joinType( JoinAlgType joinType ) {
        return switch ( joinType ) {
            case LEFT -> JoinType.LEFT;
            case RIGHT -> JoinType.RIGHT;
            case INNER -> JoinType.INNER;
            case FULL -> JoinType.FULL;
        };
    }


    /**
     * Creates a result based on a single algebra expression.
     */
    public Result result( SqlNode node, Collection<Clause> clauses, AlgNode alg, Map<String, AlgDataType> aliases ) {
        assert aliases == null
                || aliases.size() < 2
                || aliases instanceof LinkedHashMap
                || aliases instanceof ImmutableMap
                : "must use a Map implementation that preserves order";
        final String alias2 = SqlValidatorUtil.getAlias( node, -1 );
        final String alias3 = alias2 != null ? alias2 : "t";
        final String alias4 = ValidatorUtil.uniquify( alias3, aliasSet, ValidatorUtil.EXPR_SUGGESTER );
        if ( aliases != null && !aliases.isEmpty() && (!dialect.hasImplicitTableAlias() || aliases.size() > 1) ) {
            return new Result( node, clauses, alias4, alg.getTupleType(), aliases );
        }
        final String alias5;
        if ( alias2 == null || !alias2.equals( alias4 ) || !dialect.hasImplicitTableAlias() ) {
            alias5 = alias4;
        } else {
            alias5 = null;
        }
        return new Result( node, clauses, alias5, alg.getTupleType(), ImmutableMap.of( alias4, alg.getTupleType() ) );
    }


    /**
     * Creates a result based on a join. (Each join could contain one or more algebra expressions.)
     */
    public Result result( SqlNode join, Result leftResult, Result rightResult ) {
        final ImmutableMap.Builder<String, AlgDataType> builder = ImmutableMap.builder();
        collectAliases( builder, join, Iterables.concat( leftResult.aliases.values(), rightResult.aliases.values() ).iterator() );
        return new Result( join, Expressions.list( Clause.FROM ), null, null, builder.build() );
    }


    private void collectAliases( ImmutableMap.Builder<String, AlgDataType> builder, SqlNode node, Iterator<AlgDataType> aliases ) {
        if ( node instanceof SqlJoin join ) {
            collectAliases( builder, join.getLeft(), aliases );
            collectAliases( builder, join.getRight(), aliases );
        } else {
            final String alias = SqlValidatorUtil.getAlias( node, -1 );
            assert alias != null;
            builder.put( alias, aliases.next() );
        }
    }


    SqlSelect wrapSelect( SqlNode node ) {
        return wrapSelect( node, null );
    }


    /**
     * Wraps a node in a SELECT statement that has no clauses: "SELECT ... FROM (node)".
     */
    SqlSelect wrapSelect( SqlNode node, SqlNodeList sqlNodes ) {
        assert node instanceof SqlJoin
                || node instanceof SqlIdentifier
                || node instanceof SqlMatchRecognize
                || node instanceof SqlCall
                && (((SqlCall) node).getOperator() instanceof SqlSetOperator
                || ((SqlCall) node).getOperator().getOperatorName() == OperatorName.AS
                || ((SqlCall) node).getOperator().getOperatorName() == OperatorName.VALUES)
                : node;
        return new SqlSelect(
                POS,
                SqlNodeList.EMPTY,
                sqlNodes,
                node,
                null,
                null,
                null,
                SqlNodeList.EMPTY,
                null,
                null,
                null );
    }


    /**
     * Context for translating a {@link RexNode} expression (within a {@link AlgNode}) into a {@link SqlNode}
     * expression (within a SQL parse tree).
     */
    public abstract static class Context {

        final SqlDialect dialect;
        final int fieldCount;
        private final boolean ignoreCast;


        protected Context( SqlDialect dialect, int fieldCount ) {
            this( dialect, fieldCount, false );
        }


        protected Context( SqlDialect dialect, int fieldCount, boolean ignoreCast ) {
            this.dialect = dialect;
            this.fieldCount = fieldCount;
            this.ignoreCast = ignoreCast;
        }


        public abstract SqlNode field( int ordinal );


        /**
         * Converts an expression from {@link RexNode} to {@link SqlNode} format.
         *
         * @param program Required only if {@code rex} contains {@link RexLocalRef}
         * @param rex Expression to convert
         */
        public SqlNode toSql( RexProgram program, RexNode rex ) {
            final RexSubQuery subQuery;
            final SqlNode sqlSubQuery;
            switch ( rex.getKind() ) {
                case LOCAL_REF:
                    final int index = ((RexLocalRef) rex).getIndex();
                    return toSql( program, program.getExprList().get( index ) );

                case INPUT_REF:
                    return field( ((RexIndexRef) rex).getIndex() );

                case FIELD_ACCESS:
                    final Deque<RexFieldAccess> accesses = new ArrayDeque<>();
                    RexNode referencedExpr = rex;
                    while ( referencedExpr.getKind() == Kind.FIELD_ACCESS ) {
                        accesses.offerLast( (RexFieldAccess) referencedExpr );
                        referencedExpr = ((RexFieldAccess) referencedExpr).getReferenceExpr();
                    }
                    SqlIdentifier sqlIdentifier;
                    if ( Objects.requireNonNull( referencedExpr.getKind() ) == Kind.CORREL_VARIABLE ) {
                        final RexCorrelVariable variable = (RexCorrelVariable) referencedExpr;
                        final Context correlAliasContext = getAliasContext( variable );
                        final RexFieldAccess lastAccess = accesses.pollLast();
                        assert lastAccess != null;
                        sqlIdentifier = (SqlIdentifier) correlAliasContext.field( lastAccess.getField().getIndex() );
                    } else {
                        sqlIdentifier = (SqlIdentifier) toSql( program, referencedExpr );
                    }

                    int nameIndex = sqlIdentifier.names.size();
                    RexFieldAccess access;
                    while ( (access = accesses.pollLast()) != null ) {
                        sqlIdentifier = sqlIdentifier.add( nameIndex++, access.getField().getName(), POS );
                    }
                    return sqlIdentifier;

                case PATTERN_INPUT_REF:
                    final RexPatternFieldRef ref = (RexPatternFieldRef) rex;
                    String pv = ref.getAlpha();
                    SqlNode refNode = field( ref.getIndex() );
                    final SqlIdentifier id = (SqlIdentifier) refNode;
                    if ( id.names.size() > 1 ) {
                        return id.setName( 0, pv );
                    } else {
                        return new SqlIdentifier( ImmutableList.of( pv, id.names.get( 0 ) ), POS );
                    }

                case LITERAL:
                    final RexLiteral literal = (RexLiteral) rex;
                    if ( literal.getPolyType() == PolyType.SYMBOL ) {
                        final Enum<?> symbol = literal.value.asSymbol().value;
                        return SqlLiteral.createSymbol( symbol, POS );
                    }
                    if ( RexLiteral.isNullLiteral( literal ) ) {
                        return SqlLiteral.createNull( POS );
                    }
                    switch ( literal.getPolyType().getFamily() ) {
                        case CHARACTER:
                            return SqlLiteral.createCharString( literal.value.asString().value, POS );
                        case NUMERIC:
                        case EXACT_NUMERIC:
                            return SqlLiteral.createExactNumeric( literal.value.asBigDecimal().value.toString(), POS );
                        case APPROXIMATE_NUMERIC:
                            return SqlLiteral.createApproxNumeric( literal.value.asBigDecimal().value.toString(), POS );
                        case BOOLEAN:
                            return SqlLiteral.createBoolean( literal.value.asBoolean().value, POS );
                        case INTERVAL_YEAR_MONTH:
                        case INTERVAL_TIME:
                            return SqlLiteral.createInterval( literal.value.asInterval(), SqlIntervalQualifier.from( literal.getType().getIntervalQualifier() ), POS );
                        case DATE:
                            return SqlDateLiteral.createDate( literal.value.asDate(), POS );
                        case TIME:
                            return SqlLiteral.createTime( literal.value.asTime(), literal.getType().getPrecision(), POS );
                        case TIMESTAMP:
                            return SqlLiteral.createTimestamp( literal.value.asTimestamp(), literal.getType().getPrecision(), POS );
                        case BINARY:
                            return SqlBinaryStringLiteral.createBinaryString( literal.value.asBinary(), POS );
                        case ARRAY:
                            if ( dialect.supportsNestedArrays() ) {
                                List<PolyValue> array = literal.getValue().asList();
                                return SqlLiteral.createArray( array, literal.getType(), POS );
                            } else {
                                return SqlLiteral.createCharString( literal.value.toTypedJson(), POS );
                            }
                        case GRAPH:
                            // node or edge
                            return SqlLiteral.createCharString( literal.value.serialize(), POS );
                        case ANY:
                        case NULL:
                            if ( literal.getPolyType() == PolyType.NULL ) {
                                return SqlLiteral.createNull( POS );
                            }
                            // fall through
                        default:
                            throw new AssertionError( literal + ": " + literal.getPolyType() );
                    }

                case CASE:
                    final RexCall caseCall = (RexCall) rex;
                    final List<SqlNode> caseNodeList = toSql( program, caseCall.getOperands() );
                    final SqlNode valueNode;
                    final List<SqlNode> whenList = Expressions.list();
                    final List<SqlNode> thenList = Expressions.list();
                    final SqlNode elseNode;
                    if ( caseNodeList.size() % 2 == 0 ) {
                        // switched:
                        //   "case x when v1 then t1 when v2 then t2 ... else e end"
                        valueNode = caseNodeList.get( 0 );
                        for ( int i = 1; i < caseNodeList.size() - 1; i += 2 ) {
                            whenList.add( caseNodeList.get( i ) );
                            thenList.add( caseNodeList.get( i + 1 ) );
                        }
                    } else {
                        // other: "case when w1 then t1 when w2 then t2 ... else e end"
                        valueNode = null;
                        for ( int i = 0; i < caseNodeList.size() - 1; i += 2 ) {
                            whenList.add( caseNodeList.get( i ) );
                            thenList.add( caseNodeList.get( i + 1 ) );
                        }
                    }
                    elseNode = caseNodeList.get( caseNodeList.size() - 1 );
                    return new SqlCase( POS, valueNode, new SqlNodeList( whenList, POS ), new SqlNodeList( thenList, POS ), elseNode );
                case DYNAMIC_PARAM:
                    final RexDynamicParam caseParam = (RexDynamicParam) rex;
                    SqlDynamicParam sqlDynamicParam = new SqlDynamicParam( (int) caseParam.getIndex(), POS );
                    if ( caseParam.getType() instanceof IntervalPolyType ) {
                        if ( dialect.getIntervalParameterStrategy() == IntervalParameterStrategy.MULTIPLICATION ) {
                            SqlIntervalQualifier intervalQualifier = (SqlIntervalQualifier) caseParam.getType().getIntervalQualifier();
                            return (SqlNode) OperatorRegistry.get( OperatorName.MULTIPLY ).createCall(
                                    POS,
                                    sqlDynamicParam,
                                    SqlLiteral.createInterval( PolyInterval.of( 1L, intervalQualifier ), intervalQualifier, POS ) );
                        } else if ( dialect.getIntervalParameterStrategy() == IntervalParameterStrategy.CAST ) {
                            return (SqlNode) OperatorRegistry.get( OperatorName.CAST ).createCall( POS, sqlDynamicParam, dialect.getCastSpec( caseParam.getType() ) );
                        } else if ( dialect.getIntervalParameterStrategy() == IntervalParameterStrategy.NONE ) {
                            return sqlDynamicParam;
                        } else {
                            throw new GenericRuntimeException( "Unknown IntervalParameterStrategy: " + dialect.getIntervalParameterStrategy().name() );
                        }
                    } else {
                        return (SqlNode) OperatorRegistry.get( OperatorName.CAST ).createCall( POS, sqlDynamicParam, dialect.getCastSpec( caseParam.getType() ) );
                    }
                case IN:
                    if ( rex instanceof RexSubQuery ) {
                        subQuery = (RexSubQuery) rex;
                        sqlSubQuery = implementor().visitChild( 0, subQuery.alg ).asQueryOrValues();
                        final List<RexNode> operands = subQuery.operands;
                        Node op0;
                        if ( operands.size() == 1 ) {
                            op0 = toSql( program, operands.get( 0 ) );
                        } else {
                            final List<SqlNode> cols = toSql( program, operands );
                            op0 = new SqlNodeList( cols, POS );
                        }
                        return (SqlNode) subQuery.getOperator().createCall( POS, op0, sqlSubQuery );
                    } else {
                        final RexCall call = (RexCall) rex;
                        final List<SqlNode> cols = toSql( program, call.operands );
                        return (SqlNode) call.getOperator().createCall( POS, cols.get( 0 ), new SqlNodeList( cols.subList( 1, cols.size() ), POS ) );
                    }

                case EXISTS:
                case SCALAR_QUERY:
                    subQuery = (RexSubQuery) rex;
                    sqlSubQuery = implementor().visitChild( 0, subQuery.alg ).asQueryOrValues();
                    return (SqlNode) subQuery.getOperator().createCall( POS, sqlSubQuery );

                case NOT:
                    RexNode operand = ((RexCall) rex).operands.get( 0 );
                    final Node node = toSql( program, operand );
                    return switch ( operand.getKind() ) {
                        case IN -> (SqlNode) OperatorRegistry.get( OperatorName.NOT_IN ).createCall( POS, ((SqlCall) node).getOperandList() );
                        case LIKE -> (SqlNode) OperatorRegistry.get( OperatorName.NOT_LIKE ).createCall( POS, ((SqlCall) node).getOperandList() );
                        case SIMILAR -> (SqlNode) OperatorRegistry.get( OperatorName.NOT_SIMILAR_TO ).createCall( POS, ((SqlCall) node).getOperandList() );
                        default -> (SqlNode) OperatorRegistry.get( OperatorName.NOT ).createCall( POS, node );
                    };
                default:
                    if ( rex instanceof RexOver ) {
                        return toSql( program, (RexOver) rex );
                    }

                    final RexCall call = (RexCall) stripCastFromString( rex );
                    final List<SqlNode> nodes = toSql( program, call.getOperands() );
                    Operator op = call.getOperator();
                    switch ( op.getKind() ) {
                        case SUM0:
                            op = OperatorRegistry.get( OperatorName.SUM );
                            break;
                        case IS_FALSE:
                            if ( !dialect.supportsIsBoolean() ) {
                                op = OperatorRegistry.get( OperatorName.NOT );
                            }
                            break;
                        case IS_TRUE:
                            if ( !dialect.supportsIsBoolean() ) {
                                assert nodes.size() == 1;
                                return nodes.get( 0 );
                            }
                    }
                    if ( Objects.requireNonNull( call.getKind() ) == Kind.CAST ) {
                        if ( ignoreCast ) {
                            assert nodes.size() == 1;
                            return nodes.get( 0 );
                        } else {
                            if ( call.getType().getComponentType() != null && !dialect.supportsNestedArrays() ) {
                                nodes.add( dialect.getCastSpec( call.getType().getComponentType() ) );
                            } else {
                                nodes.add( dialect.getCastSpec( call.getType() ) );
                            }
                        }
                    }
                    if ( op instanceof SqlBinaryOperator && nodes.size() > 2 ) {
                        // In RexNode trees, OR and AND have any number of children;
                        // SqlCall requires exactly 2. So, convert to a left-deep binary tree.
                        return createLeftCall( op, nodes );
                    }
                    if ( op instanceof LangFunctionOperator ) {
                        return toSql( program, call.operands.get( 0 ) );
                    }
                    return (SqlNode) op.createCall( new SqlNodeList( nodes, POS ) );
            }
        }


        protected Context getAliasContext( RexCorrelVariable variable ) {
            throw new UnsupportedOperationException();
        }


        private SqlCall toSql( RexProgram program, RexOver rexOver ) {
            final RexWindow rexWindow = rexOver.getWindow();
            final SqlNodeList partitionList = new SqlNodeList( toSql( program, rexWindow.partitionKeys ), POS );

            ImmutableList.Builder<SqlNode> orderNodes = ImmutableList.builder();
            if ( rexWindow.orderKeys != null ) {
                for ( RexFieldCollation rfc : rexWindow.orderKeys ) {
                    orderNodes.add( toSql( program, rfc ) );
                }
            }
            final SqlNodeList orderList = new SqlNodeList( orderNodes.build(), POS );

            final SqlLiteral isRows = SqlLiteral.createBoolean( rexWindow.isRows(), POS );

            // null defaults to true.
            // During parsing the allowPartial == false (e.g. disallow partial) is expand into CASE expression and is handled as a such.
            // Not sure if we can collapse this CASE expression back into "disallow partial" and set the allowPartial = false.
            final SqlLiteral allowPartial = null;

            SqlAggFunction sqlAggregateFunction = (SqlAggFunction) rexOver.getAggOperator();

            SqlNode lowerBound = null;
            SqlNode upperBound = null;

            if ( sqlAggregateFunction.allowsFraming() ) {
                lowerBound = createSqlWindowBound( rexWindow.getLowerBound() );
                upperBound = createSqlWindowBound( rexWindow.getUpperBound() );
            }

            final SqlWindow sqlWindow = SqlWindow.create( null, null, partitionList, orderList, isRows, lowerBound, upperBound, allowPartial, POS );

            final List<SqlNode> nodeList = toSql( program, rexOver.getOperands() );
            return createOverCall( sqlAggregateFunction, nodeList, sqlWindow );
        }


        private SqlCall createOverCall( SqlAggFunction op, List<SqlNode> operands, SqlWindow window ) {
            if ( op instanceof SqlSumEmptyIsZeroAggFunction ) {
                // Rewrite "SUM0(x) OVER w" to "COALESCE(SUM(x) OVER w, 0)"
                final SqlCall node = createOverCall( (SqlAggFunction) OperatorRegistry.getAgg( OperatorName.SUM ), operands, window );
                return (SqlCall) OperatorRegistry.get( OperatorName.COALESCE ).createCall( POS, node, SqlLiteral.createExactNumeric( "0", POS ) );
            }
            final SqlCall aggFunctionCall = (SqlCall) op.createCall( POS, operands );
            return (SqlCall) OperatorRegistry.get( OperatorName.OVER ).createCall( POS, aggFunctionCall, window );
        }


        private SqlNode toSql( RexProgram program, RexFieldCollation rfc ) {
            SqlNode node = toSql( program, rfc.left );
            node = switch ( rfc.getDirection() ) {
                case DESCENDING, STRICTLY_DESCENDING -> (SqlNode) OperatorRegistry.get( OperatorName.DESC ).createCall( POS, node );
                default -> node;
            };
            if ( rfc.getNullDirection() != dialect.defaultNullDirection( rfc.getDirection() ) ) {
                node = switch ( rfc.getNullDirection() ) {
                    case FIRST -> (SqlNode) OperatorRegistry.get( OperatorName.NULLS_FIRST ).createCall( POS, node );
                    case LAST -> (SqlNode) OperatorRegistry.get( OperatorName.NULLS_LAST ).createCall( POS, node );
                    default -> node;
                };
            }
            return node;
        }


        private SqlNode createSqlWindowBound( RexWindowBound rexWindowBound ) {
            if ( rexWindowBound.isCurrentRow() ) {
                return (SqlNode) SqlWindow.createCurrentRow( POS );
            }
            if ( rexWindowBound.isPreceding() ) {
                if ( rexWindowBound.isUnbounded() ) {
                    return (SqlNode) SqlWindow.createUnboundedPreceding( POS );
                } else {
                    Node literal = toSql( null, rexWindowBound.getOffset() );
                    return SqlWindow.createPreceding( (SqlNode) literal, POS );
                }
            }
            if ( rexWindowBound.isFollowing() ) {
                if ( rexWindowBound.isUnbounded() ) {
                    return (SqlNode) SqlWindow.createUnboundedFollowing( POS );
                } else {
                    Node literal = toSql( null, rexWindowBound.getOffset() );
                    return SqlWindow.createFollowing( (SqlNode) literal, POS );
                }
            }

            throw new AssertionError( "Unsupported Window bound: " + rexWindowBound );
        }


        private SqlNode createLeftCall( Operator op, List<SqlNode> nodeList ) {
            if ( nodeList.size() == 2 ) {
                return (SqlNode) op.createCall( new SqlNodeList( nodeList, POS ) );
            }
            final List<SqlNode> butLast = Util.skipLast( nodeList );
            final SqlNode last = nodeList.get( nodeList.size() - 1 );
            final SqlNode call = createLeftCall( op, butLast );
            return (SqlNode) op.createCall( new SqlNodeList( ImmutableList.of( call, last ), POS ) );
        }


        private List<SqlNode> toSql( RexProgram program, List<RexNode> operands ) {
            return new ArrayList<>( operands.stream().map( o -> toSql( program, o ) ).toList() );
        }


        void addOrderItem( List<SqlNode> orderByList, AlgFieldCollation field ) {
            if ( field.nullDirection != AlgFieldCollation.NullDirection.UNSPECIFIED ) {
                final boolean first = field.nullDirection == AlgFieldCollation.NullDirection.FIRST;
                SqlNode nullDirectionNode = dialect.emulateNullDirection( field( field.getFieldIndex() ), first, field.direction.isDescending() );
                if ( nullDirectionNode != null ) {
                    orderByList.add( nullDirectionNode );
                    field = new AlgFieldCollation( field.getFieldIndex(), field.getDirection(), AlgFieldCollation.NullDirection.UNSPECIFIED );
                }
            }
            orderByList.add( toSql( field ) );
        }


        /**
         * Converts a call to an aggregate function to an expression.
         */
        public SqlNode toSql( AggregateCall aggCall ) {
            final SqlOperator op = (SqlOperator) aggCall.getAggregation();
            final List<SqlNode> operandList = Expressions.list();
            for ( int arg : aggCall.getArgList() ) {
                operandList.add( field( arg ) );
            }
            final SqlLiteral qualifier = aggCall.isDistinct() ? SqlSelectKeyword.DISTINCT.symbol( POS ) : null;
            final SqlNode[] operands = operandList.toArray( SqlNode.EMPTY_ARRAY );
            List<SqlNode> orderByList = Expressions.list();
            for ( AlgFieldCollation field : aggCall.collation.getFieldCollations() ) {
                addOrderItem( orderByList, field );
            }
            SqlNodeList orderList = new SqlNodeList( orderByList, POS );
            if ( op instanceof SqlSumEmptyIsZeroAggFunction ) {
                final SqlNode node = withOrder( (SqlCall) OperatorRegistry.get( OperatorName.SUM ).createCall( qualifier, POS, operands ), orderList );
                return (SqlNode) OperatorRegistry.get( OperatorName.COALESCE ).createCall( POS, node, SqlLiteral.createExactNumeric( "0", POS ) );
            } else {
                return withOrder( (SqlCall) op.createCall( qualifier, POS, operands ), orderList );
            }
        }


        /**
         * Wraps a call in a {@link Kind#WITHIN_GROUP} call, if {@code orderList} is non-empty.
         */
        private SqlNode withOrder( SqlCall call, SqlNodeList orderList ) {
            if ( orderList == null || orderList.size() == 0 ) {
                return call;
            }
            return (SqlNode) OperatorRegistry.get( OperatorName.WITHIN_GROUP ).createCall( POS, call, orderList );
        }


        /**
         * Converts a collation to an ORDER BY item.
         */
        public SqlNode toSql( AlgFieldCollation collation ) {
            SqlNode node = field( collation.getFieldIndex() );
            node = switch ( collation.getDirection() ) {
                case DESCENDING, STRICTLY_DESCENDING -> (SqlNode) OperatorRegistry.get( OperatorName.DESC ).createCall( POS, node );
                default -> node;
            };
            if ( collation.nullDirection != dialect.defaultNullDirection( collation.direction ) ) {
                node = switch ( collation.nullDirection ) {
                    case FIRST -> (SqlNode) OperatorRegistry.get( OperatorName.NULLS_FIRST ).createCall( POS, node );
                    case LAST -> (SqlNode) OperatorRegistry.get( OperatorName.NULLS_LAST ).createCall( POS, node );
                    default -> node;
                };
            }
            return node;
        }


        public SqlImplementor implementor() {
            throw new UnsupportedOperationException();
        }

    }


    /**
     * Implementation of {@link Context} that has an enclosing {@link SqlImplementor} and can therefore
     * do non-trivial expressions.
     */
    protected abstract class BaseContext extends Context {

        BaseContext( SqlDialect dialect, int fieldCount ) {
            super( dialect, fieldCount );
        }


        @Override
        protected Context getAliasContext( RexCorrelVariable variable ) {
            return correlTableMap.get( variable.id );
        }


        @Override
        public SqlImplementor implementor() {
            return SqlImplementor.this;
        }

    }


    private static int computeFieldCount( Map<String, AlgDataType> aliases ) {
        int x = 0;
        for ( AlgDataType type : aliases.values() ) {
            x += type.getFieldCount();
        }
        return x;
    }


    public Context aliasContext( Map<String, AlgDataType> aliases, boolean qualified ) {
        return new AliasContext( dialect, aliases, qualified );
    }


    public Context joinContext( Context leftContext, Context rightContext ) {
        return new JoinContext( dialect, leftContext, rightContext );
    }


    public Context matchRecognizeContext( Context context ) {
        return new MatchRecognizeContext( dialect, ((AliasContext) context).aliases );
    }


    /**
     * Context for translating MATCH_RECOGNIZE clause
     */
    public class MatchRecognizeContext extends AliasContext {

        protected MatchRecognizeContext( SqlDialect dialect, Map<String, AlgDataType> aliases ) {
            super( dialect, aliases, false );
        }


        @Override
        public SqlNode toSql( RexProgram program, RexNode rex ) {
            if ( rex.getKind() == Kind.LITERAL ) {
                final RexLiteral literal = (RexLiteral) rex;
                if ( literal.getPolyType().getFamily() == PolyTypeFamily.CHARACTER ) {
                    return new SqlIdentifier( RexLiteral.stringValue( literal ).value, POS );
                }
            }
            return super.toSql( program, rex );
        }

    }


    /**
     * Implementation of Context that precedes field references with their "table alias" based on the current
     * sub-query's FROM clause.
     */
    public class AliasContext extends BaseContext {

        private final boolean qualified;
        private final Map<String, AlgDataType> aliases;


        /**
         * Creates an AliasContext; use {@link #aliasContext(Map, boolean)}.
         */
        protected AliasContext( SqlDialect dialect, Map<String, AlgDataType> aliases, boolean qualified ) {
            super( dialect, computeFieldCount( aliases ) );
            this.aliases = aliases;
            this.qualified = qualified;
        }


        @Override
        public SqlNode field( int ordinal ) {
            for ( Map.Entry<String, AlgDataType> alias : aliases.entrySet() ) {
                final List<AlgDataTypeField> fields = alias.getValue().getFields();
                if ( ordinal < fields.size() ) {
                    AlgDataTypeField field = fields.get( ordinal );
                    final SqlNode mappedSqlNode = ordinalMap.get( field.getName().toLowerCase( Locale.ROOT ) );
                    if ( mappedSqlNode != null ) {
                        return mappedSqlNode;
                    }
                    String physicalColumnName = field.getName();
                    if ( field.getPhysicalName() != null ) {
                        physicalColumnName = field.getPhysicalName();
                    }

                    return new SqlIdentifier(
                            !qualified
                                    ? ImmutableList.of( physicalColumnName )
                                    : ImmutableList.of( alias.getKey(), physicalColumnName ),
                            POS );
                }
                ordinal -= fields.size();
            }
            throw new AssertionError( "field ordinal " + ordinal + " out of range " + aliases );
        }


    }


    /**
     * Context for translating ON clause of a JOIN from {@link RexNode} to {@link SqlNode}.
     */
    class JoinContext extends BaseContext {

        private final SqlImplementor.Context leftContext;
        private final SqlImplementor.Context rightContext;


        /**
         * Creates a JoinContext; use {@link #joinContext(Context, Context)}.
         */
        private JoinContext( SqlDialect dialect, Context leftContext, Context rightContext ) {
            super( dialect, leftContext.fieldCount + rightContext.fieldCount );
            this.leftContext = leftContext;
            this.rightContext = rightContext;
        }


        @Override
        public SqlNode field( int ordinal ) {
            if ( ordinal < leftContext.fieldCount ) {
                return leftContext.field( ordinal );
            } else {
                return rightContext.field( ordinal - leftContext.fieldCount );
            }
        }

    }


    /**
     * Result of implementing a node.
     */
    public class Result {

        final SqlNode node;
        private final String neededAlias;
        private final AlgDataType neededType;
        private final Map<String, AlgDataType> aliases;
        final Expressions.FluentList<Clause> clauses;


        public Result( SqlNode node, Collection<Clause> clauses, String neededAlias, AlgDataType neededType, Map<String, AlgDataType> aliases ) {
            this.node = node;
            this.neededAlias = neededAlias;
            this.neededType = neededType;
            this.aliases = aliases;
            this.clauses = Expressions.list( clauses );
        }


        /**
         * Once you have a Result of implementing a child algebra expression, call this method to create a Builder to
         * implement the current algebra expression by adding additional clauses to the SQL query.
         * <p>
         * You need to declare which clauses you intend to add. If the clauses are "later", you can add to the same query.
         * For example, "GROUP BY" comes after "WHERE". But if they are the same or earlier, this method will
         * start a new SELECT that wraps the previous result.
         * <p>
         * When you have called {@link Builder#setSelect(SqlNodeList)}, {@link Builder#setWhere(SqlNode)} etc.
         * call {@link Builder#result(SqlNode, Collection, AlgNode, Map)} to fix the new query.
         *
         * @param alg Relational expression being implemented
         * @param clauses Clauses that will be generated to implement current algebra expression
         * @return A builder
         */
        public Builder builder( AlgNode alg, boolean explicitColumnNames, Clause... clauses ) {
            final Clause maxClause = maxClause();
            boolean needNew = false;
            // If old and new clause are equal and belong to below set, then new SELECT wrap is not required
            Set<Clause> nonWrapSet = ImmutableSet.of( Clause.SELECT );
            for ( Clause clause : clauses ) {
                if ( maxClause.ordinal() > clause.ordinal() || (maxClause == clause && !nonWrapSet.contains( clause )) ) {
                    needNew = true;
                    break;
                }
            }
            if ( alg instanceof LogicalRelAggregate
                    && !dialect.supportsNestedAggregations()
                    && hasNestedAggregations( (LogicalRelAggregate) alg ) ) {
                needNew = true;
            }

            SqlSelect select;
            Expressions.FluentList<Clause> clauseList = Expressions.list();
            if ( needNew ) {
                select = subSelect();
            } else {
                if ( explicitColumnNames && alg.getInputs().size() == 1 && alg.getInput( 0 ).unwrap( JdbcScan.class ).isPresent() ) {
                    select = asSelect( alg.getInput( 0 ).unwrap( JdbcScan.class ).get().getEntity().getNodeList() );
                } else {
                    select = asSelect();
                }
                clauseList.addAll( this.clauses );
            }
            clauseList.appendAll( clauses );
            Context newContext;
            final SqlNodeList selectList = select.getSqlSelectList();
            if ( selectList != null ) {
                newContext = new Context( dialect, selectList.size() ) {
                    @Override
                    public SqlNode field( int ordinal ) {
                        final Node selectItem = selectList.get( ordinal );
                        return switch ( selectItem.getKind() ) {
                            case AS -> ((SqlCall) selectItem).operand( 0 );
                            default -> (SqlNode) selectItem;
                        };
                    }
                };
            } else {
                boolean qualified = !dialect.hasImplicitTableAlias() || aliases.size() > 1;
                // Basically, we did a subSelect() since needNew is set and neededAlias is not null now, we need to make sure that we need to update the alias context.
                // If our aliases map has a single element:  <neededAlias, rowType>, then we don't need to rewrite the alias but otherwise, it should be updated.
                if ( needNew && neededAlias != null && (aliases.size() != 1 || !aliases.containsKey( neededAlias )) ) {
                    final Map<String, AlgDataType> newAliases = ImmutableMap.of( neededAlias, alg.getInput( 0 ).getTupleType() );
                    newContext = aliasContext( newAliases, qualified );
                } else {
                    newContext = aliasContext( aliases, qualified );
                }
            }
            return new Builder( alg, clauseList, select, newContext, needNew ? null : aliases );
        }


        private boolean hasNestedAggregations( LogicalRelAggregate alg ) {
            if ( node instanceof SqlSelect ) {
                final SqlNodeList selectList = ((SqlSelect) node).getSqlSelectList();
                if ( selectList != null ) {
                    final Set<Integer> aggregatesArgs = new HashSet<>();
                    for ( AggregateCall aggregateCall : alg.getAggCallList() ) {
                        aggregatesArgs.addAll( aggregateCall.getArgList() );
                    }
                    for ( int aggregatesArg : aggregatesArgs ) {
                        if ( selectList.get( aggregatesArg ) instanceof SqlBasicCall call ) {
                            for ( SqlNode operand : call.getOperands() ) {
                                if ( operand instanceof SqlCall && ((SqlCall) operand).getOperator() instanceof SqlAggFunction ) {
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
            return false;
        }


        // make private?
        public Clause maxClause() {
            Clause maxClause = null;
            for ( Clause clause : clauses ) {
                if ( maxClause == null || clause.ordinal() > maxClause.ordinal() ) {
                    maxClause = clause;
                }
            }
            assert maxClause != null;
            return maxClause;
        }


        /**
         * Returns a node that can be included in the FROM clause or a JOIN. It has an alias that is unique within the query.
         * The alias is implicit if it can be derived using the usual rules (For example, "SELECT * FROM emp" is
         * equivalent to "SELECT * FROM emp AS emp".)
         */
        public SqlNode asFrom() {
            if ( neededAlias != null ) {
                return (SqlNode) OperatorRegistry.get( OperatorName.AS ).createCall( POS, node, new SqlIdentifier( neededAlias, POS ) );
            }
            return node;
        }


        public SqlSelect subSelect() {
            return wrapSelect( asFrom() );
        }


        /**
         * Converts a non-query node into a SELECT node. Set operators (UNION, INTERSECT, EXCEPT) remain as is.
         */
        public SqlSelect asSelect() {
            return asSelect( null );
        }


        public SqlSelect asSelect( SqlNodeList sqlNodes ) {
            if ( node instanceof SqlSelect ) {
                return (SqlSelect) node;
            }
            if ( !dialect.hasImplicitTableAlias() ) {
                return wrapSelect( asFrom(), sqlNodes );
            }
            return wrapSelect( node, sqlNodes );
        }


        /**
         * Converts a non-query node into a SELECT node. Set operators (UNION, INTERSECT, EXCEPT)
         * and DML operators (INSERT, UPDATE, DELETE, MERGE) remain as is.
         */
        public SqlNode asStatement() {
            return switch ( node.getKind() ) {
                case UNION, INTERSECT, EXCEPT, INSERT, UPDATE, DELETE, MERGE -> node;
                default -> asSelect();
            };
        }


        /**
         * Converts a non-query node into a SELECT node. Set operators (UNION, INTERSECT, EXCEPT) and VALUES remain as is.
         */
        public SqlNode asQueryOrValues() {
            return switch ( node.getKind() ) {
                case UNION, INTERSECT, EXCEPT, VALUES -> node;
                default -> asSelect();
            };
        }


        /**
         * Returns a context that always qualifies identifiers. Useful if the Context deals with just one arm
         * of a join, yet we wish to generate a join condition that qualifies column names to disambiguate them.
         */
        public Context qualifiedContext() {
            return aliasContext( aliases, true );
        }


        /**
         * In join, when the left and right nodes have been generated, update their alias with 'neededAlias' if not null.
         */
        public Result resetAlias() {
            if ( neededAlias == null ) {
                return this;
            } else {
                return new Result( node, clauses, neededAlias, neededType, ImmutableMap.of( neededAlias, neededType ) );
            }
        }

    }


    /**
     * Builder.
     */
    public class Builder {

        private final AlgNode alg;
        final List<Clause> clauses;
        final SqlSelect select;
        public final Context context;
        private final Map<String, AlgDataType> aliases;


        public Builder( AlgNode alg, List<Clause> clauses, SqlSelect select, Context context, Map<String, AlgDataType> aliases ) {
            this.alg = alg;
            this.clauses = clauses;
            this.select = select;
            this.context = context;
            this.aliases = aliases;
        }


        public void setSelect( SqlNodeList nodeList ) {
            select.setSelectList( nodeList );
        }


        public void setWhere( SqlNode node ) {
            assert clauses.contains( Clause.WHERE );
            select.setWhere( node );
        }


        public void setGroupBy( SqlNodeList nodeList ) {
            assert clauses.contains( Clause.GROUP_BY );
            select.setGroupBy( nodeList );
        }


        public void setHaving( SqlNode node ) {
            assert clauses.contains( Clause.HAVING );
            select.setHaving( node );
        }


        public void setOrderBy( SqlNodeList nodeList ) {
            assert clauses.contains( Clause.ORDER_BY );
            select.setOrderBy( nodeList );
        }


        public void setFetch( SqlNode fetch ) {
            assert clauses.contains( Clause.FETCH );
            select.setFetch( fetch );
        }


        public void setOffset( SqlNode offset ) {
            assert clauses.contains( Clause.OFFSET );
            select.setOffset( offset );
        }


        public void addOrderItem( List<SqlNode> orderByList, AlgFieldCollation field ) {
            context.addOrderItem( orderByList, field );
        }


        public Result result() {
            return SqlImplementor.this.result( select, clauses, alg, aliases );
        }

    }


    /**
     * Clauses in a SQL query. Ordered by evaluation order.
     * SELECT is set only when there is a NON-TRIVIAL SELECT clause.
     */
    public enum Clause {
        FROM, WHERE, GROUP_BY, HAVING, SELECT, SET_OP, ORDER_BY, FETCH, OFFSET
    }

}

