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

package org.polypheny.db.adapter.jdbc.rel2sql;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedSet;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.JoinConditionType;
import org.polypheny.db.algebra.constant.JoinType;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.Calc;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Intersect;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.Match;
import org.polypheny.db.algebra.core.Minus;
import org.polypheny.db.algebra.core.Modify;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Scan;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexLocalRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlDelete;
import org.polypheny.db.sql.language.SqlDialect;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlInsert;
import org.polypheny.db.sql.language.SqlIntervalLiteral;
import org.polypheny.db.sql.language.SqlJoin;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlMatchRecognize;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlSelect;
import org.polypheny.db.sql.language.SqlSetOperator;
import org.polypheny.db.sql.language.SqlUpdate;
import org.polypheny.db.sql.language.fun.SqlRowOperator;
import org.polypheny.db.sql.language.fun.SqlSingleValueAggFunction;
import org.polypheny.db.sql.language.validate.SqlValidatorUtil;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.ReflectUtil;
import org.polypheny.db.util.ReflectiveVisitor;


/**
 * Utility to convert relational expressions to SQL abstract syntax tree.
 */
public abstract class AlgToSqlConverter extends SqlImplementor implements ReflectiveVisitor {

    /**
     * Similar to {@link OperatorRegistry ROW }, but does not print "ROW".
     */
    private static final SqlRowOperator ANONYMOUS_ROW = new SqlRowOperator( " " );

    private final ReflectUtil.MethodDispatcher<Result> dispatcher;

    private final Deque<Frame> stack = new ArrayDeque<>();

    private boolean isUnion = false;


    /**
     * Creates a AlgToSqlConverter.
     */
    public AlgToSqlConverter( SqlDialect dialect ) {
        super( dialect );
        dispatcher = ReflectUtil.createMethodDispatcher( Result.class, this, "visit", AlgNode.class );
    }


    /**
     * Dispatches a call to the {@code visit(Xxx e)} method where {@code Xxx} most closely matches the runtime type of the argument.
     */
    protected Result dispatch( AlgNode e ) {
        return dispatcher.invoke( e );
    }


    @Override
    public Result visitChild( int i, AlgNode e ) {
        try {
            stack.push( new Frame( i, e ) );
            return dispatch( e );
        } finally {
            stack.pop();
        }
    }


    /**
     * @see #dispatch
     */
    public Result visit( AlgNode e ) {
        throw new AssertionError( "Need to implement " + e.getClass().getName() );
    }


    /**
     * @see #dispatch
     */
    public Result visit( Join e ) {
        final Result leftResult = visitChild( 0, e.getLeft() ).resetAlias();
        final Result rightResult = visitChild( 1, e.getRight() ).resetAlias();
        final Context leftContext = leftResult.qualifiedContext();
        final Context rightContext = rightResult.qualifiedContext();
        SqlNode sqlCondition = null;
        SqlLiteral condType = SqlLiteral.createSymbol( JoinConditionType.ON, POS );
        JoinType joinType = joinType( e.getJoinType() );
        if ( e.getJoinType() == JoinAlgType.INNER && e.getCondition().isAlwaysTrue() ) {
            joinType = JoinType.COMMA;
            condType = SqlLiteral.createSymbol( JoinConditionType.NONE, POS );
        } else {
            sqlCondition = convertConditionToSqlNode(
                    e.getCondition(),
                    leftContext,
                    rightContext,
                    e.getLeft().getRowType().getFieldCount() );
        }
        SqlNode join =
                new SqlJoin(
                        POS,
                        leftResult.asFrom(),
                        SqlLiteral.createBoolean( false, POS ),
                        SqlLiteral.createSymbol( joinType, POS ),
                        rightResult.asFrom(),
                        condType,
                        sqlCondition );
        return result( join, leftResult, rightResult );
    }


    /**
     * @see #dispatch
     */
    public Result visit( Filter e ) {
        final AlgNode input = e.getInput();
        Result x = visitChild( 0, input );
        parseCorrelTable( e, x );
        if ( input instanceof Aggregate ) {
            final Builder builder;
            if ( ((Aggregate) input).getInput() instanceof Project ) {
                builder = x.builder( e, true );
                builder.clauses.add( Clause.HAVING );
            } else {
                builder = x.builder( e, true, Clause.HAVING );
            }
            builder.setHaving( builder.context.toSql( null, e.getCondition() ) );
            return builder.result();
        } else {
            final Builder builder = x.builder( e, isUnion, Clause.WHERE );
            builder.setWhere( builder.context.toSql( null, e.getCondition() ) );
            return builder.result();
        }
    }


    /**
     * @see #dispatch
     */
    public Result visit( Project e ) {
        Result x = visitChild( 0, e.getInput() );
        parseCorrelTable( e, x );
        if ( isStar( e.getChildExps(), e.getInput().getRowType(), e.getRowType() ) ) {
            return x;
        }
        final Builder builder = x.builder( e, false, Clause.SELECT );
        final List<SqlNode> selectList = new ArrayList<>();
        for ( RexNode ref : e.getChildExps() ) {
            SqlNode sqlExpr = builder.context.toSql( null, ref );
            addSelect( selectList, sqlExpr, e.getRowType() );
        }

        builder.setSelect( new SqlNodeList( selectList, POS ) );
        return builder.result();
    }


    /**
     * @see #dispatch
     */
    public Result visit( Aggregate e ) {
        // "select a, b, sum(x) from ( ... ) group by a, b"
        final Result x = visitChild( 0, e.getInput() );
        final Builder builder;
        if ( e.getInput() instanceof Project ) {
            builder = x.builder( e, true );
            builder.clauses.add( Clause.GROUP_BY );
        } else {
            builder = x.builder( e, true, Clause.GROUP_BY );
        }
        List<SqlNode> groupByList = Expressions.list();
        final List<SqlNode> selectList = new ArrayList<>();
        for ( int group : e.getGroupSet() ) {
            final SqlNode field = builder.context.field( group );
            addSelect( selectList, field, e.getRowType() );
            groupByList.add( field );
        }
        for ( AggregateCall aggCall : e.getAggCallList() ) {
            SqlNode aggCallSqlNode = builder.context.toSql( aggCall );
            if ( aggCall.getAggregation() instanceof SqlSingleValueAggFunction ) {
                aggCallSqlNode = dialect.rewriteSingleValueExpr( aggCallSqlNode );
            }
            addSelect( selectList, aggCallSqlNode, e.getRowType() );
        }
        builder.setSelect( new SqlNodeList( selectList, POS ) );
        if ( !groupByList.isEmpty() || e.getAggCallList().isEmpty() ) {
            // Some databases don't support "GROUP BY ()". We can omit it as long as there is at least one aggregate function.
            builder.setGroupBy( new SqlNodeList( groupByList, POS ) );
        }
        return builder.result();
    }


    /**
     * @see #dispatch
     */
    public Result visit( Scan e ) {
        //final SqlIdentifier identifier = getPhysicalTableName( e.getTable().getQualifiedName() );
        return result(
                new SqlIdentifier( e.getTable().getQualifiedName(), ParserPos.ZERO ),
                ImmutableList.of( Clause.FROM ),
                e,
                null );
    }


    /**
     * @see #dispatch
     */
    public Result visit( Union e ) {
        isUnion = true;
        Result result = setOpToSql( (SqlSetOperator) (e.all
                ? OperatorRegistry.get( OperatorName.UNION_ALL )
                : OperatorRegistry.get( OperatorName.UNION )), e );
        isUnion = false;
        return result;
    }


    /**
     * @see #dispatch
     */
    public Result visit( Intersect e ) {
        return setOpToSql( (SqlSetOperator) (e.all
                ? OperatorRegistry.get( OperatorName.INTERSECT_ALL )
                : OperatorRegistry.get( OperatorName.INTERSECT )), e );
    }


    /**
     * @see #dispatch
     */
    public Result visit( Minus e ) {
        return setOpToSql( (SqlSetOperator) (e.all
                ? OperatorRegistry.get( OperatorName.EXCEPT_ALL )
                : OperatorRegistry.get( OperatorName.EXCEPT )), e );
    }


    /**
     * @see #dispatch
     */
    public Result visit( Calc e ) {
        Result x = visitChild( 0, e.getInput() );
        parseCorrelTable( e, x );
        final RexProgram program = e.getProgram();
        Builder builder =
                program.getCondition() != null
                        ? x.builder( e, true, Clause.WHERE )
                        : x.builder( e, true );
        if ( !isStar( program ) ) {
            final List<SqlNode> selectList = new ArrayList<>();
            for ( RexLocalRef ref : program.getProjectList() ) {
                SqlNode sqlExpr = builder.context.toSql( program, ref );
                addSelect( selectList, sqlExpr, e.getRowType() );
            }
            builder.setSelect( new SqlNodeList( selectList, POS ) );
        }

        if ( program.getCondition() != null ) {
            builder.setWhere( builder.context.toSql( program, program.getCondition() ) );
        }
        return builder.result();
    }


    /**
     * @see #dispatch
     */
    public Result visit( Values e ) {
        final List<Clause> clauses = ImmutableList.of( Clause.SELECT );
        final Map<String, AlgDataType> pairs = ImmutableMap.of();
        final Context context = aliasContext( pairs, false );
        SqlNode query;
        final boolean rename = stack.size() <= 1 || !(Iterables.get( stack, 1 ).r instanceof Modify);
        final List<String> fieldNames = e.getRowType().getFieldNames();
        if ( !dialect.supportsAliasedValues() && rename ) {
            // Oracle does not support "AS t (c1, c2)". So instead of
            //   (VALUES (v0, v1), (v2, v3)) AS t (c0, c1)
            // we generate
            //   SELECT v0 AS c0, v1 AS c1 FROM DUAL
            //   UNION ALL
            //   SELECT v2 AS c0, v3 AS c1 FROM DUAL
            List<SqlSelect> list = new ArrayList<>();
            for ( List<RexLiteral> tuple : e.getTuples() ) {
                final List<SqlNode> values2 = new ArrayList<>();
                final SqlNodeList exprList = exprList( context, tuple );
                for ( Pair<Node, String> value : Pair.zip( exprList, fieldNames ) ) {
                    values2.add( (SqlNode) OperatorRegistry.get( OperatorName.AS ).createCall( POS, value.left, new SqlIdentifier( value.right, POS ) ) );
                }
                list.add(
                        new SqlSelect(
                                POS,
                                null,
                                new SqlNodeList( values2, POS ),
                                new SqlIdentifier( "DUAL", POS ),
                                null,
                                null,
                                null,
                                null,
                                null,
                                null,
                                null ) );
            }
            if ( list.size() == 1 ) {
                query = list.get( 0 );
            } else {
                query = (SqlNode) OperatorRegistry.get( OperatorName.UNION_ALL ).createCall( new SqlNodeList( list, POS ) );
            }
        } else {
            // Generate ANSI syntax
            //   (VALUES (v0, v1), (v2, v3))
            // or, if rename is required
            //   (VALUES (v0, v1), (v2, v3)) AS t (c0, c1)
            final SqlNodeList selects = new SqlNodeList( POS );
            for ( List<RexLiteral> tuple : e.getTuples() ) {
                selects.add( ANONYMOUS_ROW.createCall( exprList( context, tuple ) ) );
            }
            query = (SqlNode) OperatorRegistry.get( OperatorName.VALUES ).createCall( selects );
            if ( rename ) {
                final List<SqlNode> list = new ArrayList<>();
                list.add( query );
                list.add( new SqlIdentifier( "t", POS ) );
                for ( String fieldName : fieldNames ) {
                    list.add( new SqlIdentifier( fieldName, POS ) );
                }
                query = (SqlNode) OperatorRegistry.get( OperatorName.AS ).createCall( POS, list );
            }
        }
        return result( query, clauses, e, null );
    }


    /**
     * @see #dispatch
     */
    public Result visit( Sort e ) {
        Result x = visitChild( 0, e.getInput() );
        Builder builder = x.builder( e, false, Clause.ORDER_BY );
        if ( stack.size() != 1 && builder.select.getSqlSelectList() == null ) {
            // Generates explicit column names instead of start(*) for non-root ORDER BY to avoid ambiguity.
            final List<SqlNode> selectList = Expressions.list();
            for ( AlgDataTypeField field : e.getRowType().getFieldList() ) {
                addSelect( selectList, builder.context.field( field.getIndex() ), e.getRowType() );
            }
            builder.select.setSelectList( new SqlNodeList( selectList, POS ) );
        }
        List<SqlNode> orderByList = Expressions.list();
        for ( AlgFieldCollation field : e.getCollation().getFieldCollations() ) {
            builder.addOrderItem( orderByList, field );
        }
        if ( !orderByList.isEmpty() ) {
            builder.setOrderBy( new SqlNodeList( orderByList, POS ) );
            x = builder.result();
        }
        if ( e.fetch != null ) {
            builder = x.builder( e, false, Clause.FETCH );
            builder.setFetch( (SqlNode) builder.context.toSql( null, e.fetch ) );
            x = builder.result();
        }
        if ( e.offset != null ) {
            builder = x.builder( e, false, Clause.OFFSET );
            builder.setOffset( (SqlNode) builder.context.toSql( null, e.offset ) );
            x = builder.result();
        }
        return x;
    }


    /**
     * @see #dispatch
     */
    public Result visit( Modify modify ) {
        final Map<String, AlgDataType> pairs = ImmutableMap.of();
        final Context context = aliasContext( pairs, false );

        // Target Table Name
        //final SqlIdentifier sqlTargetTable = new SqlIdentifier( modify.getTable().getQualifiedName(), POS );
        final SqlIdentifier sqlTargetTable = getPhysicalTableName( modify.getTable().getQualifiedName() );

        switch ( modify.getOperation() ) {
            case INSERT: {
                // Convert the input to a SELECT query or keep as VALUES. Not all dialects support naked VALUES,
                // but all support VALUES inside INSERT.
                final SqlNode sqlSource = visitChild( 0, modify.getInput() ).asQueryOrValues();
                final SqlInsert sqlInsert = new SqlInsert(
                        POS,
                        SqlNodeList.EMPTY,
                        sqlTargetTable,
                        sqlSource,
                        physicalIdentifierList(
                                modify.getTable().getQualifiedName(),
                                modify.getInput().getRowType().getFieldNames() ) );
                return result( sqlInsert, ImmutableList.of(), modify, null );
            }
            case UPDATE: {
                final Result input = visitChild( 0, modify.getInput() );
                final SqlUpdate sqlUpdate = new SqlUpdate(
                        POS,
                        sqlTargetTable,
                        physicalIdentifierList( modify.getTable().getQualifiedName(), modify.getUpdateColumnList() ),
                        exprList( context, modify.getSourceExpressionList() ),
                        ((SqlSelect) input.node).getWhere(),
                        input.asSelect(),
                        null );
                return result( sqlUpdate, input.clauses, modify, null );
            }
            case DELETE: {
                final Result input = visitChild( 0, modify.getInput() );
                final SqlDelete sqlDelete = new SqlDelete(
                        POS,
                        sqlTargetTable,
                        input.asSelect().getWhere(),
                        input.asSelect(),
                        null );
                return result( sqlDelete, input.clauses, modify, null );
            }
            case MERGE:
            default:
                throw new AssertionError( "not implemented: " + modify );
        }
    }


    /**
     * Converts a list of {@link RexNode} expressions to {@link SqlNode} expressions.
     */
    private SqlNodeList exprList( final Context context, List<? extends RexNode> exprs ) {
        return new SqlNodeList( exprs.stream().map( e -> context.toSql( null, e ) ).collect( Collectors.toList() ), POS );
    }


    /**
     * Converts a list of names expressions to a list of single-part {@link SqlIdentifier}s.
     */
    private SqlNodeList identifierList( List<String> names ) {
        return new SqlNodeList( names.stream().map( name -> new SqlIdentifier( name, POS ) ).collect( Collectors.toList() ), POS );
    }


    /**
     * Converts a list of names expressions to a list of single-part {@link SqlIdentifier}s.
     */
    private SqlNodeList physicalIdentifierList( List<String> tableName, List<String> columnNames ) {
        return new SqlNodeList( columnNames.stream().map( columnName -> getPhysicalColumnName( tableName, columnName ) ).collect( Collectors.toList() ), POS );
    }


    /**
     * @see #dispatch
     */
    public Result visit( Match e ) {
        final AlgNode input = e.getInput();
        final Result x = visitChild( 0, input );
        final Context context = matchRecognizeContext( x.qualifiedContext() );

        SqlNode tableRef = x.asQueryOrValues();

        final List<SqlNode> partitionSqlList = new ArrayList<>();
        if ( e.getPartitionKeys() != null ) {
            for ( RexNode rex : e.getPartitionKeys() ) {
                SqlNode sqlNode = context.toSql( null, rex );
                partitionSqlList.add( sqlNode );
            }
        }
        final SqlNodeList partitionList = new SqlNodeList( partitionSqlList, POS );

        final List<SqlNode> orderBySqlList = new ArrayList<>();
        if ( e.getOrderKeys() != null ) {
            for ( AlgFieldCollation fc : e.getOrderKeys().getFieldCollations() ) {
                if ( fc.nullDirection != AlgFieldCollation.NullDirection.UNSPECIFIED ) {
                    boolean first = fc.nullDirection == AlgFieldCollation.NullDirection.FIRST;
                    SqlNode nullDirectionNode = dialect.emulateNullDirection( context.field( fc.getFieldIndex() ), first, fc.direction.isDescending() );
                    if ( nullDirectionNode != null ) {
                        orderBySqlList.add( nullDirectionNode );
                        fc = new AlgFieldCollation( fc.getFieldIndex(), fc.getDirection(), AlgFieldCollation.NullDirection.UNSPECIFIED );
                    }
                }
                orderBySqlList.add( context.toSql( fc ) );
            }
        }
        final SqlNodeList orderByList = new SqlNodeList( orderBySqlList, ParserPos.ZERO );

        final SqlLiteral rowsPerMatch = e.isAllRows()
                ? SqlMatchRecognize.RowsPerMatchOption.ALL_ROWS.symbol( POS )
                : SqlMatchRecognize.RowsPerMatchOption.ONE_ROW.symbol( POS );

        final SqlNode after;
        if ( e.getAfter() instanceof RexLiteral ) {
            SqlMatchRecognize.AfterOption value = (SqlMatchRecognize.AfterOption) ((RexLiteral) e.getAfter()).getValue2();
            after = SqlLiteral.createSymbol( value, POS );
        } else {
            RexCall call = (RexCall) e.getAfter();
            String operand = RexLiteral.stringValue( call.getOperands().get( 0 ) );
            after = (SqlNode) call.getOperator().createCall( POS, new SqlIdentifier( operand, POS ) );
        }

        RexNode rexPattern = e.getPattern();
        final SqlNode pattern = context.toSql( null, rexPattern );
        final SqlLiteral strictStart = SqlLiteral.createBoolean( e.isStrictStart(), POS );
        final SqlLiteral strictEnd = SqlLiteral.createBoolean( e.isStrictEnd(), POS );

        RexLiteral rexInterval = (RexLiteral) e.getInterval();
        SqlIntervalLiteral interval = null;
        if ( rexInterval != null ) {
            interval = (SqlIntervalLiteral) context.toSql( null, rexInterval );
        }

        final SqlNodeList subsetList = new SqlNodeList( POS );
        for ( Map.Entry<String, SortedSet<String>> entry : e.getSubsets().entrySet() ) {
            SqlNode left = new SqlIdentifier( entry.getKey(), POS );
            List<SqlNode> rhl = new ArrayList<>();
            for ( String right : entry.getValue() ) {
                rhl.add( new SqlIdentifier( right, POS ) );
            }
            subsetList.add( OperatorRegistry.get( OperatorName.EQUALS ).createCall( POS, left, new SqlNodeList( rhl, POS ) ) );
        }

        final SqlNodeList measureList = new SqlNodeList( POS );
        for ( Map.Entry<String, RexNode> entry : e.getMeasures().entrySet() ) {
            final String alias = entry.getKey();
            final SqlNode sqlNode = context.toSql( null, entry.getValue() );
            measureList.add( as( sqlNode, alias ) );
        }

        final SqlNodeList patternDefList = new SqlNodeList( POS );
        for ( Map.Entry<String, RexNode> entry : e.getPatternDefinitions().entrySet() ) {
            final String alias = entry.getKey();
            final SqlNode sqlNode = context.toSql( null, entry.getValue() );
            patternDefList.add( as( sqlNode, alias ) );
        }

        final SqlNode matchRecognize =
                new SqlMatchRecognize(
                        POS,
                        tableRef,
                        pattern,
                        strictStart,
                        strictEnd,
                        patternDefList,
                        measureList,
                        after,
                        subsetList,
                        rowsPerMatch,
                        partitionList,
                        orderByList,
                        interval );
        return result( matchRecognize, Expressions.list( Clause.FROM ), e, null );
    }


    private SqlCall as( SqlNode e, String alias ) {
        return (SqlCall) OperatorRegistry.get( OperatorName.AS ).createCall( POS, e, new SqlIdentifier( alias, POS ) );
    }


    @Override
    public void addSelect( List<SqlNode> selectList, SqlNode node, AlgDataType rowType ) {
        //String name = rowType.getFieldNames().get( selectList.size() );
        String name = rowType.getFieldList().get( selectList.size() ).getPhysicalName();
        if ( name == null ) {
            name = rowType.getFieldList().get( selectList.size() ).getName();
        }
        String alias = SqlValidatorUtil.getAlias( node, -1 );
        final String lowerName = name.toLowerCase( Locale.ROOT );
        if ( lowerName.startsWith( "expr$" ) ) {
            // Put it in ordinalMap
            ordinalMap.put( lowerName, node );
        } else if ( alias == null || !alias.equals( name ) ) {
            node = as( node, name );
        }
        selectList.add( node );
    }


    private void parseCorrelTable( AlgNode algNode, Result x ) {
        for ( CorrelationId id : algNode.getVariablesSet() ) {
            correlTableMap.put( id, x.qualifiedContext() );
        }
        for ( CorrelationId id : algNode.getCluster().getMapCorrelToAlg().keySet() ) {
            correlTableMap.putIfAbsent( id, x.qualifiedContext() );
        }
    }


    public abstract SqlIdentifier getPhysicalTableName( List<String> tableName );


    public abstract SqlIdentifier getPhysicalColumnName( List<String> tableName, String columnName );


    /**
     * Stack frame.
     */
    private static class Frame {

        private final int ordinalInParent;
        private final AlgNode r;


        Frame( int ordinalInParent, AlgNode r ) {
            this.ordinalInParent = ordinalInParent;
            this.r = r;
        }

    }


    public static class PlainAlgToSqlConverter extends AlgToSqlConverter {

        /**
         * Creates a AlgToSqlConverter.
         */
        public PlainAlgToSqlConverter( SqlDialect dialect ) {
            super( dialect );
        }


        @Override
        public SqlIdentifier getPhysicalTableName( List<String> tableNames ) {
            return new SqlIdentifier( tableNames, POS );
        }


        @Override
        public SqlIdentifier getPhysicalColumnName( List<String> tableName, String columnName ) {
            return new SqlIdentifier( columnName, POS );
        }

    }

}

