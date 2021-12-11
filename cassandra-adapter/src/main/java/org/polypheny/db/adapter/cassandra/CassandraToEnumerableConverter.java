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

package org.polypheny.db.adapter.cassandra;


import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.cassandra.CassandraAlg.CassandraImplementContext;
import org.polypheny.db.adapter.cassandra.rules.CassandraRules;
import org.polypheny.db.adapter.enumerable.EnumerableAlg;
import org.polypheny.db.adapter.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.adapter.enumerable.JavaRowFormat;
import org.polypheny.db.adapter.enumerable.PhysType;
import org.polypheny.db.adapter.enumerable.PhysTypeImpl;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterImpl;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.util.BuiltInMethod;


/**
 * Relational expression representing a scan of a table in a Cassandra data source.
 */
@Slf4j
public class CassandraToEnumerableConverter extends ConverterImpl implements EnumerableAlg {

    public CassandraToEnumerableConverter( AlgOptCluster cluster, AlgTraitSet traits, AlgNode input ) {
        super( cluster, ConventionTraitDef.INSTANCE, traits, input );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new CassandraToEnumerableConverter( getCluster(), traitSet, sole( inputs ) );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( .1 );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        // Generates a call to "query" with the appropriate fields and predicates
        final BlockBuilder list = new BlockBuilder();
        final CassandraImplementContext cassandraContext = new CassandraImplementContext();
        cassandraContext.visitChild( 0, getInput() );
        final CassandraConvention convention = (CassandraConvention) getInput().getConvention();
        final AlgDataType rowType = getRowType();
        final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), rowType, pref.prefer( JavaRowFormat.ARRAY ) );

        String cqlString;
        switch ( cassandraContext.type ) {
            case SELECT:
                SelectFrom selectFrom = QueryBuilder.selectFrom( cassandraContext.cassandraTable.getPhysicalName() );
                Select select;
                // Construct the list of fields to project
                if ( cassandraContext.selectFields.isEmpty() ) {
                    List<String> physicalNames = CassandraRules.cassandraPhysicalFieldNames( getRowType() );
                    for ( String physicalName : physicalNames ) {
                        cassandraContext.selectFields.add( Selector.column( physicalName ) );
                    }
                }
                select = selectFrom.selectors( cassandraContext.selectFields );

                select = select.where( cassandraContext.whereClause );
                // FIXME js: Horrible hack, but hopefully works for now till I understand everything better.
                Map<String, ClusteringOrder> orderMap = new LinkedHashMap<>();
                for ( Map.Entry<String, ClusteringOrder> entry : cassandraContext.order.entrySet() ) {
                    orderMap.put( entry.getKey(), entry.getValue() );
                }

                select = select.orderBy( orderMap );
                int limit = cassandraContext.offset;
                if ( cassandraContext.fetch >= 0 ) {
                    limit += cassandraContext.fetch;
                }
                if ( limit > 0 ) {
                    select = select.limit( limit );
                }

                select = select.allowFiltering();
                cqlString = select.build().getQuery();
                break;
            case INSERT:
                if ( cassandraContext.insertValues.size() == 1 ) {
                    InsertInto insertInto = QueryBuilder.insertInto( cassandraContext.cassandraTable.getPhysicalName() );
                    RegularInsert insert = insertInto.values( cassandraContext.insertValues.get( 0 ) );
                    cqlString = insert.build().getQuery();
                } else {
//                    List<SimpleStatement> statements = new ArrayList<>(  );
                    StringJoiner joiner = new StringJoiner( ";", "BEGIN BATCH ", " APPLY BATCH;" );
                    for ( Map<String, Term> insertValue : cassandraContext.insertValues ) {
                        InsertInto insertInto = QueryBuilder.insertInto( cassandraContext.cassandraTable.getPhysicalName() );

                        joiner.add( insertInto.values( insertValue ).build().getQuery() );
                    }

                    cqlString = joiner.toString();
                }
                break;
            case UPDATE:
                cqlString = QueryBuilder.update( cassandraContext.cassandraTable.getPhysicalName() )
                        .set( cassandraContext.setAssignments )
                        .where( cassandraContext.whereClause )
                        .build()
                        .getQuery();
                break;
            case DELETE:
                cqlString = QueryBuilder.deleteFrom( cassandraContext.cassandraTable.getPhysicalName() )
                        .where( cassandraContext.whereClause )
                        .build()
                        .getQuery();
                break;
            default:
                cqlString = "";
        }

        list.add( Expressions.statement( Expressions.call(
                Schemas.unwrap( convention.expression, CassandraSchema.class ),
                "registerStore",
                DataContext.ROOT ) ) );

        Expression enumerable;

        final Expression simpleStatement = list.append( "statement", Expressions.constant( cqlString ) );
        final Expression cqlSession_ = list.append(
                "cqlSession",
                Expressions.call(
                        Schemas.unwrap( convention.expression, CassandraSchema.class ),
                        "getSession" ) );

        enumerable = list.append(
                "enumerable",
                Expressions.call(
                        CassandraMethod.CASSANDRA_STRING_ENUMERABLE_OFFSET.method,
                        cqlSession_,
                        simpleStatement,
                        Expressions.constant( cassandraContext.offset )
                ) );
        list.add( Expressions.return_( null, enumerable ) );

        return implementor.result( physType, list.toBlock() );
    }


    /**
     * E.g. {@code constantArrayList("x", "y")} returns "Arrays.asList('x', 'y')".
     */
    private static <T> MethodCallExpression constantArrayList( List<T> values, Class clazz ) {
        return Expressions.call( BuiltInMethod.ARRAYS_AS_LIST.method, Expressions.newArrayInit( clazz, constantList( values ) ) );
    }


    /**
     * E.g. {@code constantList("x", "y")} returns {@code {ConstantExpression("x"), ConstantExpression("y")}}.
     */
    private static <T> List<Expression> constantList( List<T> values ) {
        return values.stream().map( Expressions::constant ).collect( Collectors.toList() );
    }

}

