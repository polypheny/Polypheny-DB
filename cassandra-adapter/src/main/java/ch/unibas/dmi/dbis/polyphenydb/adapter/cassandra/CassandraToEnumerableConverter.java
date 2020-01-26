/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra;


import ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.CassandraRel.CassandraImplementContext;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableRel;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableRelImplementor;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.JavaRowFormat;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.PhysType;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.PhysTypeImpl;
import ch.unibas.dmi.dbis.polyphenydb.plan.ConventionTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCost;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.convert.ConverterImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schemas;
import ch.unibas.dmi.dbis.polyphenydb.util.BuiltInMethod;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;


/**
 * Relational expression representing a scan of a table in a Cassandra data source.
 */
@Slf4j
public class CassandraToEnumerableConverter extends ConverterImpl implements EnumerableRel {

    public CassandraToEnumerableConverter( RelOptCluster cluster, RelTraitSet traits, RelNode input ) {
        super( cluster, ConventionTraitDef.INSTANCE, traits, input );
    }


    @Override
    public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        return new CassandraToEnumerableConverter( getCluster(), traitSet, sole( inputs ) );
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( .1 );
    }


    @Override
    public Result implement( EnumerableRelImplementor implementor, Prefer pref ) {
        // Generates a call to "query" with the appropriate fields and predicates
        final BlockBuilder list = new BlockBuilder();
        final CassandraImplementContext cassandraContext = new CassandraImplementContext();
        cassandraContext.visitChild( 0, getInput() );
        final CassandraConvention convention = (CassandraConvention) getInput().getConvention();
        final RelDataType rowType = getRowType();
        final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), rowType, pref.prefer( JavaRowFormat.ARRAY ) );

        String cqlString;
        switch ( cassandraContext.type ) {
            case SELECT:
                SelectFrom selectFrom = QueryBuilder.selectFrom( cassandraContext.cassandraTable.getPhysicalName() );
                Select select;
                // Construct the list of fields to project
                if ( cassandraContext.selectFields.isEmpty() ) {
                    select = selectFrom.all();
                } else {
                    select = selectFrom.selectors( cassandraContext.selectFields );
                }

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

        Expression enumerable;

        final Expression simpleStatement = list.append( "statement", Expressions.constant( cqlString ) );
        final Expression cqlSession_ = list.append( "cqlSession",
                Expressions.call(
                        Schemas.unwrap( convention.expression, CassandraSchema.class ),
                        "getSession" ) );

        enumerable = list.append(
                "enumerable",
                Expressions.call(
                        CassandraMethod.CASSANDRA_STRING_ENUMERABLE.method,
                        cqlSession_,
                        simpleStatement
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

