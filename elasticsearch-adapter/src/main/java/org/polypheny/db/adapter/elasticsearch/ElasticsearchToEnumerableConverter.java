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

package org.polypheny.db.adapter.elasticsearch;


import java.util.AbstractList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
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
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Pair;


/**
 * Relational expression representing a scan of a table in an Elasticsearch data source.
 */
public class ElasticsearchToEnumerableConverter extends ConverterImpl implements EnumerableAlg {

    ElasticsearchToEnumerableConverter( AlgOptCluster cluster, AlgTraitSet traits, AlgNode input ) {
        super( cluster, ConventionTraitDef.INSTANCE, traits, input );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new ElasticsearchToEnumerableConverter( getCluster(), traitSet, sole( inputs ) );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( .1 );
    }


    @Override
    public Result implement( EnumerableAlgImplementor algImplementor, Prefer prefer ) {
        final BlockBuilder block = new BlockBuilder();
        final ElasticsearchRel.Implementor implementor = new ElasticsearchRel.Implementor();
        implementor.visitChild( 0, getInput() );

        final AlgDataType rowType = getRowType();
        final PhysType physType = PhysTypeImpl.of( algImplementor.getTypeFactory(), rowType, prefer.prefer( JavaRowFormat.ARRAY ) );
        final Expression fields = block.append(
                "fields",
                constantArrayList(
                        Pair.zip(
                                ElasticsearchRules.elasticsearchFieldNames( rowType ),
                                new AbstractList<Class>() {
                                    @Override
                                    public Class get( int index ) {
                                        return physType.fieldClass( index );
                                    }


                                    @Override
                                    public int size() {
                                        return rowType.getFieldCount();
                                    }
                                } ),
                        Pair.class ) );
        final Expression table = block.append( "table", implementor.table.getExpression( ElasticsearchTable.ElasticsearchQueryable.class ) );
        final Expression ops = block.append( "ops", Expressions.constant( implementor.list ) );
        final Expression sort = block.append( "sort", constantArrayList( implementor.sort, Pair.class ) );
        final Expression groupBy = block.append( "groupBy", Expressions.constant( implementor.groupBy ) );
        final Expression aggregations = block.append( "aggregations", constantArrayList( implementor.aggregations, Pair.class ) );

        final Expression mappings = block.append( "mappings", Expressions.constant( implementor.expressionItemMap ) );

        final Expression offset = block.append( "offset", Expressions.constant( implementor.offset ) );
        final Expression fetch = block.append( "fetch", Expressions.constant( implementor.fetch ) );

        Expression enumerable = block.append( "enumerable", Expressions.call( table, ElasticsearchMethod.ELASTICSEARCH_QUERYABLE_FIND.method, ops, fields, sort, groupBy, aggregations, mappings, offset, fetch ) );
        block.add( Expressions.return_( null, enumerable ) );
        return algImplementor.result( physType, block.toBlock() );
    }


    /**
     * E.g. {@code constantArrayList("x", "y")} returns "Arrays.asList('x', 'y')".
     *
     * @param values list of values
     * @param clazz runtime class representing each element in the list
     * @param <T> type of elements in the list
     * @return method call which creates a list
     */
    private static <T> MethodCallExpression constantArrayList( List<T> values, Class clazz ) {
        return Expressions.call( BuiltInMethod.ARRAYS_AS_LIST.method, Expressions.newArrayInit( clazz, constantList( values ) ) );
    }


    /**
     * E.g. {@code constantList("x", "y")} returns {@code {ConstantExpression("x"), ConstantExpression("y")}}.
     *
     * @param values list of elements
     * @param <T> type of elements inside this list
     * @return list of constant expressions
     */
    private static <T> List<Expression> constantList( List<T> values ) {
        return values.stream().map( Expressions::constant ).collect( Collectors.toList() );
    }

}

