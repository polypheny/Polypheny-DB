/*
 * Copyright 2019-2020 The Polypheny Project
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

package ch.unibas.dmi.dbis.polyphenydb.adapter.mongodb;


import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableRel;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableRelImplementor;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.JavaRowFormat;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.PhysType;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.PhysTypeImpl;
import ch.unibas.dmi.dbis.polyphenydb.config.RuntimeConfig;
import ch.unibas.dmi.dbis.polyphenydb.plan.ConventionTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCost;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.convert.ConverterImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Hook;
import ch.unibas.dmi.dbis.polyphenydb.util.BuiltInMethod;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import com.google.common.collect.Lists;
import java.util.AbstractList;
import java.util.List;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;


/**
 * Relational expression representing a scan of a table in a Mongo data source.
 */
public class MongoToEnumerableConverter extends ConverterImpl implements EnumerableRel {

    protected MongoToEnumerableConverter( RelOptCluster cluster, RelTraitSet traits, RelNode input ) {
        super( cluster, ConventionTraitDef.INSTANCE, traits, input );
    }


    @Override
    public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        return new MongoToEnumerableConverter( getCluster(), traitSet, sole( inputs ) );
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( .1 );
    }


    @Override
    public Result implement( EnumerableRelImplementor implementor, Prefer pref ) {
        // Generates a call to "find" or "aggregate", depending upon whether
        // an aggregate is present.
        //
        //   ((MongoTable) schema.getTable("zips")).find(
        //     "{state: 'CA'}",
        //     "{city: 1, zipcode: 1}")
        //
        //   ((MongoTable) schema.getTable("zips")).aggregate(
        //     "{$filter: {state: 'CA'}}",
        //     "{$group: {_id: '$city', c: {$sum: 1}, p: {$sum: "$pop"}}")
        final BlockBuilder list = new BlockBuilder();
        final MongoRel.Implementor mongoImplementor = new MongoRel.Implementor();
        mongoImplementor.visitChild( 0, getInput() );
        int aggCount = 0;
        int findCount = 0;
        String project = null;
        String filter = null;
        for ( Pair<String, String> op : mongoImplementor.list ) {
            if ( op.left == null ) {
                ++aggCount;
            }
            if ( op.right.startsWith( "{$match:" ) ) {
                filter = op.left;
                ++findCount;
            }
            if ( op.right.startsWith( "{$project:" ) ) {
                project = op.left;
                ++findCount;
            }
        }
        final RelDataType rowType = getRowType();
        final PhysType physType =
                PhysTypeImpl.of( implementor.getTypeFactory(), rowType, pref.prefer( JavaRowFormat.ARRAY ) );
        final Expression fields =
                list.append( "fields",
                        constantArrayList(
                                Pair.zip( MongoRules.mongoFieldNames( rowType ),
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
        final Expression table = list.append( "table", mongoImplementor.table.getExpression( MongoTable.MongoQueryable.class ) );
        List<String> opList = Pair.right( mongoImplementor.list );
        final Expression ops = list.append( "ops", constantArrayList( opList, String.class ) );
        Expression enumerable = list.append( "enumerable", Expressions.call( table, MongoMethod.MONGO_QUERYABLE_AGGREGATE.method, fields, ops ) );
        if ( RuntimeConfig.DEBUG.getBoolean() ) {
            System.out.println( "Mongo: " + opList );
        }
        Hook.QUERY_PLAN.run( opList );
        list.add( Expressions.return_( null, enumerable ) );
        return implementor.result( physType, list.toBlock() );
    }


    /**
     * E.g. {@code constantArrayList("x", "y")} returns "Arrays.asList('x', 'y')".
     *
     * @param values List of values
     * @param clazz Type of values
     * @return expression
     */
    private static <T> MethodCallExpression constantArrayList( List<T> values, Class clazz ) {
        return Expressions.call( BuiltInMethod.ARRAYS_AS_LIST.method, Expressions.newArrayInit( clazz, constantList( values ) ) );
    }


    /**
     * E.g. {@code constantList("x", "y")} returns {@code {ConstantExpression("x"), ConstantExpression("y")}}.
     */
    private static <T> List<Expression> constantList( List<T> values ) {
        return Lists.transform( values, Expressions::constant );
    }
}

