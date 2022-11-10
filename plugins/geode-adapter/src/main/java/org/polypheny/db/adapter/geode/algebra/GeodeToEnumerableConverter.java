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

package org.polypheny.db.adapter.geode.algebra;


import static org.polypheny.db.adapter.geode.algebra.GeodeRules.geodeFieldNames;

import com.google.common.collect.Lists;
import java.lang.reflect.Method;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.enumerable.EnumerableAlg;
import org.polypheny.db.adapter.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.adapter.enumerable.JavaRowFormat;
import org.polypheny.db.adapter.enumerable.PhysType;
import org.polypheny.db.adapter.enumerable.PhysTypeImpl;
import org.polypheny.db.adapter.geode.algebra.GeodeAlg.GeodeImplementContext;
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
 * Relational expression representing a scan of a table in a Geode data source.
 */
public class GeodeToEnumerableConverter extends ConverterImpl implements EnumerableAlg {

    protected GeodeToEnumerableConverter( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode input ) {
        super( cluster, ConventionTraitDef.INSTANCE, traitSet, input );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new GeodeToEnumerableConverter( getCluster(), traitSet, sole( inputs ) );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( .1 );
    }


    /**
     * Reference to the method {@link GeodeTable.GeodeQueryable#query}, used in the {@link Expression}.
     */
    private static final Method GEODE_QUERY_METHOD = Types.lookupMethod( GeodeTable.GeodeQueryable.class, "query", List.class, List.class, List.class, List.class, List.class, List.class, Long.class );


    /**
     * {@inheritDoc}
     *
     * @param implementor GeodeImplementContext
     */
    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        // travers all relations form this to the scan leaf
        final GeodeImplementContext geodeImplementContext = new GeodeImplementContext();
        ((GeodeAlg) getInput()).implement( geodeImplementContext );

        final AlgDataType rowType = getRowType();

        // PhysType is Enumerable Adapter class that maps SQL types (getRowType) with physical Java types (getJavaTypes())
        final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), rowType, pref.prefer( JavaRowFormat.ARRAY ) );

        final List<Class> physFieldClasses = new AbstractList<Class>() {
            @Override
            public Class get( int index ) {
                return physType.fieldClass( index );
            }


            @Override
            public int size() {
                return rowType.getFieldCount();
            }
        };

        // Expression meta-program for calling the GeodeTable.GeodeQueryable#query method form the generated code
        final BlockBuilder blockBuilder = new BlockBuilder().append(
                Expressions.call(
                        geodeImplementContext.table.getExpression( GeodeTable.GeodeQueryable.class ),
                        GEODE_QUERY_METHOD,
                        // fields
                        constantArrayList( Pair.zip( geodeFieldNames( rowType ), physFieldClasses ), Pair.class ),
                        // selected fields
                        constantArrayList( toListMapPairs( geodeImplementContext.selectFields ), Pair.class ),
                        // aggregate functions
                        constantArrayList( toListMapPairs( geodeImplementContext.oqlAggregateFunctions ), Pair.class ),
                        constantArrayList( geodeImplementContext.groupByFields, String.class ),
                        constantArrayList( geodeImplementContext.whereClause, String.class ),
                        constantArrayList( geodeImplementContext.orderByFields, String.class ),
                        Expressions.constant( geodeImplementContext.limitValue ) ) );

        return implementor.result( physType, blockBuilder.toBlock() );
    }


    private static List<Map.Entry<String, String>> toListMapPairs( Map<String, String> map ) {
        List<Map.Entry<String, String>> selectList = new ArrayList<>();
        for ( Map.Entry<String, String> entry : Pair.zip( map.keySet(), map.values() ) ) {
            selectList.add( entry );
        }
        return selectList;
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
        return Lists.transform( values, Expressions::constant );
    }

}

