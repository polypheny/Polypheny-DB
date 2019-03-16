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

package ch.unibas.dmi.dbis.polyphenydb.adapter.geode.rel;


import ch.unibas.dmi.dbis.polyphenydb.rel.convert.ConverterImpl;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableRel;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableRelImplementor;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.JavaRowFormat;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.PhysType;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.PhysTypeImpl;
import ch.unibas.dmi.dbis.polyphenydb.adapter.geode.rel.GeodeRel.GeodeImplementContext;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.Types;
import ch.unibas.dmi.dbis.polyphenydb.plan.ConventionTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCost;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.convert.ConverterImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.util.BuiltInMethod;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;

import com.google.common.collect.Lists;

import java.lang.reflect.Method;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static ch.unibas.dmi.dbis.polyphenydb.adapter.geode.rel.GeodeRules.geodeFieldNames;


/**
 * Relational expression representing a scan of a table in a Geode data source.
 */
public class GeodeToEnumerableConverter extends ConverterImpl implements EnumerableRel {

    protected GeodeToEnumerableConverter( RelOptCluster cluster, RelTraitSet traitSet, RelNode input ) {
        super( cluster, ConventionTraitDef.INSTANCE, traitSet, input );
    }


    @Override
    public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        return new GeodeToEnumerableConverter( getCluster(), traitSet, sole( inputs ) );
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
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
    public Result implement( EnumerableRelImplementor implementor, Prefer pref ) {
        // travers all relations form this to the scan leaf
        final GeodeImplementContext geodeImplementContext = new GeodeImplementContext();
        ((GeodeRel) getInput()).implement( geodeImplementContext );

        final RelDataType rowType = getRowType();

        // PhysType is Enumerable Adapter class that maps SQL types (getRowType) with physical Java types (getJavaTypes())
        final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), rowType, pref.prefer( JavaRowFormat.ARRAY ) );

        final List<Class> physFieldClasses = new AbstractList<Class>() {
            public Class get( int index ) {
                return physType.fieldClass( index );
            }


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

