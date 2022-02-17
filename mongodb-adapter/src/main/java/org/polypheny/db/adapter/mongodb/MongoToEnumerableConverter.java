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

package org.polypheny.db.adapter.mongodb;


import com.google.common.collect.Lists;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.polypheny.db.adapter.enumerable.EnumUtils;
import org.polypheny.db.adapter.enumerable.EnumerableAlg;
import org.polypheny.db.adapter.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.adapter.enumerable.JavaRowFormat;
import org.polypheny.db.adapter.enumerable.PhysType;
import org.polypheny.db.adapter.enumerable.PhysTypeImpl;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterImpl;
import org.polypheny.db.algebra.core.TableModify.Operation;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.runtime.Hook;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Pair;


/**
 * Relational expression representing a scan of a table in a Mongo data source.
 */
@Slf4j
public class MongoToEnumerableConverter extends ConverterImpl implements EnumerableAlg {

    protected MongoToEnumerableConverter( AlgOptCluster cluster, AlgTraitSet traits, AlgNode input ) {
        super( cluster, ConventionTraitDef.INSTANCE, traits, input );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new MongoToEnumerableConverter( getCluster(), traitSet, AbstractAlgNode.sole( inputs ) );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( .1 );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder list = new BlockBuilder();
        final MongoAlg.Implementor mongoImplementor = new MongoAlg.Implementor();
        mongoImplementor.visitChild( 0, getInput() );

        final AlgDataType rowType = getRowType();
        final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), rowType, pref.prefer( JavaRowFormat.ARRAY ) );

        if ( mongoImplementor.table == null ) {
            return implementor.result( physType, new BlockBuilder().toBlock() );
        }

        final Expression fields =
                list.append(
                        list.newName( "fields" ),
                        constantArrayList(
                                Pair.zip(
                                        MongoRules.mongoFieldNames( rowType ),
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

        List<AlgDataTypeField> fieldList = rowType.getFieldList();

        final Expression arrayClassFields =
                list.append(
                        "arrayClassFields",
                        constantArrayList(
                                Pair.zip(
                                        MongoRules.mongoFieldNames( rowType ),
                                        new AbstractList<Class>() {

                                            @Override
                                            public Class get( int index ) {
                                                Class clazz = physType.fieldClass( index );
                                                if ( clazz != List.class ) {
                                                    return physType.fieldClass( index );
                                                } else {
                                                    return EnumUtils.javaRowClass( implementor.getTypeFactory(), fieldList.get( index ).getType().getComponentType() );
                                                }

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
        final Expression filter = list.append( "filter", Expressions.constant( mongoImplementor.getFilterSerialized() ) );

        Expression enumerable;
        if ( !mongoImplementor.isDML() ) {
            final Expression logicalCols = list.append( "logical", constantArrayList( Arrays.asList( mongoImplementor.physicalMapper.toArray() ), String.class ) );
            final Expression preProjects = list.append( "prePro", constantArrayList( mongoImplementor.getPreProjects(), String.class ) );
            enumerable = list.append(
                    list.newName( "enumerable" ),
                    Expressions.call( table, MongoMethod.MONGO_QUERYABLE_AGGREGATE.method, fields, arrayClassFields, ops, filter, preProjects, logicalCols ) );
        } else {
            final Expression operations = list.append( list.newName( "operations" ), constantArrayList( mongoImplementor.getOperations(), String.class ) );
            final Expression operation = list.append( list.newName( "operation" ), Expressions.constant( mongoImplementor.getOperation(), Operation.class ) );
            final Expression onlyOne = list.append( list.newName( "onlyOne" ), Expressions.constant( mongoImplementor.onlyOne, boolean.class ) );
            final Expression needsDocument = list.append( list.newName( "needsUpdate" ), Expressions.constant( mongoImplementor.isDocumentUpdate, boolean.class ) );
            enumerable = list.append(
                    list.newName( "enumerable" ),
                    Expressions.call( table, MongoMethod.HANDLE_DIRECT_DML.method, operation, filter, operations, onlyOne, needsDocument ) );
        }

        if ( RuntimeConfig.DEBUG.getBoolean() ) {
            log.info( "Mongo: {}", opList );
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
    protected static <T> MethodCallExpression constantArrayList( List<T> values, Class clazz ) {
        return Expressions.call( BuiltInMethod.ARRAYS_AS_LIST.method, Expressions.newArrayInit( clazz, constantList( values ) ) );
    }


    /**
     * E.g. {@code constantList("x", "y")} returns {@code {ConstantExpression("x"), ConstantExpression("y")}}.
     */
    protected static <T> List<Expression> constantList( List<T> values ) {
        return Lists.transform( values, Expressions::constant );
    }

}

