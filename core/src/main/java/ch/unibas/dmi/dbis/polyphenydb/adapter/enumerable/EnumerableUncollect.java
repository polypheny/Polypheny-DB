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

package ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Uncollect;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.runtime.SqlFunctions.FlatProductInputType;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.MapSqlType;
import ch.unibas.dmi.dbis.polyphenydb.util.BuiltInMethod;
import com.google.common.primitives.Ints;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;


/**
 * Implementation of {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Uncollect} in {@link EnumerableConvention enumerable calling convention}.
 */
public class EnumerableUncollect extends Uncollect implements EnumerableRel {

    @Deprecated // to be removed before 2.0
    public EnumerableUncollect( RelOptCluster cluster, RelTraitSet traitSet, RelNode child ) {
        this( cluster, traitSet, child, false );
    }


    /**
     * Creates an EnumerableUncollect.
     *
     * Use {@link #create} unless you know what you're doing.
     */
    public EnumerableUncollect( RelOptCluster cluster, RelTraitSet traitSet, RelNode child, boolean withOrdinality ) {
        super( cluster, traitSet, child, withOrdinality );
        assert getConvention() instanceof EnumerableConvention;
        assert getConvention() == child.getConvention();
    }


    /**
     * Creates an EnumerableUncollect.
     *
     * Each field of the input relational expression must be an array or multiset.
     *
     * @param traitSet Trait set
     * @param input Input relational expression
     * @param withOrdinality Whether output should contain an ORDINALITY column
     */
    public static EnumerableUncollect create( RelTraitSet traitSet, RelNode input, boolean withOrdinality ) {
        final RelOptCluster cluster = input.getCluster();
        return new EnumerableUncollect( cluster, traitSet, input, withOrdinality );
    }


    @Override
    public EnumerableUncollect copy( RelTraitSet traitSet, RelNode newInput ) {
        return new EnumerableUncollect( getCluster(), traitSet, newInput, withOrdinality );
    }


    public Result implement( EnumerableRelImplementor implementor, Prefer pref ) {
        final BlockBuilder builder = new BlockBuilder();
        final EnumerableRel child = (EnumerableRel) getInput();
        final Result result = implementor.visitChild( this, 0, child, pref );
        final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), getRowType(), JavaRowFormat.LIST );

        // final Enumerable<List<Employee>> child = <<child adapter>>;
        // return child.selectMany(FLAT_PRODUCT);
        final Expression child_ = builder.append( "child", result.block );

        final List<Integer> fieldCounts = new ArrayList<>();
        final List<FlatProductInputType> inputTypes = new ArrayList<>();

        for ( RelDataTypeField field : child.getRowType().getFieldList() ) {
            final RelDataType type = field.getType();
            if ( type instanceof MapSqlType ) {
                fieldCounts.add( 2 );
                inputTypes.add( FlatProductInputType.MAP );
            } else {
                final RelDataType elementType = type.getComponentType();
                if ( elementType.isStruct() ) {
                    fieldCounts.add( elementType.getFieldCount() );
                    inputTypes.add( FlatProductInputType.LIST );
                } else {
                    fieldCounts.add( -1 );
                    inputTypes.add( FlatProductInputType.SCALAR );
                }
            }
        }

        final Expression lambda =
                Expressions.call( BuiltInMethod.FLAT_PRODUCT.method,
                        Expressions.constant( Ints.toArray( fieldCounts ) ),
                        Expressions.constant( withOrdinality ),
                        Expressions.constant( inputTypes.toArray( new FlatProductInputType[0] ) ) );
        builder.add( Expressions.return_( null, Expressions.call( child_, BuiltInMethod.SELECT_MANY.method, lambda ) ) );
        return implementor.result( physType, builder.toBlock() );
    }

}

