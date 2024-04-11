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

package org.polypheny.db.algebra.enumerable;


import com.google.common.primitives.Ints;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Uncollect;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.functions.Functions.FlatProductInputType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.type.MapPolyType;
import org.polypheny.db.util.BuiltInMethod;


/**
 * Implementation of {@link org.polypheny.db.algebra.core.Uncollect} in {@link EnumerableConvention enumerable calling convention}.
 */
public class EnumerableUncollect extends Uncollect implements EnumerableAlg {


    /**
     * Creates an EnumerableUncollect.
     * <p>
     * Use {@link #create} unless you know what you're doing.
     */
    public EnumerableUncollect( AlgCluster cluster, AlgTraitSet traitSet, AlgNode child, boolean withOrdinality ) {
        super( cluster, traitSet, child, withOrdinality );
        assert getConvention() instanceof EnumerableConvention;
        assert getConvention() == child.getConvention();
    }


    /**
     * Creates an EnumerableUncollect.
     * <p>
     * Each field of the input relational expression must be an array or multiset.
     *
     * @param traitSet Trait set
     * @param input Input relational expression
     * @param withOrdinality Whether output should contain an ORDINALITY column
     */
    public static EnumerableUncollect create( AlgTraitSet traitSet, AlgNode input, boolean withOrdinality ) {
        final AlgCluster cluster = input.getCluster();
        return new EnumerableUncollect( cluster, traitSet, input, withOrdinality );
    }


    @Override
    public EnumerableUncollect copy( AlgTraitSet traitSet, AlgNode newInput ) {
        return new EnumerableUncollect( getCluster(), traitSet, newInput, withOrdinality );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder builder = new BlockBuilder();
        final EnumerableAlg child = (EnumerableAlg) getInput();
        final Result result = implementor.visitChild( this, 0, child, pref );
        final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), getTupleType(), JavaTupleFormat.LIST );

        // final Enumerable<List<Employee>> child = <<child adapter>>;
        // return child.selectMany(FLAT_PRODUCT);
        final Expression child_ = builder.append( "child", result.block() );

        final List<Integer> fieldCounts = new ArrayList<>();
        final List<FlatProductInputType> inputTypes = new ArrayList<>();

        for ( AlgDataTypeField field : child.getTupleType().getFields() ) {
            final AlgDataType type = field.getType();
            if ( type instanceof MapPolyType ) {
                fieldCounts.add( 2 );
                inputTypes.add( FlatProductInputType.MAP );
            } else {
                final AlgDataType elementType = type.getComponentType();
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
                Expressions.call(
                        BuiltInMethod.FLAT_PRODUCT.method,
                        Expressions.constant( Ints.toArray( fieldCounts ) ),
                        Expressions.constant( withOrdinality ),
                        Expressions.constant( inputTypes.toArray( new FlatProductInputType[0] ) ) );
        builder.add( Expressions.return_( null, Expressions.call( child_, BuiltInMethod.SELECT_MANY.method, lambda ) ) );
        return implementor.result( physType, builder.toBlock() );
    }

}

