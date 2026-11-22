/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.algebra.enumerable.lpg;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.common.Streamer;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.JavaTupleFormat;
import org.polypheny.db.algebra.enumerable.PhysType;
import org.polypheny.db.algebra.enumerable.PhysTypeImpl;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgMergeRows;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgUnion;
import org.polypheny.db.algebra.polyalg.arguments.BooleanArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.functions.CypherFunctions;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.BuiltInMethod;


public class EnumerableLpgUnion extends LogicalLpgUnion implements EnumerableAlg {

    public EnumerableLpgUnion( AlgCluster cluster, AlgTraitSet traits, List<AlgNode> inputs, boolean all ) {
        super( cluster, traits, inputs, all );
    }


    public static EnumerableLpgUnion create( PolyAlgArgs args, List<AlgNode> children, AlgCluster cluster ) {
        AlgTraitSet traits = cluster.traitSet().replace( ModelTrait.GRAPH ).replace( EnumerableConvention.INSTANCE );
        return new EnumerableLpgUnion( cluster, traits, children, args.getArg( "all", BooleanArg.class ).toBool() );
    }


    @Override
    public EnumerableLpgUnion copy( AlgTraitSet traitSet, List<AlgNode> inputs, boolean all ) {
        return new EnumerableLpgUnion( getCluster(), traitSet, inputs, all );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), getTupleType(), JavaTupleFormat.ARRAY );
        BlockBuilder builder = new BlockBuilder();
        Expression combined = null;
        boolean writes = inputs.stream().anyMatch( EnumerableLpgUnion::containsWrite );
        for ( Ord<AlgNode> input : Ord.zip( inputs ) ) {
            Result child = implementor.visitChild( this, input.i, (EnumerableAlg) input.e, Prefer.ARRAY );
            Expression rows = builder.append( "branch" + input.i, child.block(), !writes );
            if ( child.format() != JavaTupleFormat.ARRAY ) {
                ParameterExpression row = Expressions.parameter( child.physType().getJavaTupleType(), "row" );
                List<Expression> fields = new ArrayList<>( getTupleType().getFieldCount() );
                for ( int i = 0; i < getTupleType().getFieldCount(); i++ ) {
                    fields.add( child.physType().fieldReference( row, i, PolyValue.class ) );
                }
                rows = Expressions.call( rows, BuiltInMethod.SELECT.method, Expressions.lambda( Function1.class, physType.record( fields ), row ) );
            }
            if ( writes ) {
                // A subsequent branch must see every write from its predecessors.
                Expression list = Expressions.call( rows, BuiltInMethod.ENUMERABLE_TO_LIST.method );
                rows = builder.append( "materialized" + input.i, Expressions.call( BuiltInMethod.AS_ENUMERABLE2.method, list ), false );
            }
            combined = combined == null ? rows : Expressions.call( combined, BuiltInMethod.CONCAT.method, rows );
        }
        if ( !all ) {
            combined = Expressions.call( CypherFunctions.class, "distinctRows", combined );
        }
        builder.add( combined );
        return implementor.result( physType, builder.toBlock() );
    }


    private static boolean containsWrite( AlgNode node ) {
        return node instanceof Modify<?> || node instanceof Streamer || node instanceof LogicalLpgMergeRows
                || node.getInputs().stream().anyMatch( EnumerableLpgUnion::containsWrite );
    }

}
