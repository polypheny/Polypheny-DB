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
 */

package org.polypheny.db.algebra.enumerable.lpg;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.polypheny.db.algebra.core.lpg.LpgValues;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.PhysType;
import org.polypheny.db.algebra.enumerable.PhysTypeImpl;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.util.BuiltInMethod;

public class EnumerableLpgValues extends LpgValues implements EnumerableAlg {

    public EnumerableLpgValues( AlgCluster cluster, AlgTraitSet traitSet, Collection<PolyNode> nodes, Collection<PolyEdge> edges, ImmutableList<ImmutableList<RexLiteral>> values, AlgDataType rowType ) {
        super( cluster, traitSet.replace( EnumerableConvention.INSTANCE ), nodes, edges, values, rowType );
    }


    public static EnumerableLpgValues create( LpgValues values ) {
        return new EnumerableLpgValues( values.getCluster(), values.getTraitSet(), values.getNodes(), values.getEdges(), values.getValues(), values.getRowType() );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder builder = new BlockBuilder();
        final PhysType physType = PhysTypeImpl.of(
                implementor.getTypeFactory(),
                getTupleType(),
                pref.preferCustom()
        );
        final List<Expression> expressions = Stream.concat(
                nodes.stream().map( n -> Expressions.newArrayInit( PolyNode.class, n.asExpression() ) ),
                edges.stream().map( n -> Expressions.newArrayInit( PolyEdge.class, n.asExpression() ) )
        ).collect( Collectors.toCollection( ArrayList::new ) );
        builder.add(
                Expressions.return_(
                        null,
                        Expressions.call(
                                BuiltInMethod.AS_ENUMERABLE.method,
                                Expressions.newArrayInit( Primitive.box( PolyValue.class ), 2, expressions ) ) ) );
        return implementor.result( physType, builder.toBlock() );
    }

}
