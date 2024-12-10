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

package org.polypheny.db.algebra.enumerable;

import lombok.Getter;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgDistributionTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.metadata.AlgMdCollation;
import org.polypheny.db.algebra.metadata.AlgMdDistribution;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;

@Getter
public class EnumerableIdentifierInjection extends SingleAlg implements EnumerableAlg{

    public final Entity entity;

    public EnumerableIdentifierInjection( Entity entity, AlgCluster cluster, AlgTraitSet traitSet, AlgNode input) {
        super(cluster, traitSet, input);
        this.entity = entity;
        this.rowType = input.getTupleType();
        assert getConvention() instanceof EnumerableConvention;
        assert getConvention() == input.getConvention();
    }

    public static EnumerableIdentifierInjection create(Entity table, AlgNode input) {
        final AlgCluster cluster = input.getCluster();
        final AlgMetadataQuery mq = cluster.getMetadataQuery();
        final AlgTraitSet traitSet =
                cluster.traitSetOf( EnumerableConvention.INSTANCE )
                        .replaceIfs( AlgCollationTraitDef.INSTANCE, () -> AlgMdCollation.limit( mq, input ) )
                        .replaceIf( AlgDistributionTraitDef.INSTANCE, () -> AlgMdDistribution.limit( mq, input ) );
        return new EnumerableIdentifierInjection( table, cluster, traitSet, input );
    }

    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        double rowCount = mq.getTupleCount( this );
        return planner.getCostFactory().makeCost( rowCount, 0, 0 );
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" +
                input.algCompareString() + "$" +
                entity.hashCode()+ "&";
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder builder = new BlockBuilder();
        final EnumerableAlg input = (EnumerableAlg) getInput();

        final Result result = implementor.visitChild( this, 0, input, pref );
        final PhysType physicalType = PhysTypeImpl.of( implementor.getTypeFactory(), getTupleType(), result.format() );

        Expression expression = builder.append( "input", result.block() );

        // ToDo: actually do something here...


        builder.add( Expressions.return_( null, expression ) );
        return implementor.result( physicalType, builder.toBlock() );
    }
}
