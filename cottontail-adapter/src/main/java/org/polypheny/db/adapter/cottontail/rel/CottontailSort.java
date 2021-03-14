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
 */

package org.polypheny.db.adapter.cottontail.rel;


import java.lang.reflect.Modifier;
import java.util.Map;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Sort;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.BuiltInMethod;


public class CottontailSort extends Sort implements CottontailRel {

    public CottontailSort( RelOptCluster cluster, RelTraitSet traits, RelNode child, RelCollation collation, RexNode offset, RexNode fetch ) {
        super( cluster, traits, child, collation, offset, fetch );
    }


    public CottontailSort( RelOptCluster cluster, RelTraitSet traits, RelNode child, RelCollation collation ) {
        super( cluster, traits, child, collation );
    }


    private static Expression numberBuilderBuilder( RexNode node ) {
        BlockBuilder inner = new BlockBuilder();
        ParameterExpression dynamicParameterMap_ = Expressions.parameter( Modifier.FINAL, Map.class, inner.newName( "dynamicParameters" ) );

        Expression expr;
        if ( node instanceof RexLiteral ) {
            expr = Expressions.constant( ((RexLiteral) node).getValueAs( Integer.class ) );
        } else if ( node instanceof RexDynamicParam ) {
            expr = Expressions.call( dynamicParameterMap_, BuiltInMethod.MAP_GET.method, Expressions.constant( ((RexDynamicParam) node).getIndex() ) );
        } else {
            throw new RuntimeException( "Node statement is neither a Literal nor a Dynamic Parameter." );
        }

        inner.add( Expressions.return_( null, expr ) );

        return Expressions.lambda( inner.toBlock(), dynamicParameterMap_ );
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        final double rowCount = mq.getRowCount( this );
        return planner.getCostFactory().makeCost( rowCount, 0, 0 );
    }


    @Override
    public void implement( CottontailImplementContext context ) {
        context.visitChild( 0, getInput() );
        if ( this.offset != null || this.fetch != null ) {

            if ( this.fetch != null ) {
                context.limitBuilder = numberBuilderBuilder( this.fetch );
            }

            if ( this.offset != null ) {
                context.offsetBuilder = numberBuilderBuilder( this.offset );
            }
        }
    }


    @Override
    public Sort copy( RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch ) {
        return new CottontailSort( getCluster(), traitSet, newInput, newCollation, offset, fetch );
    }

}
