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

package org.polypheny.db.adapter.cottontail.algebra;

import java.lang.reflect.Modifier;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.NewExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.BuiltInMethod;
import org.vitrivr.cottontail.grpc.CottontailGrpc;


/**
 * A {@link CottontailAlg} that implements ORDER BY clauses and pushes them down to Cottontail DB.
 */
public class CottontailSort extends Sort implements CottontailAlg {

    public CottontailSort( AlgOptCluster cluster, AlgTraitSet traits, AlgNode child, AlgCollation collation, RexNode offset, RexNode fetch ) {
        super( cluster, traits, child, collation, offset, fetch );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        final double rowCount = mq.getRowCount( this ) + 0.01;
        return planner.getCostFactory().makeCost( rowCount, 0, 0 );
    }


    @Override
    public void implement( CottontailImplementContext context ) {
        context.visitChild( 0, getInput() );
        if ( this.fetch != null ) {
            context.limitBuilder = numberBuilderBuilder( this.fetch );
        }
        if ( this.offset != null ) {
            context.offsetBuilder = numberBuilderBuilder( this.offset );
        }
        if ( this.collation != null && this.collation.getFieldCollations().size() > 0 ) {
            context.sortMap = sortMapBuilder( this.collation, context, getRowType().getFieldNames() );
        }
    }


    @Override
    public Sort copy( AlgTraitSet traitSet, AlgNode newInput, AlgCollation newCollation, RexNode offset, RexNode fetch ) {
        return new CottontailSort( getCluster(), traitSet, newInput, newCollation, offset, fetch );
    }


    /**
     * Constructs a {@link ParameterExpression} that generates a map containing the field to order by.
     *
     * @param node The {@link AlgCollation} node to implement.
     * @param context The {@link CottontailImplementContext} instance.
     * @param columnNames List of column names.
     * @return {@link ParameterExpression}
     */
    private static ParameterExpression sortMapBuilder( AlgCollation node, CottontailImplementContext context, List<String> columnNames ) {
        final BlockBuilder builder = context.blockBuilder;
        final ParameterExpression orderMap_ = Expressions.variable( Map.class, builder.newName( "orderMap" ) );
        final NewExpression orderMapCreator = Expressions.new_( LinkedHashMap.class );
        builder.add( Expressions.declare( Modifier.FINAL, orderMap_, orderMapCreator ) );
        for ( AlgFieldCollation c : node.getFieldCollations() ) {
            final String logicalName = columnNames.get( c.getFieldIndex() );
            String physicalName;
            try {
                physicalName = context.cottontailTable.getPhysicalColumnName( logicalName );
            } catch ( IndexOutOfBoundsException e ) {
                physicalName = logicalName; /* Case of column being calculated and there being no physical column. */
            }
            final Expression sortOrder;
            switch ( c.direction ) {
                case DESCENDING:
                case STRICTLY_DESCENDING:
                    sortOrder = Expressions.constant( CottontailGrpc.Order.Direction.DESCENDING.toString() );
                    break;
                default:
                    sortOrder = Expressions.constant( CottontailGrpc.Order.Direction.ASCENDING.toString() );
                    break;
            }
            builder.add( Expressions.statement( Expressions.call( orderMap_, BuiltInMethod.MAP_PUT.method, Expressions.constant( physicalName ), sortOrder ) ) );
        }
        return orderMap_;
    }


    /**
     * Constructs a {@link  Expression} that represents a numeric value (LIMIT, SKIP).
     *
     * @param node The {@link RexNode} node to implement.
     * @return {@link Expression}
     */
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

}
