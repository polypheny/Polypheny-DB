/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.cypher.clause;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgFieldCollation.Direction;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgAggregate;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgProject;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgSort;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Pair;


@Getter
public class CypherReturnClause extends CypherClause {

    private final boolean distinct;
    private final List<CypherReturn> returnItems;
    private final List<CypherOrderItem> order;
    private final ParserPos pos1;
    private final CypherExpression skip;
    private final ParserPos pos2;
    private final CypherExpression limit;
    private final ParserPos pos3;


    public CypherReturnClause(
            ParserPos pos,
            boolean distinct,
            List<CypherReturn> returnItems,
            List<CypherOrderItem> order,
            ParserPos pos1,
            CypherExpression skip,
            ParserPos pos2,
            CypherExpression limit,
            ParserPos pos3 ) {
        super( pos );
        this.distinct = distinct;
        this.returnItems = returnItems;
        this.order = order;
        this.pos1 = pos1;
        this.skip = skip;
        this.pos2 = pos2;
        this.limit = limit;
        this.pos3 = pos3;
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.RETURN;
    }


    public AlgNode getGraphProject( CypherContext context ) {
        List<Pair<String, RexNode>> nameAndProject = returnItems.stream()
                .map( i -> i.getRex( context, RexType.PROJECT ) )
                .filter( Objects::nonNull )
                .collect( Collectors.toCollection( ArrayList::new ) );
        nameAndProject.addAll( 0, context.popNodes() );

        AlgNode node = getProject( context, nameAndProject );

        if ( context.containsAggs() ) {
            // we use not yet existing fields in the aggregate
            // we have to insert a project between, this is automatically "removed" after the aggregate
            // we do this every time even when not really necessary, worst-case with two projects is removed later

            return getAggregate( context, node );
        }
        return node;
    }


    private LogicalLpgAggregate getAggregate( CypherContext context, AlgNode node ) {
        List<Pair<String, AggregateCall>> aggIndexes = context.popAggNodes();
        List<AggregateCall> aggCalls = new ArrayList<>();
        for ( Pair<String, AggregateCall> namedAgg : aggIndexes ) {
            if ( namedAgg.left == null ) {
                aggCalls.add( namedAgg.right );
            } else {
                int i = node.getRowType().getFieldNames().indexOf( namedAgg.left );
                aggCalls.add( namedAgg.right.adjustedCopy( List.of( i ) ) );
            }
        }

        List<String> aggNames = Pair.left( aggIndexes ).stream()
                .filter( Objects::nonNull )
                .collect( Collectors.toList() );

        List<Integer> groupIndexes = node
                .getRowType()
                .getFieldList()
                .stream()
                .filter( f -> !aggNames.contains( f.getName() ) )
                .map( AlgDataTypeField::getIndex )
                .collect( Collectors.toList() );

        return new LogicalLpgAggregate(
                node.getCluster(),
                node.getTraitSet(),
                node,
                false,
                ImmutableBitSet.of( groupIndexes ),
                null, // instead of grouping by only one filed add multiple combinations (), (1,2)
                aggCalls );
    }


    private AlgNode getProject( CypherContext context, List<Pair<String, RexNode>> nameAndProject ) {
        AlgNode node = context.pop();

        if ( node == null ) {
            node = context.asValues( nameAndProject );
        }

        AlgNode project = new LogicalLpgProject(
                context.cluster,
                context.cluster.traitSet(),
                node,
                Pair.right( nameAndProject ),
                Pair.left( nameAndProject )
        );

        if ( node.getRowType().equals( project.getRowType() ) ) {
            return node;
        }
        return project;
    }


    public AlgNode getGraphSort( CypherContext context ) {
        List<Pair<Direction, String>> orders = order.stream()
                .map( o -> Pair.of( o.isAsc() ? Direction.ASCENDING : Direction.DESCENDING, o.getExpression().getName() ) )
                .collect( Collectors.toList() );

        AlgNode node = context.peek();

        List<AlgFieldCollation> collations = new ArrayList<>();

        for ( Pair<Direction, String> item : orders ) {
            int index = node.getRowType().getFieldNames().indexOf( item.right );
            collations.add( new AlgFieldCollation( index, item.left ) );
        }

        AlgCollation collation = AlgCollations.of( collations );

        Integer skip = null;
        if ( this.skip != null ) {
            skip = (Integer) this.skip.getComparable();
        }

        Integer limit = null;
        if ( this.limit != null ) {
            limit = (Integer) this.limit.getComparable();
        }

        AlgTraitSet traitSet = node.getTraitSet().replace( collation );

        return new LogicalLpgSort( node.getCluster(), traitSet, collation, context.pop(), skip, limit );
    }


    public List<String> getSortFields() {
        return order.stream().map( o -> o.getExpression().getName() ).collect( Collectors.toList() );
    }

}
