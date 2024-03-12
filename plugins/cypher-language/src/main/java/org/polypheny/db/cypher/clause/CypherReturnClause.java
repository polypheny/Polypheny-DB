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
import org.polypheny.db.algebra.core.LaxAggregateCall;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgAggregate;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgProject;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgSort;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.cypher.expression.CypherAggregate;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.cypher.expression.CypherProperty;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.entity.PolyString;
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
        List<Pair<PolyString, RexNode>> nameAndProject = returnItems.stream()
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
                int i = node.getTupleType().getFieldNames().indexOf( namedAgg.left );
                aggCalls.add( namedAgg.right.adjustedCopy( List.of( i ) ) );
            }
        }

        List<RexNameRef> groupIndexes = new ArrayList<>();
        List<LaxAggregateCall> calls = new ArrayList<>();
        AlgDataTypeFactory.Builder builder = context.typeFactory.builder();

        int aggCount = 0;
        for ( CypherReturn returnItem : returnItems ) {
            if ( returnItem instanceof CypherReturnItem item ) {
                if ( item.getExpression() instanceof CypherProperty name ) {
                    AlgDataType type = node.getTupleType().getField( name.getName(), false, false ).getType();
                    builder.add( name.getName(), null, type );

                    int i = node.getTupleType().getFieldNames().indexOf( name.getName() );
                    RexNameRef ref = RexNameRef.create( name.getName(), i, node.getTupleType().getFields().get( i ).getType() );
                    groupIndexes.add( ref );
                } else if ( item.getExpression() instanceof CypherAggregate aggregate ) {

                    Pair<String, AggregateCall> aggIndex = aggIndexes.get( aggCount );
                    LaxAggregateCall call = LaxAggregateCall.from( aggIndex.right, aggIndex.left == null ? null : node );
                    calls.add( call );

                    builder.add( item.getVariable() != null ? item.getVariable().getName() : aggregate.op.name(), null, call.getType( node.getCluster() ) );
                }
            }
        }

        return new LogicalLpgAggregate(
                node.getCluster(),
                node.getTraitSet(),
                node,
                groupIndexes,// instead of grouping by only one filed add multiple combinations (), (1,2)
                calls,
                builder.build() );
    }


    private AlgNode getProject( CypherContext context, List<Pair<PolyString, RexNode>> nameAndProject ) {
        AlgNode node = context.pop();

        if ( node == null ) {
            node = context.asValues( nameAndProject );
        }
        if ( nameAndProject.isEmpty() ) {
            // use all e.g. count(*)
            return node;
        }

        AlgNode project = new LogicalLpgProject(
                context.cluster,
                context.cluster.traitSet(),
                node,
                Pair.right( nameAndProject ),
                Pair.left( nameAndProject )
        );

        if ( node.getTupleType().equals( project.getTupleType() ) ) {
            return node;
        }
        return project;
    }


    public AlgNode getGraphSort( CypherContext context ) {
        List<Pair<Direction, String>> orders = order.stream()
                .map( o -> Pair.of( o.isAsc() ? Direction.ASCENDING : Direction.DESCENDING, o.getExpression().getName() ) )
                .toList();

        AlgNode node = context.peek();

        List<AlgFieldCollation> collations = new ArrayList<>();

        for ( Pair<Direction, String> item : orders ) {
            int index = node.getTupleType().getFieldNames().indexOf( item.right );
            collations.add( new AlgFieldCollation( index, item.left ) );
        }

        AlgCollation collation = AlgCollations.of( collations );

        Integer skip = null;
        if ( this.skip != null ) {
            skip = this.skip.getComparable().asInteger().value;
        }

        Integer limit = null;
        if ( this.limit != null ) {
            limit = this.limit.getComparable().asInteger().value;
        }

        AlgTraitSet traitSet = node.getTraitSet().replace( collation );

        return new LogicalLpgSort( node.getCluster(), traitSet, collation, context.pop(), skip, limit );
    }


    public List<String> getSortFields() {
        return order.stream().map( o -> o.getExpression().getName() ).toList();
    }

}
