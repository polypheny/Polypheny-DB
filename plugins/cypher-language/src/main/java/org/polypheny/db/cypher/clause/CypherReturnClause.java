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
import java.util.stream.IntStream;
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
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.cypher.expression.CypherAggregate;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.cypher.expression.CypherProperty;
import org.polypheny.db.cypher.expression.CypherVariable;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
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


    private AlgNode getAggregate( CypherContext context, AlgNode node ) {
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
        AlgDataTypeFactory.Builder groupedType = context.typeFactory.builder();
        List<String> aggregateNames = new ArrayList<>();
        List<Integer> requestedOrder = new ArrayList<>();

        int aggCount = 0;
        for ( CypherReturn returnItem : returnItems ) {
            if ( returnItem instanceof CypherReturnItem item ) {
                if ( item.getExpression() instanceof CypherAggregate aggregate ) {
                    requestedOrder.add( -1 - aggCount );
                    AggregateCall call = aggCalls.get( aggCount );
                    calls.add( LaxAggregateCall.from( call, aggIndexes.get( aggCount ).left == null ? null : node ) );
                    aggregateNames.add( item.getVariable() != null ? item.getVariable().getName() : aggregate.op.name() );
                    aggCount++;
                } else if ( item.getVariable() != null || item.getExpression() instanceof CypherProperty || item.getExpression() instanceof CypherVariable ) {
                    String name = item.getVariable() != null ? item.getVariable().getName() : item.getExpression().getName();
                    int index = node.getTupleType().getFieldNames().indexOf( name );
                    AlgDataType type = node.getTupleType().getFields().get( index ).getType();
                    requestedOrder.add( groupIndexes.size() );
                    groupIndexes.add( RexNameRef.create( name, index, type ) );
                    groupedType.add( name, null, type );
                }
            }
        }

        for ( int i = 0; i < calls.size(); i++ ) {
            groupedType.add( aggregateNames.get( i ), null, calls.get( i ).getType( node.getCluster() ) );
        }
        AlgNode aggregate = new LogicalLpgAggregate(
                node.getCluster(),
                node.getTraitSet(),
                node,
                groupIndexes,
                calls,
                groupedType.build() );
        List<Integer> positions = requestedOrder.stream().map( index -> index < 0 ? groupIndexes.size() - 1 - index : index ).toList();
        if ( IntStream.range( 0, positions.size() ).allMatch( index -> positions.get( index ) == index ) ) {
            return aggregate;
        }
        return LogicalLpgProject.create(
                aggregate,
                positions.stream().map( index -> context.rexBuilder.makeInputRef( aggregate, index ) ).toList(),
                positions.stream().map( index -> PolyString.of( aggregate.getTupleType().getFieldNames().get( index ) ) ).toList() );
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

        Integer skip = this.skip == null ? null : getRowBound( this.skip, "SKIP" );
        Integer limit = this.limit == null ? null : getRowBound( this.limit, "LIMIT" );

        AlgTraitSet traitSet = node.getTraitSet().replace( collation );

        return new LogicalLpgSort( node.getCluster(), traitSet, collation, context.pop(), skip, limit );
    }


    private static int getRowBound( CypherExpression expression, String clause ) {
        PolyValue value = expression.getComparable();
        if ( value == null || value.isNull() || !PolyType.INT_TYPES.contains( value.getType() ) ) {
            throw new GenericRuntimeException( "%s requires a non-negative integer", clause );
        }
        long bound = value.asNumber().longValue();
        if ( bound < 0 || bound > Integer.MAX_VALUE ) {
            throw new GenericRuntimeException( "%s requires an integer between 0 and %d", clause, Integer.MAX_VALUE );
        }
        return (int) bound;
    }


    public List<String> getSortFields() {
        return order.stream().map( o -> o.getExpression().getName() ).toList();
    }

}
