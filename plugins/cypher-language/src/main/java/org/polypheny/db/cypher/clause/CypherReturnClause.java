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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgFieldCollation.Direction;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.LaxAggregateCall;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgAggregate;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgProject;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgSort;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.cypher.cypher2alg.CypherSyntaxException;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext.ProjectionAggregate;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.rex.RexUtil;
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


    public AlgNode getGraphProject( CypherContext context, boolean withClause ) {
        Set<String> explicitNames = new HashSet<>();
        for ( CypherReturn item : returnItems ) {
            if ( item instanceof CypherReturnItem projected ) {
                String name = projected.columnName();
                if ( name != null && !explicitNames.add( name ) ) {
                    throw new CypherSyntaxException( "Multiple result columns named '%s'.".formatted( name ) );
                }
            }
        }

        List<Pair<PolyString, RexNode>> projections = new ArrayList<>();
        List<Integer> groupPositions = new ArrayList<>();
        for ( CypherReturn item : returnItems ) {
            if ( item instanceof CypherStarReturn star ) {
                for ( Pair<PolyString, RexNode> field : star.expand( context, explicitNames, withClause ) ) {
                    groupPositions.add( projections.size() );
                    projections.add( field );
                }
                continue;
            }

            int before = context.aggregateCount();
            Pair<PolyString, RexNode> expression = context.withAggregates( true, () -> item.getRex( context, RexType.PROJECT ) );
            if ( context.aggregateCount() == before ) {
                groupPositions.add( projections.size() );
            }
            projections.add( expression.left == null
                    ? Pair.of( PolyString.of( "EXPR$" + projections.size() ), expression.right )
                    : expression );
        }

        AlgNode result = context.containsAggs()
                ? getAggregate( context, projections, groupPositions )
                : getProject( context, projections );
        if ( !distinct ) {
            return result;
        }
        List<RexNameRef> keys = result.getTupleType().getFields().stream()
                .map( field -> RexNameRef.create( field.getName(), field.getIndex(), field.getType() ) )
                .toList();
        if ( keys.isEmpty() ) {
            return LogicalLpgSort.create( AlgCollations.EMPTY, result, null, 1 );
        }
        return LogicalLpgAggregate.create( result, keys, List.of() );
    }


    private AlgNode getAggregate( CypherContext context, List<Pair<PolyString, RexNode>> projections, List<Integer> groupPositions ) {
        List<ProjectionAggregate> aggregates = context.popAggregates();
        List<Pair<PolyString, RexNode>> groups = groupPositions.stream().map( projections::get ).toList();
        List<Pair<PolyString, RexNode>> inputs = new ArrayList<>( groups );
        Set<PolyString> usedNames = new HashSet<>( Pair.left( projections ) );
        List<Integer> operandIndexes = new ArrayList<>();
        for ( ProjectionAggregate aggregate : aggregates ) {
            if ( aggregate.operand() == null ) {
                operandIndexes.add( -1 );
                continue;
            }
            PolyString name = PolyString.of( "_aggregateInput" + operandIndexes.size() );
            while ( !usedNames.add( name ) ) {
                name = PolyString.of( name.value + "_" );
            }
            operandIndexes.add( inputs.size() );
            inputs.add( Pair.of( name, aggregate.operand() ) );
        }

        AlgNode input = getProject( context, inputs );
        List<RexNameRef> groupIndexes = new ArrayList<>();
        List<LaxAggregateCall> calls = new ArrayList<>();
        AlgDataTypeFactory.Builder groupedType = context.typeFactory.builder();
        for ( int i = 0; i < groups.size(); i++ ) {
            String name = groups.get( i ).left.value;
            AlgDataType type = input.getTupleType().getFields().get( i ).getType();
            groupIndexes.add( RexNameRef.create( name, i, type ) );
            groupedType.add( name, null, type );
        }
        for ( int i = 0; i < aggregates.size(); i++ ) {
            PolyString name = PolyString.of( "_aggregateResult" + i );
            while ( !usedNames.add( name ) ) {
                name = PolyString.of( name.value + "_" );
            }
            AggregateCall aggregate = aggregates.get( i ).call().rename( name.value );
            int operandIndex = operandIndexes.get( i );
            LaxAggregateCall call = operandIndex < 0
                    ? LaxAggregateCall.from( aggregate, null )
                    : LaxAggregateCall.from( aggregate.adjustedCopy( List.of( operandIndex ) ), input );
            calls.add( call );
            groupedType.add( name.value, null, call.getType( input.getCluster() ) );
        }

        AlgNode aggregate = new LogicalLpgAggregate(
                input.getCluster(), input.getTraitSet(), input, groupIndexes, calls, groupedType.build() );
        Map<RexNode, RexNode> groupRefs = new HashMap<>();
        for ( int i = 0; i < groups.size(); i++ ) {
            groupRefs.putIfAbsent( groups.get( i ).right, context.rexBuilder.makeInputRef( aggregate, i ) );
        }
        Map<RexNameRef, RexNode> aggregateRefs = new HashMap<>();
        for ( int i = 0; i < aggregates.size(); i++ ) {
            aggregateRefs.put( aggregates.get( i ).reference(), context.rexBuilder.makeInputRef( aggregate, groups.size() + i ) );
        }

        RexShuttle postAggregation = new RexShuttle() {
            @Override
            public RexNode visitCall( RexCall call ) {
                RexNode group = groupRefs.get( call );
                return group == null ? super.visitCall( call ) : group;
            }


            @Override
            public RexNode visitNameRef( RexNameRef reference ) {
                return aggregateRefs.getOrDefault( reference, reference );
            }


            @Override
            public RexNode visitIndexRef( RexIndexRef reference ) {
                RexNode group = groupRefs.get( reference );
                if ( group == null ) {
                    throw new CypherSyntaxException( "AmbiguousAggregationExpression: A variable used outside an aggregate must be a grouping key." );
                }
                return group;
            }
        };
        List<RexNode> output = new ArrayList<>();
        int groupIndex = 0;
        for ( int i = 0; i < projections.size(); i++ ) {
            if ( groupIndex < groupPositions.size() && groupPositions.get( groupIndex ) == i ) {
                output.add( context.rexBuilder.makeInputRef( aggregate, groupIndex++ ) );
            } else {
                output.add( projections.get( i ).right.accept( postAggregation ) );
            }
        }
        return LogicalLpgProject.create( aggregate, output, Pair.left( projections ) );
    }


    private AlgNode getProject( CypherContext context, List<Pair<PolyString, RexNode>> nameAndProject ) {
        AlgNode node = context.pop();

        if ( node == null ) {
            // A projection without input evaluates once, including when it contains expressions.
            List<Pair<PolyString, RexNode>> literals = nameAndProject.stream().allMatch( pair -> pair.right.isA( Kind.LITERAL ) )
                    ? nameAndProject : List.of();
            node = context.asValues( literals );
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

        if ( node.getTupleType().equals( project.getTupleType() ) && RexUtil.isIdentity( Pair.right( nameAndProject ), node.getTupleType() ) ) {
            return node;
        }
        return project;
    }


    public AlgNode getGraphSort( AlgNode node, List<Integer> sortFields ) {
        List<AlgFieldCollation> collations = new ArrayList<>();
        for ( int i = 0; i < sortFields.size(); i++ ) {
            Direction direction = order.get( i ).isAsc() ? Direction.ASCENDING : Direction.DESCENDING;
            collations.add( new AlgFieldCollation( sortFields.get( i ), direction ) );
        }
        AlgCollation collation = AlgCollations.of( collations );
        Integer skip = this.skip == null ? null : getRowBound( this.skip, "SKIP" );
        Integer limit = this.limit == null ? null : getRowBound( this.limit, "LIMIT" );
        AlgTraitSet traitSet = node.getTraitSet().replace( collation );
        return new LogicalLpgSort( node.getCluster(), traitSet, collation, node, skip, limit );
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

}
