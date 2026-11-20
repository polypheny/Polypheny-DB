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
import java.util.stream.IntStream;
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


    public AlgNode getGraphProject( CypherContext context ) {
        List<Pair<PolyString, RexNode>> projections = new ArrayList<>();
        List<Pair<PolyString, RexNode>> operands = new ArrayList<>();
        List<Integer> requestedOrder = new ArrayList<>();
        for ( CypherReturn item : returnItems ) {
            Pair<PolyString, RexNode> expression = item.getRex( context, RexType.PROJECT );
            if ( item.isStar() ) {
                for ( Pair<PolyString, RexNode> field : context.popNodes() ) {
                    requestedOrder.add( projections.size() );
                    projections.add( field );
                }
            } else if ( item instanceof CypherReturnItem value && value.getExpression() instanceof CypherAggregate ) {
                requestedOrder.add( -1 - operands.size() );
                operands.add( expression );
            } else {
                requestedOrder.add( projections.size() );
                projections.add( expression.left == null
                        ? Pair.of( PolyString.of( "EXPR$" + projections.size() ), expression.right )
                        : expression );
            }
        }

        AlgNode result = context.containsAggs()
                ? getAggregate( context, projections, operands, requestedOrder )
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


    private AlgNode getAggregate(
            CypherContext context, List<Pair<PolyString, RexNode>> groups, List<Pair<PolyString, RexNode>> operands, List<Integer> requestedOrder ) {
        List<Pair<String, AggregateCall>> aggregates = context.popAggNodes();
        List<Pair<PolyString, RexNode>> inputProjections = new ArrayList<>( groups );
        List<Integer> operandIndexes = new ArrayList<>();
        List<PolyString> fieldNames = new ArrayList<>( Pair.left( groups ) );
        for ( Pair<PolyString, RexNode> operand : operands ) {
            if ( operand == null ) {
                operandIndexes.add( -1 );
                continue;
            }

            // The argument is resolved in the input scope, independently of any aliases in the output.
            PolyString name = operand.left;
            if ( name == null || name.value.startsWith( "$" ) || fieldNames.contains( name ) ) {
                name = PolyString.of( "_aggregateInput" + operandIndexes.size() );
                while ( fieldNames.contains( name ) ) {
                    name = PolyString.of( name.value + "_" );
                }
            }
            operandIndexes.add( inputProjections.size() );
            inputProjections.add( Pair.of( name, operand.right ) );
            fieldNames.add( name );
        }

        AlgNode input = getProject( context, inputProjections );
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
            AggregateCall aggregate = aggregates.get( i ).right;
            int operandIndex = operandIndexes.get( i );
            LaxAggregateCall call = operandIndex < 0
                    ? LaxAggregateCall.from( aggregate, null )
                    : LaxAggregateCall.from( aggregate.adjustedCopy( List.of( operandIndex ) ), input );
            calls.add( call );
            groupedType.add( aggregate.name, null, call.getType( input.getCluster() ) );
        }

        AlgNode aggregate = new LogicalLpgAggregate(
                input.getCluster(), input.getTraitSet(), input, groupIndexes, calls, groupedType.build() );
        List<Integer> positions = requestedOrder.stream()
                .map( index -> index < 0 ? groups.size() - 1 - index : index )
                .toList();
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


    public AlgNode getGraphSort( CypherContext context ) {
        AlgNode node = context.peek();
        List<String> fields = node.getTupleType().getFieldNames();
        List<AlgFieldCollation> collations = new ArrayList<>();

        for ( CypherOrderItem item : order ) {
            String name = sortField( item.getExpression(), fields );
            int index = fields.indexOf( name );
            if ( index < 0 ) {
                throw new GenericRuntimeException( "Unknown sort expression: %s", name );
            }
            collations.add( new AlgFieldCollation( index, item.isAsc() ? Direction.ASCENDING : Direction.DESCENDING ) );
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


    public List<String> getSortFields( List<String> projectedFields ) {
        return order.stream().map( item -> sortField( item.getExpression(), projectedFields ) ).toList();
    }


    private String sortField( CypherExpression expression, List<String> projectedFields ) {
        String name = expression.getName();
        if ( projectedFields.contains( name ) ) {
            return name;
        }
        // A grouping expression remains sortable through its projected alias.
        for ( CypherReturn item : returnItems ) {
            if ( item instanceof CypherReturnItem projection && projection.getVariable() != null ) {
                CypherExpression source = projection.getExpression();
                if ( (source instanceof CypherProperty || source instanceof CypherVariable) && name.equals( source.getName() ) ) {
                    return projection.getVariable().getName();
                }
            }
        }
        return name;
    }

}
