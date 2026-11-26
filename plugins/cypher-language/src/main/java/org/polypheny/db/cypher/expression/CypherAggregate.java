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

package org.polypheny.db.cypher.expression;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import javax.annotation.Nullable;
import lombok.Getter;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.LaxAggregateCall;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.cypher.cypher2alg.CypherSyntaxException;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.Pair;

public class CypherAggregate extends CypherExpression {

    public final OperatorName op;
    @Getter
    private final CypherExpression target;
    @Getter
    private final boolean distinct;
    @Nullable
    private final CypherExpression percentile;
    private final boolean continuous;


    public CypherAggregate( ParserPos pos, OperatorName op, @Nullable CypherExpression target, boolean distinct ) {
        this( pos, op, target, distinct, null, false );
    }


    public CypherAggregate( ParserPos pos, CypherExpression target, CypherExpression percentile, boolean distinct, boolean continuous ) {
        this( pos, OperatorName.CYPHER_PERCENTILE, target, distinct, percentile, continuous );
    }


    private CypherAggregate( ParserPos pos, OperatorName op, @Nullable CypherExpression target, boolean distinct,
            @Nullable CypherExpression percentile, boolean continuous ) {
        super( pos );
        this.op = op;
        this.target = target;
        this.distinct = distinct;
        this.percentile = percentile;
        this.continuous = continuous;
        if ( distinct && op != OperatorName.COUNT && op != OperatorName.COLLECT && op != OperatorName.STDDEV_SAMP && op != OperatorName.STDDEV_POP
                && op != OperatorName.CYPHER_PERCENTILE ) {
            throw new CypherSyntaxException( "DISTINCT is supported for count(), collect(), stDev(), stDevP(), percentileCont(), and percentileDisc()." );
        }
    }


    @Override
    protected Pair<PolyString, RexNode> toRex( CypherContext context, RexType type ) {
        RexNode operand = target == null ? null : aggregateOperand( context, target );
        if ( percentile != null ) {
            return Pair.of( PolyString.of( op.name() ), percentile( context, Objects.requireNonNull( operand ), aggregateOperand( context, percentile ) ) );
        }
        if ( operand != null && operand.getType().getPolyType() == PolyType.NULL ) {
            operand = context.rexBuilder.makeNullLiteral( context.typeFactory.createPolyType( PolyType.ANY ) );
        }
        Pair<RexNode, AlgDataType> typeAndValue = getReturnType( context, operand );
        return Pair.of( PolyString.of( op.name() ), addAggregate( context, op, distinct, typeAndValue.left, typeAndValue.right, value -> value ) );
    }


    private static RexNode aggregateOperand( CypherContext context, CypherExpression expression ) {
        RexNode operand = context.withAggregateOperand( () -> expression.getRex( context, RexType.PROJECT ).right );
        if ( RexUtil.findOperatorCall( OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_RAND ), operand ) != null ) {
            throw new CypherSyntaxException( "NonConstantExpression: Evaluate rand() in a preceding WITH clause before aggregating it." );
        }
        return operand;
    }


    private RexNode percentile( CypherContext context, RexNode value, RexNode fraction ) {
        QueryLanguage language = QueryLanguage.from( "cypher" );
        AlgDataType any = context.typeFactory.createPolyType( PolyType.ANY );
        // Preserve order and both arguments through the unary collection aggregate.
        // The finalizer selects the group's first fraction and applies DISTINCT to values only.
        AlgDataType pairType = context.typeFactory.createArrayType( any, 2 );
        RexNode pair = context.rexBuilder.makeCall( pairType, OperatorRegistry.get( language, OperatorName.CYPHER_LIST_CONSTRUCTOR ), List.of( value, fraction ) );
        RexNode fallback = fraction instanceof RexLiteral ? fraction : context.rexBuilder.makeNullLiteral( any );
        AlgDataType result = context.typeFactory.createTypeWithNullability( continuous ? any : value.getType(), true );
        return addAggregate( context, OperatorName.COLLECT, false, pair, context.typeFactory.createArrayType( pairType, -1 ),
                pairs -> context.rexBuilder.makeCall( result, OperatorRegistry.get( language, OperatorName.CYPHER_PERCENTILE ),
                        List.of( pairs, fallback, context.rexBuilder.makeLiteral( continuous ), context.rexBuilder.makeLiteral( distinct ) ) ) );
    }


    private static RexNode addAggregate( CypherContext context, OperatorName op, boolean distinct, @Nullable RexNode operand, AlgDataType type,
            Function<RexNode, RexNode> finalizer ) {
        AggregateCall call = AggregateCall.create(
                OperatorRegistry.getAgg( op ), distinct, false, List.of(), -1,
                AlgCollations.EMPTY, type, op.name()
        );
        return context.addAggregate( call, operand, finalizer );
    }


    private Pair<RexNode, AlgDataType> getReturnType( CypherContext context, RexNode node ) {
        return switch ( op ) {
            case COLLECT -> Pair.of( node, context.typeFactory.createArrayType( Objects.requireNonNull( node ).getType(), -1 ) );
            case SUM, AVG -> Pair.of( node, LaxAggregateCall.inferGraphNumericType( context.typeFactory,
                    op == OperatorName.SUM ? Kind.SUM : Kind.AVG, Objects.requireNonNull( node ).getType() ) );
            case MIN, MAX -> Pair.of( node, context.typeFactory.createTypeWithNullability( Objects.requireNonNull( node ).getType(), true ) );
            case STDDEV_SAMP, STDDEV_POP -> {
                PolyType type = Objects.requireNonNull( node ).getType().getPolyType();
                if ( type != PolyType.ANY && !PolyType.NUMERIC_TYPES.contains( type ) ) {
                    throw new CypherSyntaxException( (op == OperatorName.STDDEV_SAMP ? "stDev()" : "stDevP()") + " requires a number." );
                }
                yield Pair.of( node, context.typeFactory.createPolyType( PolyType.DOUBLE ) );
            }
            default -> Pair.of( node, context.typeFactory.createPolyType( PolyType.BIGINT ) );
        };
    }


    @Override
    public ExpressionType getType() {
        return ExpressionType.AGGREGATE;
    }

}
