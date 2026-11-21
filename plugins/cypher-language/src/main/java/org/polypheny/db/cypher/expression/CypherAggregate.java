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
import javax.annotation.Nullable;
import lombok.Getter;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.LaxAggregateCall;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.Pair;

public class CypherAggregate extends CypherExpression {

    public final OperatorName op;
    @Getter
    private final CypherExpression target;
    @Getter
    private final boolean distinct;


    public CypherAggregate( ParserPos pos, OperatorName op, @Nullable CypherExpression target, boolean distinct ) {
        super( pos );
        this.op = op;
        this.target = target;
        this.distinct = distinct;
        if ( distinct && op != OperatorName.COUNT && op != OperatorName.COLLECT ) {
            throw new GenericRuntimeException( "DISTINCT is supported for COUNT and COLLECT aggregations." );
        }
    }


    public Pair<PolyString, RexNode> getAggregate( CypherContext context, String alias ) {
        PolyString name = null;
        Pair<PolyString, RexNode> namedNode = null;
        if ( target != null ) {
            namedNode = target.getRex( context, RexType.PROJECT );
            name = namedNode.left;
        }

        RexNode operand = namedNode != null ? namedNode.right : null;
        if ( operand != null && operand.getType().getPolyType() == PolyType.NULL ) {
            operand = context.rexBuilder.makeNullLiteral( context.typeFactory.createPolyType( PolyType.ANY ) );
        }
        Pair<RexNode, AlgDataType> typeAndValue = getReturnType( context, operand );

        AggregateCall call = AggregateCall.create(
                OperatorRegistry.getAgg( op ),
                distinct,
                false,
                List.of(),
                -1,
                AlgCollations.EMPTY,
                typeAndValue.right,
                alias != null ? alias : op.name()
        );

        context.addAgg( Pair.of( name != null ? name.value : null, call ) );

        if ( namedNode != null ) {
            return Pair.of( namedNode.left, typeAndValue.left );
        } else {
            return null;
        }

    }


    private Pair<RexNode, AlgDataType> getReturnType( CypherContext context, RexNode node ) {
        return switch ( op ) {
            case COLLECT -> Pair.of( node, context.typeFactory.createArrayType( Objects.requireNonNull( node ).getType(), -1 ) );
            case SUM, AVG -> Pair.of( node, LaxAggregateCall.inferGraphNumericType( context.typeFactory,
                    op == OperatorName.SUM ? Kind.SUM : Kind.AVG, Objects.requireNonNull( node ).getType() ) );
            case MIN, MAX -> Pair.of( node, context.typeFactory.createTypeWithNullability( Objects.requireNonNull( node ).getType(), true ) );
            default -> Pair.of( node, context.typeFactory.createPolyType( PolyType.BIGINT ) );
        };
    }


    @Override
    public ExpressionType getType() {
        return ExpressionType.AGGREGATE;
    }

}
