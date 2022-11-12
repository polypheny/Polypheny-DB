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

package org.polypheny.db.cypher.expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

public class CypherAggregate extends CypherExpression {

    private final OperatorName op;
    private final CypherExpression target;
    private final boolean distinct;


    public CypherAggregate( ParserPos pos, OperatorName op, @Nullable CypherExpression target, boolean distinct ) {
        super( pos );
        this.op = op;
        this.target = target;
        this.distinct = distinct;
        if ( this.op != OperatorName.COUNT && distinct ) {
            throw new RuntimeException( "Only count([name]) aggregations can be DISTINCT." );
        }
    }


    public Pair<String, RexNode> getAggregate( CypherContext context, String alias ) {
        List<Integer> aggIndexes = new ArrayList<>();

        String name = null;
        Pair<String, RexNode> namedNode = null;
        if ( target != null ) {
            namedNode = target.getRex( context, RexType.PROJECT );
            name = namedNode.left;
        }

        Pair<RexNode, AlgDataType> typeAndValue = getReturnType( context, namedNode != null ? namedNode.right : null );

        AggregateCall call = AggregateCall.create(
                OperatorRegistry.getAgg( op ),
                distinct,
                false,
                aggIndexes,
                -1,
                AlgCollations.EMPTY,
                typeAndValue.right,
                alias != null ? alias : String.format( "%s(%s)", op.name(), name )
        );

        context.addAgg( Pair.of( name, call ) );

        if ( namedNode != null ) {
            return Pair.of( namedNode.left, typeAndValue.left );
        } else {
            return null;
        }

    }


    private Pair<RexNode, AlgDataType> getReturnType( CypherContext context, RexNode node ) {
        switch ( op ) {
            case COLLECT:
                Pair<String, RexNode> rex = Objects.requireNonNull( this.target ).getRex( context, RexType.PROJECT );
                return Pair.of( node, context.typeFactory.createArrayType( rex.getValue().getType(), -1 ) );
            case AVG:
                RexNode casted = context.rexBuilder.makeCast( context.typeFactory.createTypeWithNullability( context.typeFactory.createPolyType( PolyType.DOUBLE ), true ), node );
                return Pair.of( casted, context.typeFactory.createTypeWithNullability( context.typeFactory.createPolyType( PolyType.DOUBLE ), true ) );
            default:
                return Pair.of( node, context.typeFactory.createPolyType( PolyType.BIGINT ) );
        }
    }


    @Override
    public ExpressionType getType() {
        return ExpressionType.AGGREGATE;
    }

}
