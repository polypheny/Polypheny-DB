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

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import lombok.Getter;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.Pair;

@Getter
public class CypherFunctionInvocation extends CypherExpression {

    private final ParserPos namePos;
    private final List<String> namespace;
    private final boolean distinct;
    private final List<CypherExpression> arguments;

    private static final List<String> operatorNames = Arrays.stream( OperatorName.values() ).map( Enum::name ).toList();
    private final OperatorName op;


    public CypherFunctionInvocation( ParserPos beforePos, ParserPos namePos, List<String> namespace, String image, boolean distinct, List<CypherExpression> arguments ) {
        super( beforePos );
        this.namePos = namePos;
        this.namespace = namespace;
        if ( operatorNames.contains( image.toUpperCase( Locale.ROOT ) ) ) {
            this.op = OperatorName.valueOf( image.toUpperCase( Locale.ROOT ) );
        } else {
            throw new GenericRuntimeException( "Used function is not supported!" );
        }
        this.distinct = distinct;
        this.arguments = arguments;
    }


    @Override
    public Pair<PolyString, RexNode> getRex( CypherContext context, RexType type ) {
        if ( this.op == OperatorName.VECTOR_DISTANCE ) {
            return getVectorDistanceRex( context, type );
        }
        return super.getRex( context, type );
    }


    private Pair<PolyString, RexNode> getVectorDistanceRex( CypherContext context, RexType type ) {
        if ( arguments.size() != 3 ) {
            throw new GenericRuntimeException( "vector_distance requires exactly 3 arguments" );
        }

        RexNode v1 = arguments.get( 0 ).getRex( context, type ).right;
        RexNode v2 = arguments.get( 1 ).getRex( context, type ).right;

        RexNode metricRex = arguments.get( 2 ).getRex( context, type ).right;
        if ( !(metricRex instanceof RexLiteral metricLit) ) {
            throw new GenericRuntimeException( "vector_distance metric must be a string literal" );
        }
        String metric = metricLit.value.asString().value.toUpperCase( Locale.ROOT );

        OperatorName namedOp = switch ( metric ) {
            case "L1"      -> OperatorName.L1_DISTANCE;
            case "L2"      -> OperatorName.L2_DISTANCE;
            case "COSINE"  -> OperatorName.COS_DISTANCE;
            case "HAMMING" -> OperatorName.HAMMING_DISTANCE;
            case "JACCARD" -> OperatorName.JACCARD_DISTANCE;
            case "INNER_PRODUCT" -> OperatorName.INNER_PRODUCT_DISTANCE;
            // parameterized version
            case "CHISQUARED", "L2SQUARED" -> OperatorName.DISTANCE;
            default -> throw new GenericRuntimeException( "Unknown distance metric: ", metric );
        };
        Operator operator = OperatorRegistry.get( namedOp );

        if ( namedOp == OperatorName.DISTANCE ) {
            return Pair.of( PolyString.of( namedOp.name() ), context.rexBuilder.makeCall( operator, List.of( v1, v2, metricRex ) ) );
        }

        return Pair.of( PolyString.of( namedOp.name() ), context.rexBuilder.makeCall( operator, List.of( v1, v2 ) ) );
    }



}
