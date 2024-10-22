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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.Pair;

import static org.polypheny.db.algebra.operators.OperatorName.CYPHER_POINT;

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
        } else if ( operatorNames.contains( "CYPHER_" + image.toUpperCase( Locale.ROOT ) ) ) {
            this.op = OperatorName.valueOf( "CYPHER_" + image.toUpperCase( Locale.ROOT ) );
        } else {
            throw new GenericRuntimeException( "Used function is not supported!" );
        }
        this.distinct = distinct;
        this.arguments = arguments;
    }


    public ImmutableList<CypherExpression> getArguments() {
        return ImmutableList.copyOf( arguments );
    }


    public OperatorName getOperatorName() {
        return op;
    }


    @Override
    public Pair<PolyString, RexNode> getRex( CypherContext context, RexType type ) {
        // At this point, we do not know what is on the left side of the Pair.
        // The caller has to discard the left side, and use a variable name or something else.
        return Pair.of( PolyString.of( "???" ), getRexCall( context ));
    }

    public RexNode getRexCall( CypherContext context) {
        switch ( getOperatorName() ) {
            case CYPHER_POINT: {
                // VERY UGLY, but it works for now. This could be improved by using the function MAP_OF_ENTRIES,
                // but I am not sure how to call it.
                CypherLiteral mapExpression = (CypherLiteral) getArguments().get( 0 );
                List<RexNode> arguments = new ArrayList<>();
                mapExpression.getMapValue().forEach( ( key, value ) -> {
                    Pair<PolyString, RexNode> pair = value.getRex( context, RexType.PROJECT );
                    arguments.add( context.rexBuilder.makeLiteral( key ) );
                    arguments.add( pair.right );
                } );
                // Fill with NULL to make sure we have the correct amount of arguments.
                // 3 coordinates + 3 names + srid + crs = up to 8 possible
                while ( arguments.size() < 10 ) {
                    arguments.add( context.rexBuilder.makeNullLiteral( context.typeFactory.createUnknownType() ) );
                }
                return new RexCall(
                        context.geometryType,
                        OperatorRegistry.get( QueryLanguage.from( "cypher" ), CYPHER_POINT ),
                        arguments );

            }
            case DISTANCE: {
                return new RexCall(
                        context.numberType,
                        OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.DISTANCE ),
                        List.of(
                                arguments.get( 0 ).getRex( context, RexType.PROJECT ).getRight(),
                                arguments.get( 1 ).getRex( context, RexType.PROJECT ).getRight()
                        ) );
            }
            case CYPHER_WITHINBBOX:
                return new RexCall(
                        context.booleanType,
                        OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_WITHINBBOX ),
                        List.of(
                                arguments.get( 0 ).getRex( context, RexType.PROJECT ).getRight(),
                                // CypherFunctionInvocation.getRex -> throw
                                // Because create function logic is implemented in
                                arguments.get( 1 ).getRex( context, RexType.PROJECT ).getRight(),
                                arguments.get( 2 ).getRex( context, RexType.PROJECT ).getRight()
                        ) );
            default:
                throw new NotImplementedException( "Cypher Function to alg conversion missing: " + getOperatorName() );
        }
    }

}
