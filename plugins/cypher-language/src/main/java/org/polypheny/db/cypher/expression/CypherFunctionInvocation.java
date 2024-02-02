/*
 * Copyright 2019-2023 The Polypheny Project
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
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.Pair;

@Getter
public class CypherFunctionInvocation extends CypherExpression {

    private static final List<String> operatorNames = Arrays.stream( OperatorName.values() ).map( Enum::name )
            .map( s -> s.replace( "CYPHER_", "" ) ).toList();
    private final ParserPos namePos;
    private final List<String> namespace;
    private final boolean distinct;
    private final List<CypherExpression> arguments;
    private final OperatorName op;


    public CypherFunctionInvocation( ParserPos beforePos, ParserPos namePos, List<String> namespace, String image, boolean distinct, List<CypherExpression> arguments ) {
        super( beforePos );
        this.namePos = namePos;
        this.namespace = namespace;
        if ( operatorNames.contains( image.toUpperCase( Locale.ROOT ) ) ) {
            this.op = OperatorName.valueOf( "CYPHER_" + image.toUpperCase( Locale.ROOT ) );
        } else {
            throw new GenericRuntimeException( "Used function is not supported!" );
        }
        this.distinct = distinct;
        this.arguments = arguments;
    }


    @Override
    public Pair<PolyString, RexNode> getRex( CypherContext context, RexType type ) {
        List<RexNode> functionArguments = arguments.stream().map( arg -> arg.getRex( context, type ).right ).toList();
        return Pair.of( PolyString.of( op.name() ),
                context.rexBuilder.makeCall(
                        getOperationReturnType( context ),
                        OperatorRegistry.get( QueryLanguage.from( "cypher" ), op ),
                        functionArguments ) );
    }


    private AlgDataType getOperationReturnType( CypherContext context ) {
        return switch ( op ) {
            case CYPHER_GEO_DISTANCE -> context.cluster.getTypeFactory().createPolyType( PolyType.FLOAT );
            default -> context.cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN );
        };
    }

}
