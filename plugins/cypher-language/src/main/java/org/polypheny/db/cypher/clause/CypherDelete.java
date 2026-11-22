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

import java.util.List;
import lombok.Getter;
import org.polypheny.db.adapter.GraphDeleteTargets;
import org.polypheny.db.cypher.cypher2alg.CypherSyntaxException;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.Pair;


@Getter
public class CypherDelete extends CypherClause {


    private final boolean detach;
    private final List<CypherExpression> expressions;


    public CypherDelete( ParserPos pos, boolean detach, List<CypherExpression> expressions ) {
        super( pos );
        this.detach = detach;
        this.expressions = expressions;
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.DELETE;
    }


    public void getDelete( CypherContext context ) {
        if ( !context.hasInput() ) {
            context.add( context.asValues( List.of() ) );
        }
        List<Pair<PolyString, RexNode>> targets = expressions.stream()
                .map( expression -> expression.getRex( context, RexType.PROJECT ) )
                .toList();
        for ( Pair<PolyString, RexNode> target : targets ) {
            if ( !GraphDeleteTargets.supportsType( target.right.getType().getPolyType() ) ) {
                throw new CypherSyntaxException( "Graph deletes require a node, relationship, or path binding" );
            }
            context.add( target );
        }

        context.combineDelete( detach );
    }

}
