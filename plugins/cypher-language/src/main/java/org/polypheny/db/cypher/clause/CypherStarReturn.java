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

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.cypher.cypher2alg.CypherSyntaxException;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.Pair;


public class CypherStarReturn extends CypherReturn {

    protected CypherStarReturn( ParserPos pos ) {
        super( pos );
    }


    @Override
    public boolean isStar() {
        return true;
    }


    public List<Pair<PolyString, RexNode>> expand( CypherContext context, Set<String> explicitNames, boolean withClause ) {
        List<AlgDataTypeField> fields = context.hasInput()
                ? context.peek().getTupleType().getFields().stream()
                        .filter( field -> field.getName() != null && context.isVisibleVariable( field.getName() ) )
                        .toList()
                : List.of();
        if ( fields.isEmpty() && !withClause ) {
            throw new CypherSyntaxException( "RETURN * requires a variable in scope." );
        }
        // Explicit columns replace names from the wildcard and keep their requested position.
        return fields.stream()
                .filter( field -> !explicitNames.contains( field.getName() ) )
                .sorted( Comparator.comparing( AlgDataTypeField::getName ) )
                .map( field -> Pair.<PolyString, RexNode>of( PolyString.of( field.getName() ),
                        context.rexBuilder.makeInputRef( field.getType(), field.getIndex() ) ) )
                .toList();
    }

}
