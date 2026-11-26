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

import java.util.List;
import lombok.Getter;
import org.polypheny.db.cypher.clause.CypherClause;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.Pair;

@Getter
public class CypherExistSubQuery extends CypherExpression {

    private final List<CypherClause> clauses;


    public CypherExistSubQuery( ParserPos pos, List<CypherClause> clauses ) {
        super( pos );
        this.clauses = List.copyOf( clauses );
    }


    @Override
    public Pair<PolyString, RexNode> toRex( CypherContext context, RexType type ) {
        return Pair.of( null, context.existsSubquery( this ) );
    }

}
