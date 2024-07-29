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

package org.polypheny.db.cypher;

import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.cypher.parser.StringPos;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.Pair;

@Getter
public class CypherHasLabelOrTypes extends CypherExpression {

    private final CypherExpression subject;
    private final List<StringPos> labels;


    protected CypherHasLabelOrTypes( CypherExpression subject, List<StringPos> labels ) {
        super( ParserPos.ZERO );
        this.subject = subject;
        this.labels = labels;
    }


    @Override
    public Pair<PolyString, RexNode> getRex( CypherContext context, RexType type ) {

        Pair<PolyString, RexNode> namedSubject = subject.getRex( context, type );

        RexNode hasLabels;
        if ( labels.size() == 1 ) {
            hasLabels = context.rexBuilder.makeCall(
                    context.booleanType,
                    OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_HAS_LABEL ),
                    List.of( namedSubject.right, context.rexBuilder.makeLiteral( labels.get( 0 ).getImage() ) ) );
        } else {
            throw new UnsupportedOperationException();
        }

        return Pair.of( null, hasLabels );
    }

}
