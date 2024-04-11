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
import lombok.Getter;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.cypher.parser.StringPos;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.Pair;

@Getter
public class CypherProperty extends CypherExpression {

    private final CypherExpression subject;
    private final StringPos propKeyName;


    public CypherProperty( CypherExpression subject, StringPos propKeyName ) {
        super( ParserPos.ZERO );
        this.subject = subject;
        this.propKeyName = propKeyName;
    }


    @Override
    public String getName() {
        if ( subject.getType() == ExpressionType.VARIABLE ) {
            return subject.getName() + "." + propKeyName.getImage();
        }
        throw new UnsupportedOperationException();
    }


    public String getPropertyKey() {
        return this.propKeyName.getImage();
    }


    public String getSubjectName() {
        return this.getSubject().getName();
    }


    @Override
    public Pair<PolyString, RexNode> getRex( CypherContext context, RexType type ) {
        String key = propKeyName.getImage();
        String subject = this.subject.getName();

        Pair<PolyString, RexNode> namedSubject = this.subject.getRex( context, type );
        assert namedSubject.left.equals( PolyString.of( subject ) );

        if ( type == RexType.FILTER ) {
            // first check if property even exist, this is use if it is used as filter
            RexNode hasProperty = context.rexBuilder.makeCall(
                    context.booleanType,
                    OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_HAS_PROPERTY ),
                    List.of( namedSubject.right, context.rexBuilder.makeLiteral( key ) ) );

            context.add( Pair.of( PolyString.of( subject + "." + key ), hasProperty ) );
        }

        return context.getPropertyExtract( key, subject, namedSubject.right );
    }

}
