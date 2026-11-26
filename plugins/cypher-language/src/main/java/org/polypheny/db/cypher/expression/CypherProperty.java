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

import lombok.Getter;
import org.polypheny.db.cypher.cypher2alg.CypherSyntaxException;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.cypher.cypher2alg.CypherTypeException;
import org.polypheny.db.cypher.parser.StringPos;
import org.polypheny.db.functions.CypherFunctions;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
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
    public PolyValue getComparable() {
        return CypherFunctions.extractProperty( subject.getComparable(), PolyString.of( getPropertyKey() ) );
    }


    @Override
    protected Pair<PolyString, RexNode> toRex( CypherContext context, RexType type ) {
        RexNode operand = subject.getRex( context, type ).right;
        switch ( operand.getType().getPolyType() ) {
            case MAP, DOCUMENT, NODE, EDGE, ANY, NULL, DATE, TIME, TIMESTAMP, INTERVAL, CYPHER_TEMPORAL -> {
            }
            case PATH -> throw new CypherSyntaxException( "InvalidArgumentType: A path does not expose properties." );
            default -> throw new CypherTypeException( "InvalidArgumentType: Property access requires a map, node, relationship, or temporal value." );
        }
        if ( subject.getType() == ExpressionType.VARIABLE ) {
            return context.getPropertyExtract( getPropertyKey(), getSubjectName(), operand );
        }
        return Pair.of( null, context.getPropertyExtract( getPropertyKey(), null, operand ).right );
    }

}
