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

import lombok.Getter;
import org.polypheny.db.cypher.CypherNode;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.pattern.CypherPattern;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.rex.RexNode;

@Getter
public class CypherExpression extends CypherNode {

    private final ExpressionType type;
    private CypherVariable variable;
    private CypherExpression expression;
    private CypherExpression where;
    private CypherPattern pattern;


    public CypherExpression( ParserPos pos ) {
        super( pos );
        this.type = ExpressionType.DEFAULT;
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.EXPRESSION;
    }


    public CypherExpression( ParserPos pos, ExpressionType type, CypherVariable variable, CypherExpression expression, CypherExpression where ) {
        super( pos );
        this.type = type;
        this.variable = variable;
        this.expression = expression;
        this.where = where;
    }


    public CypherExpression( ParserPos pos, ExpressionType type, CypherPattern pattern ) {
        super( pos );
        this.type = type;
        this.pattern = pattern;
    }


    public RexNode getRexNode( CypherContext context ) {
    }


    public enum ExpressionType {
        ALL, NONE, SINGLE, PATTERN, ANY, DEFAULT
    }


    public Comparable<?> getComparable() {
        throw new UnsupportedOperationException();
    }

}
