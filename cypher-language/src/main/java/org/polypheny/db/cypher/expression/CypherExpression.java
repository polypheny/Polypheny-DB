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

import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.cypher.CypherNode;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.pattern.CypherPattern;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.Pair;

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

        OperatorName operatorName;
        switch ( type ) {
            case PATTERN:
                // EveryPathPattern
                return pattern.getPatternFilter( context );
            case ALL:
                operatorName = OperatorName.CYPHER_ALL_MATCH;
                break;
            case ANY:
                operatorName = OperatorName.CYPHER_ANY_MATCH;
                break;
            case NONE:
                operatorName = OperatorName.CYPHER_NONE_MATCH;
                break;
            case SINGLE:
                operatorName = OperatorName.CYPHER_SINGLE_MATCH;
                break;
            default:
                throw new UnsupportedOperationException();
        }

        //  ANY ( Variable IN Expression(list) Where? )
        //  MATCH p = (a)-[*1..3]->(b)
        //  WHERE
        //  a.name = 'Alice'
        //  AND b.name = 'Daniel'
        //  AND all(x IN nodes(p) WHERE x.age > 30)
        //  RETURN p

        return new RexCall(
                context.booleanType,
                OperatorRegistry.get( operatorName ),
                List.of( context.rexBuilder.makeInputRef( context.graphType, 0 ), where.getRexNode( context ) ) );
    }


    public Pair<String, RexNode> getRexAsProject( CypherContext context ) {
        throw new UnsupportedOperationException();
    }


    public enum ExpressionType {
        ALL, NONE, SINGLE, PATTERN, ANY, DEFAULT
    }


    public Comparable<?> getComparable() {
        throw new UnsupportedOperationException();
    }

}
