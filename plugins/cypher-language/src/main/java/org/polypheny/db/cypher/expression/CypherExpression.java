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

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgValues;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.cypher.CypherNode;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.cypher.pattern.CypherPattern;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
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


    public Pair<PolyString, RexNode> getRex( CypherContext context, RexType type ) {
        OperatorName operatorName = switch ( this.type ) {
            // EveryPathPattern
            //return pattern.getPatternMatch( context );
            case PATTERN, ALL -> OperatorName.CYPHER_ALL_MATCH;
            case ANY -> OperatorName.CYPHER_ANY_MATCH;
            case NONE -> OperatorName.CYPHER_NONE_MATCH;
            case SINGLE -> OperatorName.CYPHER_SINGLE_MATCH;
            default -> throw new UnsupportedOperationException();
        };

        //  ANY ( Variable IN Expression(list) Where? )
        //  MATCH p = (a)-[*1..3]->(b)
        //  WHERE
        //  a.name = 'Alice'
        //  AND b.name = 'Daniel'
        //  AND all(x IN nodes(p) WHERE x.age > 30)
        //  RETURN p

        String var = variable.getName();

        return Pair.of( PolyString.of( var ), new RexCall(
                context.booleanType,
                OperatorRegistry.get( operatorName ),
                List.of( context.rexBuilder.makeInputRef( context.graphType, 0 ), where.getRex( context, type ).right ) ) );
    }


    public String getName() {
        throw new UnsupportedOperationException();
    }


    public Pair<PolyString, RexNode> getValues( CypherContext context, String name ) {
        Pair<PolyString, RexNode> namedNode = getRex( context, RexType.PROJECT );
        if ( namedNode.right.isA( Kind.LITERAL ) ) {
            ImmutableList<ImmutableList<RexLiteral>> values = ImmutableList.of( ImmutableList.of( (RexLiteral) namedNode.right ) );

            AlgRecordType rowType = new AlgRecordType( List.of( new AlgDataTypeFieldImpl( -1L, name, 0, namedNode.right.getType() ) ) );
            LogicalLpgValues node = LogicalLpgValues.create( context.cluster, context.cluster.traitSet(), rowType, values );

            context.add( node );
        } else {
            throw new UnsupportedOperationException();
        }

        return namedNode;
    }


    public enum ExpressionType {
        ALL, NONE, SINGLE, PATTERN, ANY, VARIABLE, AGGREGATE, DEFAULT, LITERAL
    }


    public PolyValue getComparable() {
        throw new UnsupportedOperationException();
    }

}
