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

package org.polypheny.db.cypher.set;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.cypher.expression.CypherExpression.ExpressionType;
import org.polypheny.db.cypher.expression.CypherVariable;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.runtime.PolyCollections.PolyDictionary;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

@Getter
public class CypherSetVariable extends CypherSetItem {

    private final CypherVariable variable;
    private final CypherExpression expression;
    private final boolean increment;


    /**
     * MATCH (p {name: 'Peter'})
     * SET p += {age: 38, hungry: true, position: 'Entrepreneur'} // adds to node
     *
     * SET p = {age: 38, hungry: true, position: 'Entrepreneur'} // replaces properties
     */
    public CypherSetVariable( CypherVariable variable, CypherExpression expression, boolean increment ) {

        this.variable = variable;
        this.expression = expression;
        this.increment = increment;
    }


    @Override
    public void convertItem( CypherContext context ) {
        String nodeName = variable.getName();
        AlgNode node = context.peek();
        int index = node.getRowType().getFieldNames().indexOf( nodeName );
        if ( index < 0 ) {
            throw new RuntimeException( String.format( "Unknown variable with name %s", nodeName ) );
        }
        AlgDataTypeField field = node.getRowType().getFieldList().get( index );

        RexNode ref = context.getRexNode( nodeName );
        if ( ref == null ) {
            ref = context.rexBuilder.makeInputRef( field.getType(), index );
        }

        assert expression.getType() == ExpressionType.LITERAL;
        Comparable<?> value = expression.getComparable();
        assert value instanceof Map;

        RexNode op = context.rexBuilder.makeCall(
                field.getType(),
                OperatorRegistry.get( QueryLanguage.CYPHER, OperatorName.CYPHER_SET_PROPERTIES ),
                List.of(
                        ref,
                        context.rexBuilder.makeArray(
                                context.typeFactory.createArrayType( context.typeFactory.createPolyType( PolyType.VARCHAR, 255 ), -1 ),
                                ((PolyDictionary) value).keySet().stream().map( o -> context.rexBuilder.makeLiteral( o, context.typeFactory.createPolyType( PolyType.VARCHAR, 255 ), false ) ).collect( Collectors.toList() ) ),
                        context.rexBuilder.makeArray(
                                context.typeFactory.createArrayType( context.typeFactory.createPolyType( PolyType.ANY ), -1 ),
                                ((PolyDictionary) value).values().stream().map( o -> context.rexBuilder.makeLiteral( o, context.typeFactory.createPolyType( PolyType.ANY ), false ) ).collect( Collectors.toList() ) ),
                        context.rexBuilder.makeLiteral( !increment ) ) );

        context.add( Pair.of( nodeName, op ) );
    }

}
