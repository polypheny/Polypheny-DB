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

package org.polypheny.db.cypher.set;

import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.cypher.expression.CypherProperty;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.Pair;

@Getter
public class CypherSetProperty extends CypherSetItem {

    private final CypherProperty property;
    private final CypherExpression expression;


    public CypherSetProperty( CypherProperty property, CypherExpression expression ) {
        this.property = property;
        this.expression = expression;
    }


    @Override
    public void convertItem( CypherContext context ) {
        String nodeName = property.getSubjectName();
        AlgNode node = context.peek();
        int index = node.getTupleType().getFieldNames().indexOf( nodeName );
        if ( index < 0 ) {
            throw new GenericRuntimeException( String.format( "Unknown variable with name %s", nodeName ) );
        }
        AlgDataTypeField field = node.getTupleType().getFields().get( index );

        RexNode ref = context.getRexNode( nodeName );
        if ( ref == null ) {
            ref = context.rexBuilder.makeInputRef( field.getType(), index );
        }

        RexNode op = context.rexBuilder.makeCall(
                field.getType(),
                OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_SET_PROPERTY ),
                List.of(
                        ref,
                        context.rexBuilder.makeLiteral( getProperty().getPropertyKey() ),
                        expression.getRex( context, RexType.PROJECT ).right ) );

        context.add( Pair.of( PolyString.of( nodeName ), op ) );

    }

}
