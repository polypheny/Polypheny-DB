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

package org.polypheny.db.cypher.remove;

import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.expression.CypherVariable;
import org.polypheny.db.cypher.parser.StringPos;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;

@Getter
public class CypherRemoveLabels extends CypherRemoveItem {

    private final CypherVariable variable;
    private final List<StringPos> labels;


    public CypherRemoveLabels( CypherVariable variable, List<StringPos> labels ) {
        this.variable = variable;
        this.labels = labels;
    }


    @Override
    public void removeItem( CypherContext context ) {
        AlgNode node = context.peek();
        int index = node.getTupleType().getFieldNames().indexOf( variable.getName() );
        if ( index < 0 ) {
            throw new GenericRuntimeException( String.format( "Unknown variable with name %s", variable ) );
        }
        AlgDataTypeField field = node.getTupleType().getFields().get( index );

        if ( field.getType().getPolyType() == PolyType.EDGE && labels.size() != 1 ) {
            throw new GenericRuntimeException( "Edges require exactly one label" );
        }

        RexNode ref = context.getRexNode( variable.getName() );
        if ( ref == null ) {
            ref = context.rexBuilder.makeInputRef( field.getType(), index );
        }

        RexNode op = context.rexBuilder.makeCall(
                field.getType(),
                OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_REMOVE_LABELS ),
                List.of(
                        ref,
                        context.rexBuilder.makeArray(
                                context.typeFactory.createArrayType( context.typeFactory.createPolyType( PolyType.VARCHAR, 255 ), -1 ),
                                labels.stream().map( l -> (PolyValue) PolyString.of( l.getImage() ) ).toList() ) ) );

        context.add( Pair.of( PolyString.of( variable.getName() ), op ) );
    }


}
