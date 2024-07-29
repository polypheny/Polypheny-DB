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
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PathType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.Pair;

@Getter
public class CypherVariable extends CypherExpression {

    private final String name;


    public CypherVariable( ParserPos pos, String name ) {
        super( pos );
        this.name = name;
    }


    @Override
    public CypherVariable getVariable() {
        return super.getVariable();
    }


    @Override
    public Pair<PolyString, RexNode> getRex( CypherContext context, RexType type ) {
        AlgNode node = context.peek();

        int index = node.getTupleType().getFieldNames().indexOf( name );

        if ( index >= 0 ) {
            // search r  -> RowType(r:Edge)
            return Pair.of(
                    PolyString.of( name ),
                    context.rexBuilder.makeInputRef( node.getTupleType().getFields().get( index ).getType(), index ) );
        }

        for ( AlgDataTypeField field : node.getTupleType().getFields() ) {
            if ( field.getType() instanceof PathType ) {
                for ( AlgDataTypeField pathField : field.getType().getFields() ) {
                    if ( pathField.getName().equals( name ) ) {
                        // search r -> RowType(Path(r:Edge, n:Node))
                        RexIndexRef pathRef = context.rexBuilder.makeInputRef( field.getType(), field.getIndex() );
                        return Pair.of(
                                PolyString.of( name ),
                                context.rexBuilder.makeCall(
                                        pathField.getType(),
                                        OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_EXTRACT_FROM_PATH ),
                                        List.of( pathRef, context.rexBuilder.makeLiteral( pathField.getName() ) ) ) );
                    }
                }
            }
        }

        throw new GenericRuntimeException( "The used variable is not known." );
    }


    @Override
    public ExpressionType getType() {
        return ExpressionType.VARIABLE;
    }


}
