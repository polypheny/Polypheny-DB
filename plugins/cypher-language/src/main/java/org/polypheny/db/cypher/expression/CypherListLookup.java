/*
 * Copyright 2019-2023 The Polypheny Project
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
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.cypher.cypher2alg.CypherSyntaxException;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.Pair;

@Getter
public class CypherListLookup extends CypherExpression {


    private final CypherExpression subject;
    private final CypherExpression list;


    public CypherListLookup( CypherExpression subject, CypherExpression list ) {
        super( ParserPos.ZERO );
        this.subject = subject;
        this.list = list;
    }


    @Override
    public Pair<PolyString, RexNode> getRex( CypherContext context, RexType type ) {
        RexNode value = subject.getRex( context, type ).right;
        RexNode index = list.getRex( context, type ).right;
        PolyType valueType = value.getType().getPolyType();
        PolyType indexType = index.getType().getPolyType();
        if ( valueType == PolyType.ARRAY ) {
            requireIntegerIndex( index );
        } else if ( valueType == PolyType.MAP || valueType == PolyType.NODE || valueType == PolyType.EDGE ) {
            if ( indexType != PolyType.ANY && indexType != PolyType.NULL && !PolyType.CHAR_TYPES.contains( indexType ) ) {
                throw new CypherSyntaxException( "A map or property key must be a string." );
            }
        } else if ( valueType != PolyType.ANY && valueType != PolyType.NULL ) {
            throw new CypherSyntaxException( "Indexing requires a list, map, node, or relationship." );
        }

        AlgDataType componentType = value.getType().getComponentType();
        AlgDataType resultType = componentType == null ? context.typeFactory.createPolyType( PolyType.ANY ) : componentType;
        return Pair.of( null, context.rexBuilder.makeCall( context.typeFactory.createTypeWithNullability( resultType, true ),
                OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_INDEX ), List.of( value, index ) ) );
    }


    static void requireIntegerIndex( RexNode index ) {
        PolyType type = index.getType().getPolyType();
        if ( type != PolyType.ANY && type != PolyType.NULL && !PolyType.INT_TYPES.contains( type ) ) {
            throw new CypherSyntaxException( "A list index or slice bound must be an integer." );
        }
    }

}
