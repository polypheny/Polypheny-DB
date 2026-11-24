/*
 * Copyright 2019-2026 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.cypher.expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.functions.CypherFunctions;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyDictionary;


/** Keeps literal pattern constraints on the template and leaves computed values for candidate evaluation. */
record CypherPatternProperties( Map<PolyString, PolyValue> literals, @Nullable RexNode computed ) {

    static CypherPatternProperties split( RexNode map ) {
        if ( map instanceof RexLiteral literal && literal.value instanceof PolyDictionary dictionary ) {
            return new CypherPatternProperties( dictionary, null );
        }
        if ( !(map instanceof RexCall call) || call.getOperator().getOperatorName() != OperatorName.CYPHER_MAP_CONSTRUCTOR ) {
            return new CypherPatternProperties( Map.of(), map );
        }

        List<PolyValue> literals = new ArrayList<>();
        List<RexNode> computed = new ArrayList<>();
        for ( int i = 0; i < call.operands.size(); i += 2 ) {
            RexNode key = call.operands.get( i );
            if ( !(key instanceof RexLiteral literalKey) || !(literalKey.value instanceof PolyString) ) {
                return new CypherPatternProperties( Map.of(), map );
            }
            RexNode value = call.operands.get( i + 1 );
            if ( value instanceof RexLiteral literalValue ) {
                literals.add( literalKey.value );
                literals.add( literalValue.value );
            } else {
                computed.add( key );
                computed.add( value );
            }
        }
        if ( literals.isEmpty() ) {
            return new CypherPatternProperties( Map.of(), map );
        }
        return new CypherPatternProperties( CypherFunctions.constructMap( literals.toArray( PolyValue[]::new ) ),
                computed.isEmpty() ? null : call.clone( call.getType(), computed ) );
    }

}
