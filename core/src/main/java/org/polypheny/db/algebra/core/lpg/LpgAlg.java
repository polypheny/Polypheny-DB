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

package org.polypheny.db.algebra.core.lpg;

import com.google.common.hash.Hashing;
import java.util.List;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;

/**
 * {@link ModelTrait#GRAPH} native node.
 */
public interface LpgAlg {

    NodeType getNodeType();


    /** Graph parameters can remain embedded in the plan, so their values are part of its cache identity. */
    static String comparisonKey( Object... arguments ) {
        StringBuilder key = new StringBuilder();
        for ( Object argument : arguments ) {
            String content = argument == null ? null : comparisonComponent( argument );
            if ( content == null ) {
                key.append( "-1:" );
            } else {
                key.append( content.length() ).append( ':' ).append( content );
            }
        }
        // Keep keys small and avoid exposing bound values in plan monitoring.
        return Hashing.sha256().hashUnencodedChars( key ).toString();
    }


    private static String comparisonComponent( Object argument ) {
        if ( argument instanceof PolyValue value ) {
            return comparisonValue( value );
        }
        if ( argument instanceof List<?> list ) {
            return comparisonKey( list.toArray() );
        }
        if ( argument instanceof RexLiteral literal ) {
            return comparisonKey( literal.getType().getFullTypeString(), literal.getValue() );
        }
        if ( argument instanceof RexCall call ) {
            return comparisonKey( call.getOperator().getName(), call.getOperator().getOperatorName(), call.getType().getFullTypeString(), call.getOperands() );
        }
        if ( argument instanceof RexNode expression ) {
            return comparisonKey( expression.getClass().getName(), expression.getType().getFullTypeString(), expression.toString() );
        }
        return argument.toString();
    }


    private static String comparisonValue( PolyValue value ) {
        if ( value.isNull() ) {
            return comparisonKey( value.getType(), null );
        }
        if ( value instanceof PolyList<?> list ) {
            return comparisonKey( value.getType(), comparisonKey( list.toArray() ) );
        }
        if ( value instanceof PolyBinary binary ) {
            return comparisonKey( value.getType(), binary.count, binary.as64String() );
        }
        if ( value.isTemporal() ) {
            return comparisonKey( value.getType(), value.asTemporal().getMillisSinceEpoch() );
        }
        if ( value instanceof PolyDouble number ) {
            return comparisonKey( value.getType(), Double.doubleToRawLongBits( number.value ) );
        }
        if ( value instanceof PolyFloat number ) {
            return comparisonKey( value.getType(), Float.floatToRawIntBits( number.value ) );
        }
        return comparisonKey( value.getType(), value.toString() );
    }


    enum NodeType {
        MATCH,
        JOIN,
        FILTER,
        SCAN,
        UNWIND,
        PROJECT,
        MODIFY,
        VALUES,
        AGGREGATE,
        MERGE,
        SORT
    }

}
