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

package org.polypheny.db.cypher;

import lombok.Getter;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;

public class CypherParameter extends CypherExpression {

    @Getter
    private final ParameterType parameterType;
    @Getter
    private final String name;
    private final PolyValue value;


    protected CypherParameter( ParserPos pos, String name, ParameterType parameterType ) {
        this( pos, name, parameterType, null );
    }


    private CypherParameter( ParserPos pos, String name, ParameterType parameterType, PolyValue value ) {
        super( pos );
        this.name = name;
        this.parameterType = parameterType;
        this.value = value;
    }


    public CypherParameter withValue( PolyValue value ) {
        return new CypherParameter( getPos(), name, parameterType, value );
    }


    @Override
    public PolyValue getComparable() {
        if ( value == null ) {
            throw new GenericRuntimeException( "Missing Cypher parameter: $" + name );
        }
        return value;
    }


    @Override
    protected Pair<PolyString, RexNode> toRex( CypherContext context, RexType type ) {
        PolyValue boundValue = getComparable();
        RexNode literal;
        if ( boundValue.isNull() ) {
            literal = context.rexBuilder.makeNullLiteral( context.typeFactory.createPolyType( PolyType.NULL ) );
        } else if ( boundValue.isMap() ) {
            AlgDataType mapType = context.typeFactory.createMapType(
                    context.typeFactory.createPolyType( PolyType.VARCHAR ), context.typeFactory.createPolyType( PolyType.ANY ) );
            literal = context.rexBuilder.makeLiteral( boundValue, mapType );
        } else {
            literal = context.rexBuilder.makeLiteral( boundValue );
        }
        return Pair.of( PolyString.of( "$" + name ), literal );
    }

}
