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

package org.polypheny.db.algebra.polyalg.parser.nodes;

import java.util.List;
import java.util.StringJoiner;
import lombok.NonNull;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.type.PolyType;

public class PolyAlgDataType extends PolyAlgNode {

    private final String type;
    private final List<Integer> args; // precision / scale
    private final boolean nullable;


    public PolyAlgDataType( @NonNull String type, @NonNull List<Integer> args, boolean nullable, ParserPos pos ) {
        super( pos );
        this.type = type;
        this.args = args;
        this.nullable = nullable;
    }


    public AlgDataType toAlgDataType() {
        AlgDataTypeFactory factory = AlgDataTypeFactory.DEFAULT;
        PolyType polyType = getPolyType();
        AlgDataType dataType = switch ( args.size() ) {
            case 0 -> factory.createPolyType( polyType );
            case 1 -> factory.createPolyType( polyType, args.get( 0 ) );
            case 2 -> factory.createPolyType( polyType, args.get( 0 ), args.get( 1 ) );
            default -> throw new GenericRuntimeException( "Unexpected number of type arguments: " + args.size() );
        };
        return factory.createTypeWithNullability( dataType, nullable );
    }


    public PolyType getPolyType() {
        return PolyType.get( type );
    }


    @Override
    public String toString() {
        String str = type;
        if ( !args.isEmpty() ) {
            str += "(";
            StringJoiner joiner = new StringJoiner( ", " );
            for ( Integer arg : args ) {
                joiner.add( arg.toString() );
            }
            str += joiner + ")";
        }
        return str;
    }

}
