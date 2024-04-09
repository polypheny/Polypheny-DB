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

import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.ParserPos;

public class PolyAlgLiteral extends PolyAlgNode {

    private final String str;
    private final boolean isNumber;


    public PolyAlgLiteral( String str, boolean isNumber, ParserPos pos ) {
        super( pos );

        this.str = str;
        this.isNumber = isNumber;
    }


    public int toInt() {
        if ( !this.isNumber ) {
            throw new GenericRuntimeException( "Not a valid integer" );
        }
        return Integer.parseInt( str );
    }


    public boolean toBoolean() {
        return Boolean.parseBoolean( str ) || str.equals( "1" );
    }


    @Override
    public String toString() {
        return str;
    }

}
