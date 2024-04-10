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

import java.util.Locale;
import lombok.Getter;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgFieldCollation.Direction;
import org.polypheny.db.algebra.AlgFieldCollation.NullDirection;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.ParserPos;

public class PolyAlgLiteral extends PolyAlgNode {

    private final String str;
    @Getter
    private final boolean isNumber;
    @Getter
    private final boolean isQuoted;
    @Getter
    private final boolean isBoolean;


    public PolyAlgLiteral( String str, boolean isNumber, boolean isQuoted, boolean isBoolean, ParserPos pos ) {
        super( pos );

        this.str = str;
        this.isNumber = isNumber;
        this.isQuoted = isQuoted;
        this.isBoolean = isBoolean;
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


    public Number toNumber() {
        if ( !this.isNumber ) {
            throw new GenericRuntimeException( "Not a valid number" );
        }

        Number num;
        double dbl = Double.parseDouble( str );
        num = dbl;
        if ( dbl % 1 == 0 ) {
            num = Integer.parseInt( str );
        }
        return num;
    }


    public AlgFieldCollation.Direction toDirection() {
        return switch ( str.toUpperCase( Locale.ROOT ) ) {
            case "ASC" -> Direction.ASCENDING;
            case "DESC" -> Direction.DESCENDING;
            case "SASC" -> Direction.STRICTLY_ASCENDING;
            case "SDESC" -> Direction.STRICTLY_DESCENDING;
            case "CLU" -> Direction.CLUSTERED;
            default -> throw new IllegalArgumentException( "'" + str + "' is not a valid direction" );
        };
    }


    public AlgFieldCollation.NullDirection toNullDirection() {
        return NullDirection.valueOf( str.toUpperCase( Locale.ROOT ) );
    }


    @Override
    public String toString() {
        return str;
    }


    public String toUnquotedString() {
        if ( isQuoted ) {
            return str.substring( 1, str.length() - 1 );
        }
        return str;
    }

}
