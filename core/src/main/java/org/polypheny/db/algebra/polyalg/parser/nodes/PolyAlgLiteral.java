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
    private final LiteralType type;


    public PolyAlgLiteral( String str, LiteralType type, ParserPos pos ) {
        super( pos );
        this.str = str;
        this.type = type;
    }


    public void checkType( LiteralType type ) {
        if ( this.type != type ) {
            throw new GenericRuntimeException( "Not a valid " + this.type + ": '" + str + "'" );
        }
    }


    public int toInt() {
        checkType( LiteralType.NUMBER );
        return Integer.parseInt( str );
    }


    public boolean toBoolean() {
        checkType( LiteralType.BOOLEAN );
        return Boolean.parseBoolean( str );
    }


    public Number toNumber() {
        checkType( LiteralType.NUMBER );

        Number num;
        double dbl = Double.parseDouble( str );
        num = dbl;
        if ( dbl % 1 == 0 ) {
            num = Integer.parseInt( str );
        }
        return num;
    }


    public AlgFieldCollation.Direction toDirection() {
        checkType( LiteralType.DIRECTION );
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
        checkType( LiteralType.NULL_DIRECTION );
        return NullDirection.valueOf( str.toUpperCase( Locale.ROOT ) );
    }


    @Override
    public String toString() {
        return str;
    }


    public String toUnquotedString() {
        if ( type == LiteralType.QUOTED ) {
            return str.substring( 1, str.length() - 1 );
        }
        return str;
    }


    public int toDynamicParam() {
        checkType( LiteralType.DYNAMIC_PARAM );
        return Integer.parseInt( str.substring( 1 ) ); // str looks like "?0"
    }


    public enum LiteralType {

        QUOTED,
        NUMBER,
        BOOLEAN,
        NULL,
        DIRECTION, // AlgFieldCollation.Direction
        NULL_DIRECTION, // AlgFieldCollation.NullDirection
        DYNAMIC_PARAM,
        STRING; // This is the least specific type and is used e.g. for field or entity names

        public static LiteralType DEFAULT = STRING;

    }

}
