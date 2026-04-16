/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.schema.document;

import java.util.Locale;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonNumber;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.polypheny.db.type.PolyType;

/**
 * Maps JSON schema type tokens to PolyType and BSON values.
 */
public final class JsonTypeTokens {

    private JsonTypeTokens() {
    }


    /**
     * Accepts canonical JSON tokens and a small set of legacy persisted aliases.
     */
    private static String normalizeToJsonToken( String rawToken ) {
        if ( rawToken == null ) {
            return null;
        }

        String normalized = rawToken.trim();
        int parenthesisIndex = normalized.indexOf( '(' );
        if ( parenthesisIndex >= 0 ) {
            normalized = normalized.substring( 0, parenthesisIndex );
        }

        String token = normalized.toLowerCase( Locale.ROOT );

        return switch ( token ) {
            case "string", "text", "number", "boolean", "bool", "null", "object", "array" -> token;
            case "double" -> "number";
            case "varchar", "char", "json" -> "string";
            default -> token;
        };
    }


    public static PolyType toPolyType( String rawToken ) {
        if ( rawToken == null ) {
            throw new IllegalArgumentException( "Type token must be provided" );
        }

        String token = normalizeToJsonToken( rawToken );

        return switch ( token ) {
            case "string", "text" -> PolyType.TEXT;
            case "number" -> PolyType.DOUBLE;
            case "boolean", "bool" -> PolyType.BOOLEAN;
            case "null" -> PolyType.NULL;
            case "object", "array" -> throw new IllegalArgumentException( "Structural types require nested specification: object/array with properties/items" );
            default -> throw new IllegalArgumentException( "Unsupported type '" + rawToken + "'. Allowed: string (text), number, boolean, null, object, array." );
        };
    }


    public static String toJsonToken( PolyType polyType ) {
        return switch ( polyType ) {
            case TEXT, CHAR, VARCHAR -> "string";
            case DOUBLE -> "number";
            case BOOLEAN -> "boolean";
            case NULL -> "null";
            default -> throw new IllegalArgumentException( "Cannot serialize non-JSON PolyType: " + polyType );
        };
    }


    public static boolean isBsonNumeric( BsonValue value ) {
        return value instanceof BsonNumber || value != null && value.getBsonType() == BsonType.DECIMAL128;
    }


    public static boolean matchesJson( BsonValue value, PolyType polyType ) {
        if ( polyType == PolyType.NULL ) {
            return value == null || value.isNull();
        }

        return switch ( polyType ) {
            case BOOLEAN -> value instanceof BsonBoolean;
            case CHAR, VARCHAR, TEXT -> value instanceof BsonString;
            case DOUBLE -> isBsonNumeric( value );
            case ARRAY -> value instanceof BsonArray;
            case MAP -> value instanceof BsonDocument;
            default -> false;
        };
    }


    public static boolean isJsonNumberPolyType( PolyType polyType ) {
        return polyType == PolyType.DOUBLE;
    }

}