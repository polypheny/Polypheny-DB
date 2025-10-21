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

import java.util.*;
import java.util.stream.Collectors;
import org.bson.*;
import org.polypheny.db.schema.document.DocumentSchema.ArrayNode;
import org.polypheny.db.schema.document.DocumentSchema.Node;
import org.polypheny.db.schema.document.DocumentSchema.ObjectNode;
import org.polypheny.db.schema.document.DocumentSchema.ScalarNode;
import org.polypheny.db.type.PolyType;

public final class SchemaValidator {

    private SchemaValidator() {
    }


    public record Violation( String path, String code, String message ) {

    }


    public record ValidationResult( boolean ok, List<Violation> violations ) {

        public String compactSummary( int maxItems ) {
            if ( ok || violations.isEmpty() ) {
                return "ok";
            }
            return violations.stream()
                    .limit( Math.max( 1, maxItems ) )
                    .map( v -> v.code + "@" + v.path + "(" + v.message + ")" )
                    .collect( Collectors.joining( "; " ) )
                    + (violations.size() > maxItems ? " … +" + (violations.size() - maxItems) + " more" : "");
        }

    }


    public static ValidationResult validate( DocumentSchema schema, BsonDocument doc ) {
        List<Violation> out = new ArrayList<>();
        validateObject( "$", schema.root(), doc, out, schema.additionalProperties() );
        return new ValidationResult( out.isEmpty(), out );
    }


    public static boolean conformsTo( DocumentSchema schema, BsonDocument doc ) {
        return validate( schema, doc ).ok();
    }


    private static void validateObject(
            String path,
            DocumentSchema.ObjectNode schemaNode,
            BsonDocument doc,
            List<Violation> out,
            DocumentSchema.AdditionalProperties rootAp ) {

        if ( doc == null ) {
            out.add( v( path, "TYPE", "Expected object but was null" ) );
            return;
        }

        // Every declared property is required by dialect
        for ( Map.Entry<String, DocumentSchema.Node> e : schemaNode.properties.entrySet() ) {
            String key = e.getKey();
            DocumentSchema.Node child = e.getValue();
            String p = pathDot( path, key );

            if ( !doc.containsKey( key ) || doc.get( key ).isNull() ) {
                out.add( v( p, "REQUIRED_MISSING", "Required field is missing" ) );
                continue;
            }

            BsonValue bv = doc.get( key );

            if ( child instanceof DocumentSchema.ObjectNode on ) {
                if ( !(bv instanceof BsonDocument) ) {
                    out.add( v( p, "TYPE", "Expected object, got " + bsonTypeName( bv ) ) );
                } else {
                    validateObject( p, on, (BsonDocument) bv, out, rootAp ); // propagate root AP
                }
            } else if ( child instanceof DocumentSchema.ArrayNode an ) {
                if ( !(bv instanceof BsonArray) ) {
                    out.add( v( p, "TYPE", "Expected array, got " + bsonTypeName( bv ) ) );
                } else {
                    validateArray( p, an, (BsonArray) bv, out, rootAp ); // propagate root AP
                }
            } else if ( child instanceof DocumentSchema.ScalarNode sn ) {
                PolyType t = sn.type;
                if ( !matchesType( bv, t ) ) {
                    out.add( v( p, "TYPE_MISMATCH", "Expected " + t + " but got " + bsonTypeName( bv ) ) );
                }
            } else {
                out.add( v( p, "INTERNAL", "Unknown schema node" ) );
            }
        }

        // Enforce root AP at every level
        if ( rootAp == DocumentSchema.AdditionalProperties.FORBID ) {
            for ( String k : doc.keySet() ) {
                if ( !schemaNode.properties.containsKey( k ) ) {
                    out.add( v( pathDot( path, k ), "ADDITIONAL_PROPERTY", "Unexpected field" ) );
                }
            }
        }
    }


    private static void validateArray(
            String path,
            ArrayNode schema,
            BsonArray arr,
            List<Violation> out,
            DocumentSchema.AdditionalProperties rootAp ) {

        if ( schema.minItems != null && arr.size() < schema.minItems ) {
            out.add( v( path, "MIN_ITEMS", "Expected at least " + schema.minItems + " items" ) );
        }
        if ( Boolean.TRUE.equals( schema.uniqueItems ) ) {
            Set<String> uniq = new HashSet<>();
            for ( int i = 0; i < arr.size(); i++ ) {
                String key = arr.get( i ).toString();
                if ( !uniq.add( key ) ) {
                    out.add( v( pathDot( path, Integer.toString( i ) ), "UNIQUE", "Duplicate array item" ) );
                }
            }
        }

        Node item = schema.items;
        for ( int i = 0; i < arr.size(); i++ ) {
            BsonValue v = arr.get( i );
            String ip = pathDot( path, Integer.toString( i ) );

            if ( item instanceof ObjectNode on ) {
                if ( !(v instanceof BsonDocument) ) {
                    out.add( v( ip, "TYPE", "Expected object, got " + bsonTypeName( v ) ) );
                } else {
                    validateObject( ip, on, (BsonDocument) v, out, rootAp ); // propagate root AP
                }
            } else if ( item instanceof ArrayNode an ) {
                if ( !(v instanceof BsonArray) ) {
                    out.add( v( ip, "TYPE", "Expected array, got " + bsonTypeName( v ) ) );
                } else {
                    validateArray( ip, an, (BsonArray) v, out, rootAp ); // propagate root AP
                }
            } else if ( item instanceof ScalarNode sn ) {
                PolyType t = sn.type;
                if ( !matchesType( v, t ) ) {
                    out.add( v( ip, "TYPE_MISMATCH", "Expected " + t + " but got " + bsonTypeName( v ) ) );
                }
            } else {
                out.add( v( ip, "INTERNAL", "Unknown schema node" ) );
            }
        }
    }

    // ----------------------------------------------------------------------


    private static Violation v( String path, String code, String msg ) {
        return new Violation( path, code, msg );
    }


    private static String pathDot( String base, String next ) {
        return base.equals( "$" ) ? "$." + next : base + "." + next;
    }


    private static String bsonTypeName( BsonValue v ) {
        return v == null ? "NULL" : v.getBsonType().name();
    }


    private static boolean matchesType( BsonValue v, PolyType t ) {
        if ( t == PolyType.ANY ) {
            return true;
        }
        if ( t == PolyType.NULL ) {
            return v == null || v.isNull();
        }

        switch ( t ) {
            case BOOLEAN:
                return v instanceof BsonBoolean;

            // Strings
            case CHAR:
            case VARCHAR:
            case TEXT:
            case JSON:
                return v instanceof BsonString;

            // Numerics
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return (v instanceof BsonInt32) || (v instanceof BsonInt64);
            case BIGINT:
                return (v instanceof BsonInt64) || (v instanceof BsonInt32);
            case DECIMAL:
            case FLOAT:
            case REAL:
            case DOUBLE:
                return (v instanceof BsonNumber)
                        || (v instanceof BsonDouble)
                        || (v instanceof BsonInt32)
                        || (v instanceof BsonInt64);

            // Collections / documents
            case ARRAY:
                return v instanceof BsonArray;
            case DOCUMENT:
            case MAP:
                return v instanceof BsonDocument;

            // Temporal
            case DATE:
                return v instanceof BsonDateTime;
            case TIMESTAMP:
                return v instanceof BsonTimestamp;

            // Binary / blobs
            case BINARY:
            case VARBINARY:
            case FILE:
            case IMAGE:
            case VIDEO:
            case AUDIO:
                return v instanceof BsonBinary;

            default:
                return false;
        }
    }

}
