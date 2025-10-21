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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;

/**
 * Parses and validates schema-related options for CREATE and ALTER document statements.
 * Consumes a JSON-like options payload and produces a normalized {@link Resolved}.
 */
public final class SchemaOptionsResolver {

    private SchemaOptionsResolver() {
    }


    /**
     * Shared JSON mapper used for parsing payloads.
     */
    private static final ObjectMapper M = new ObjectMapper();


    /**
     * Alteration strategy for applying schema changes.
     * REPLACE - replace the entire schema.
     * PATCH - merge the into existing schema.
     */
    public enum AlterMode {REPLACE, PATCH}


    public static final class Rename {

        public final String from, to;

        /**
         * Rename directive mapping one path to another.
         * Contains the source path {@code from} and the destination path {@code to}.
         */
        public Rename( String f, String t ) {
            from = f;
            to = t;
        }


        @Override
        public String toString() {
            return from + "→" + to;
        }

    }


    public static final class Coercion {

        public final String target, onFailure;


        public Coercion( String t, String o ) {
            target = t;
            onFailure = o;
        }

    }

    /**
     * Final, normalized set of options.
     * Contains the parsed schema
     */
    public static final class Resolved {

        public final DocumentSchema schema;
        public final EnforcementMode mode;
        public final AlterMode alterMode;
        public final List<Rename> renames;
        public final Map<String, JsonNode> defaults;
        public final Map<String, Coercion> coercions;
        public final boolean pruneExtras, dryRun;

        /**
         * Final, normalized set of options derived from the input payload.
         * Contains the parsed schema (if provided), enforcement mode, alter mode, rename directives,
         * default value specifications, coercion rules, and execution flags.
         *
         * Fields:
         *  - schema: parsed {@link DocumentSchema} to apply. May be {@code null} for ALTER when no schema section is supplied.
         *  - mode: enforcement setting (OFF, WARN, STRICT) that governs how validation violations are handled.
         *  - alterMode: application strategy; REPLACE overwrites the full schema, PATCH merges into the existing schema.
         *  - renames: list of path-level rename operations to move or rename fields during migration.
         *  - defaults: map from JSON path to a default {@link JsonNode} value; when a property is missing, this value is written before validation.
         *  - coercions: map from JSON path to a {@link Coercion} rule; each rule specifies a target type token and an on-failure policy (e.g., "error", "skip")
         *               and is applied before validation to convert incompatible values.
         *  - pruneExtras: when {@code true}, undeclared properties are removed during processing (useful when additionalProperties is FORBID);
         *                 when {@code false}, extra fields are left as-is and may trigger violations depending on the enforcement mode.
         *  - dryRun: when {@code true}, performs planning/validation only and does not persist catalog changes or modify stored documents.
         */
        public Resolved(
                DocumentSchema s, EnforcementMode m, AlterMode a, List<Rename> r,
                Map<String, JsonNode> d, Map<String, Coercion> c, boolean p, boolean dr ) {
            schema = s;
            mode = m;
            alterMode = a;
            renames = r;
            defaults = d;
            coercions = c;
            pruneExtras = p;
            dryRun = dr;
        }

    }

    /**
     * Resolves options for a CREATE statement.
     * Requires a {@code docSchema} definition and returns a normalized {@link Resolved}.
     *
     * @param options JSON-like options
     * @return resolved options with a non-null schema
     * @throws IllegalArgumentException if {@code options} is missing or {@code docSchema} is absent or invalid
     */
    public static Resolved resolve( PolyValue options ) {
        var r = parseCommon( options, false );
        if ( r.schema == null ) {
            throw new IllegalArgumentException( "CREATE requires 'docSchema' object." );
        }
        return r;
    }

    /**
     * Resolves options for an ALTER statement.
     * The {@code docSchema} section is optional.
     *
     * @param options JSON-like options
     * @return resolved options; {@code schema} may be null when no schema change is supplied
     * @throws IllegalArgumentException if the options payload is malformed
     */
    public static Resolved resolveAlter( PolyValue options ) {
        return parseCommon( options, true );
    }

    /**
     * Ensures that a given JSON node represents an object.
     *
     * @param first initial node to inspect
     * @return the object node view of the input
     * @throws IllegalArgumentException if the input cannot be resolved to an object
     */
    private static ObjectNode requireObjectNode( JsonNode first ) {
        JsonNode n = first;
        // unwrap up to 3 times if it's a JSON string containing JSON
        for ( int i = 0; i < 3 && n != null; i++ ) {
            if ( n instanceof ObjectNode obj ) {
                return obj;
            }
            if ( n.isTextual() ) {
                try {
                    n = M.readTree( n.asText() );
                    continue;
                } catch ( Exception ignored ) {
                    break;
                }
            }
            break;
        }
        throw new IllegalArgumentException( "Options must be a JSON object." );
    }

    /**
     * Parses the common options structure used by CREATE and ALTER statements.
     * Handles enforcement mode, alter mode, rename directives, defaults, coercions, pruning, dry-run,
     * and optionally a {@code docSchema} section.
     *
     * @param options JSON-like options wrapped in {@link PolyValue}
     * @param schemaOptional whether the {@code docSchema} section is optional
     * @return normalized {@link Resolved} options
     * @throws IllegalArgumentException if the payload is invalid or required fields are missing
     */
    private static Resolved parseCommon( PolyValue options, boolean schemaOptional ) {
        final ObjectNode root;
        if ( options == null ) {
            if ( schemaOptional ) {
                return new Resolved( null, null, AlterMode.REPLACE, List.of(), Map.of(), Map.of(), false, false );
            }
            throw new IllegalArgumentException( "Missing options." );
        }
        try {
            JsonNode raw = M.readTree( options.toJson() );
            root = requireObjectNode( raw );
        } catch ( Exception e ) {
            throw new IllegalArgumentException( "Invalid options payload: " + e.getMessage(), e );
        }

        if ( root.has( "validator" ) || root.has( "$jsonSchema" ) ) {
            throw new IllegalArgumentException( "Use 'docSchema' instead of 'validator.$jsonSchema'." );
        }

        EnforcementMode mode = null;
        if ( root.has( "validationAction" ) ) {
            String s = root.get( "validationAction" ).asText( "" );
            mode = switch ( s.toLowerCase( Locale.ROOT ) ) {
                case "error", "strict" -> EnforcementMode.STRICT;
                case "warn" -> EnforcementMode.WARN;
                case "off" -> EnforcementMode.OFF;
                default -> throw new IllegalArgumentException( "Unknown validationAction: " + s );
            };
        }

        AlterMode alterMode = AlterMode.REPLACE;
        if ( root.has( "mode" ) ) {
            alterMode = "patch".equalsIgnoreCase( root.get( "mode" ).asText( "" ) ) ? AlterMode.PATCH : AlterMode.REPLACE;
        }

        List<Rename> renames = new ArrayList<>();
        if ( root.has( "renames" ) && root.get( "renames" ).isArray() ) {
            for ( JsonNode r : root.get( "renames" ) ) {
                if ( r.has( "from" ) && r.has( "to" ) ) {
                    renames.add( new Rename( r.get( "from" ).asText(), r.get( "to" ).asText() ) );
                }
            }
        }

        Map<String, JsonNode> defaults = new HashMap<>();
        if ( root.has( "defaults" ) && root.get( "defaults" ).isObject() ) {
            root.get( "defaults" ).fields().forEachRemaining( e -> defaults.put( e.getKey(), e.getValue() ) );
        }

        Map<String, Coercion> coercions = new HashMap<>();
        if ( root.has( "coercions" ) && root.get( "coercions" ).isObject() ) {
            root.get( "coercions" ).fields().forEachRemaining( e -> {
                String path = e.getKey();
                JsonNode spec = e.getValue();
                coercions.put( path, new Coercion(
                        spec.has( "target" ) ? spec.get( "target" ).asText() : "text",
                        spec.has( "onFailure" ) ? spec.get( "onFailure" ).asText( "error" ) : "error" ) );
            } );
        }

        boolean pruneExtras = root.has( "pruneExtras" ) && root.get( "pruneExtras" ).asBoolean( false );
        boolean dryRun = root.has( "dryRun" ) && root.get( "dryRun" ).asBoolean( false );

        DocumentSchema schema = null;
        if ( root.has( "docSchema" ) ) {
            JsonNode ds = root.get( "docSchema" );
            if ( !ds.isObject() ) {
                throw new IllegalArgumentException( "'docSchema' must be an object" );
            }

            // Root-aware parse: nested objects may NOT define additionalProperties.
            DocumentSchema.ObjectNode rootNode = readObjectNode( (ObjectNode) ds, /*isRoot=*/true );

            // Root-level additionalProperties is REQUIRED (strict mode).
            DocumentSchema.AdditionalProperties ap = readRootAPOrThrow( (ObjectNode) ds );

            schema = new DocumentSchema( rootNode, ap );
        } else if ( !schemaOptional ) {
            throw new IllegalArgumentException( "Missing 'docSchema'." );
        }

        return new Resolved( schema, mode, alterMode, renames, defaults, coercions, pruneExtras, dryRun );
    }

    // ---------- Recursive readers ----------

    /**
     * Reads an object-node schema from a JSON.
     * Validates that the node is an object and
     * recursively parses {@code properties}.
     *
     * @param objSpec object specification
     * @param isRoot whether this node is the schema root
     * @return parsed {@link DocumentSchema.ObjectNode}
     * @throws IllegalArgumentException if the specification is invalid
     */
    private static DocumentSchema.ObjectNode readObjectNode( ObjectNode objSpec, boolean isRoot ) {
        if ( objSpec.has( "type" ) ) {
            String t = objSpec.get( "type" ).asText( "" ).trim().toLowerCase( Locale.ROOT );
            if ( !t.isEmpty() && !t.equals( "object" ) ) {
                throw new IllegalArgumentException( "Object node expected, found type: " + t );
            }
        }
        if ( objSpec.has( "required" ) ) {
            throw new IllegalArgumentException( "This dialect does not support 'required'. All declared properties are required." );
        }

        // Reject nested additionalProperties outright
        if ( !isRoot && objSpec.has( "additionalProperties" ) ) {
            throw new IllegalArgumentException( "Nested 'additionalProperties' is not allowed; only the top-level may define it." );
        }

        Map<String, DocumentSchema.Node> props = new LinkedHashMap<>();
        if ( objSpec.has( "properties" ) ) {
            JsonNode propsNode = objSpec.get( "properties" );
            if ( !propsNode.isObject() ) {
                throw new IllegalArgumentException( "'properties' must be an object" );
            }
            propsNode.fields().forEachRemaining( e -> props.put( e.getKey(), readNode( e.getValue(), /*isRoot=*/false ) ) );
        }
        return new DocumentSchema.ObjectNode( props );
    }

    /**
     * Reads an arbitrary schema node from a JSON specification.
     * Accepts string shorthand for scalars or an object with a {@code type} field.
     * Dispatches to {@code readObjectNode} or {@code readArrayNode} as appropriate.
     *
     * @param spec node specification (string or object)
     * @param isRoot whether this node is the schema root
     * @return parsed {@link DocumentSchema.Node}
     * @throws IllegalArgumentException if the specification is invalid or unsupported
     */
    private static DocumentSchema.Node readNode( JsonNode spec, boolean isRoot ) {
        if ( spec.isTextual() ) {
            PolyType pt = mapInputTypeToPoly( spec.asText() );
            return new DocumentSchema.ScalarNode( pt );
        }
        if ( !spec.isObject() ) {
            throw new IllegalArgumentException( "Property spec must be string or object" );
        }

        ObjectNode o = (ObjectNode) spec;
        if ( o.has( "type" ) && o.get( "type" ).isTextual() ) {
            String typeText = o.get( "type" ).asText().trim().toLowerCase( Locale.ROOT );
            if ( typeText.equals( "object" ) ) {
                return readObjectNode( o, /*isRoot=*/false );
            }
            if ( typeText.equals( "array" ) ) {
                return readArrayNode( o );
            }
            PolyType pt = mapInputTypeToPoly( typeText );
            return new DocumentSchema.ScalarNode( pt );
        }

        if ( o.has( "properties" ) ) {
            return readObjectNode( o, /*isRoot=*/false );
        }
        throw new IllegalArgumentException( "Missing or unsupported 'type' in property spec: " + o );
    }

    /**
     * Reads an array-node schema from a JSON.
     * Requires an {@code items} definition and supports optional constraints.
     *
     * @param arrSpec array specification
     * @return parsed {@link DocumentSchema.ArrayNode}
     * @throws IllegalArgumentException if the specification is invalid
     */
    private static DocumentSchema.ArrayNode readArrayNode( ObjectNode arrSpec ) {
        if ( !arrSpec.has( "items" ) ) {
            throw new IllegalArgumentException( "Array spec requires 'items'" );
        }
        DocumentSchema.Node items = readNode( arrSpec.get( "items" ), /*isRoot=*/false );
        Integer minItems = arrSpec.has( "minItems" ) ? arrSpec.get( "minItems" ).asInt() : null;
        Boolean unique = arrSpec.has( "uniqueItems" ) ? arrSpec.get( "uniqueItems" ).asBoolean() : null;
        return new DocumentSchema.ArrayNode( items, minItems, unique );
    }


    /**
     * Reads the root-level {@code additionalProperties} flag.
     *
     * @param o root object specification
     * @return resolved {@link DocumentSchema.AdditionalProperties} value
     * @throws IllegalArgumentException if the field is missing or invalid
     */
    private static DocumentSchema.AdditionalProperties readRootAPOrThrow( ObjectNode o ) {
        if ( !o.has( "additionalProperties" ) ) {
            throw new IllegalArgumentException( "Top-level 'additionalProperties' must be specified (true/false or ALLOW/FORBID)." );
        }
        JsonNode n = o.get( "additionalProperties" );
        if ( n.isBoolean() ) {
            return n.asBoolean() ? DocumentSchema.AdditionalProperties.ALLOW : DocumentSchema.AdditionalProperties.FORBID;
        }
        if ( n.isTextual() ) {
            String s = n.asText();
            if ( "FORBID".equalsIgnoreCase( s ) || "false".equalsIgnoreCase( s ) ) {
                return DocumentSchema.AdditionalProperties.FORBID;
            }
            if ( "ALLOW".equalsIgnoreCase( s ) || "true".equalsIgnoreCase( s ) ) {
                return DocumentSchema.AdditionalProperties.ALLOW;
            }
        }
        throw new IllegalArgumentException( "'additionalProperties' must be boolean or 'FORBID'/'ALLOW'" );
    }

    // ---------- Mapping from friendly tokens (and legacy) to PolyType ----------

    /**
     * Maps a type token to a{@link PolyType}.
     *
     * @param raw input token; may include width or precision suffixes
     * @return resolved {@link PolyType}
     * @throws IllegalArgumentException if the token is not recognized
     */
    private static PolyType mapInputTypeToPoly( String raw ) {
        if ( raw == null ) {
            return PolyType.ANY;
        }
        String s = raw.trim();
        // strip any legacy (p,s) suffix: VARCHAR(50), DECIMAL(10,2), etc.
        int paren = s.indexOf( '(' );
        if ( paren >= 0 ) {
            s = s.substring( 0, paren );
        }
        String t = s.toLowerCase( Locale.ROOT );

        // Friendly dialect
        switch ( t ) {
            case "text":
            case "string":
                return PolyType.TEXT;

            case "number":
            case "numeric":
                return PolyType.DOUBLE;

            case "boolean":
            case "bool":
                return PolyType.BOOLEAN;

            case "date":
                return PolyType.DATE;

            case "timestamp":
            case "datetime":
                return PolyType.TIMESTAMP;

            case "binary":
            case "blob":
                return PolyType.BINARY;

            case "any":
                return PolyType.ANY;
        }

        // Legacy SQL-ish tokens we still accept
        switch ( t ) {
            // strings
            case "char":
            case "varchar":
            case "json":
                return PolyType.TEXT;

            // integers
            case "tinyint":
            case "smallint":
            case "int":
            case "integer":
            case "bigint":
                return PolyType.INTEGER;

            // floating / decimal
            case "decimal":
            case "float":
            case "real":
            case "double":
                return PolyType.DOUBLE;

            // binary-ish
            case "varbinary":
            case "file":
            case "image":
            case "video":
            case "audio":
                return PolyType.BINARY;

            // temporal
            case "time":
                return PolyType.TIMESTAMP;
        }

        throw new IllegalArgumentException( "Unknown type token: " + raw );
    }

}
