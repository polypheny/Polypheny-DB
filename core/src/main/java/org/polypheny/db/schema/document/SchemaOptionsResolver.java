/*
 * Copyright 2019-2026 The Polypheny Project
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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;

/**
 * Parses and validates schema-related options for CREATE and ALTER document statements.
 * Consumes a JSON-like options payload and produces a normalized {@link Resolved}.
 *
 * <p>This resolver supports a JSON-Schema-inspired dialect that includes:</p>
 * <ul>
 *   <li>required (per object)</li>
 *   <li>per-subdocument additionalProperties (INHERIT/ALLOW/FORBID)</li>
 *   <li>scalar union types: type: ["text","null"]</li>
 *   <li>composition: anyOf/oneOf/allOf/not</li>
 *   <li>extra constraints: enum/const/multipleOf, maxItems, min/maxProperties</li>
 * </ul>
 */
public final class SchemaOptionsResolver {

    private SchemaOptionsResolver() {
    }

    private static final ObjectMapper M = new ObjectMapper();

    public enum AlterMode {REPLACE, PATCH}

    public static final class Rename {
        public final String from, to;

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

    public static final class Resolved {

        public final DocumentSchema schema;
        public final EnforcementMode mode;
        public final AlterMode alterMode;
        public final List<Rename> renames;
        public final Map<String, JsonNode> defaults;
        public final Map<String, Coercion> coercions;
        public final boolean pruneExtras, dryRun;

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

    public static Resolved resolve( PolyValue options ) {
        var r = parseCommon( options, false );
        if ( r.schema == null ) {
            throw new IllegalArgumentException( "CREATE requires 'docSchema' object." );
        }
        return r;
    }

    public static Resolved resolveAlter( PolyValue options ) {
        return parseCommon( options, true );
    }

    private static ObjectNode requireObjectNode( JsonNode first ) {
        JsonNode n = first;
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

            // Root-aware parse: root additionalProperties is stored in wrapper, not on the node.
            DocumentSchema.ObjectNode rootNode = readObjectNode( (ObjectNode) ds, /*isRoot=*/true );

            // Root-level additionalProperties is REQUIRED for CREATE/REPLACE.
            // For PATCH schema fragments, it may be omitted (inherit during merge).
            DocumentSchema.AdditionalProperties ap;
            ObjectNode dso = (ObjectNode) ds;
            if ( schemaOptional && alterMode == AlterMode.PATCH && !dso.has( "additionalProperties" ) ) {
                ap = null; // inherit during merge
            } else {
                ap = readRootAPOrThrow( dso );
            }

            schema = new DocumentSchema( rootNode, ap );
        } else if ( !schemaOptional ) {
            throw new IllegalArgumentException( "Missing 'docSchema'." );
        }

        return new Resolved( schema, mode, alterMode, renames, defaults, coercions, pruneExtras, dryRun );
    }

    // ---------- Recursive readers ----------

    private static DocumentSchema.ObjectNode readObjectNode( ObjectNode objSpec, boolean isRoot ) {

        // type can be omitted or must be "object"
        if ( objSpec.has( "type" ) && objSpec.get("type").isTextual() ) {
            String t = objSpec.get( "type" ).asText( "" ).trim().toLowerCase( Locale.ROOT );
            if ( !t.isEmpty() && !t.equals( "object" ) ) {
                throw new IllegalArgumentException( "Object node expected, found type: " + t );
            }
        }

        // required
        Set<String> required = null;
        if ( objSpec.has("required") ) {
            JsonNode r = objSpec.get("required");
            if ( !r.isArray() ) {
                throw new IllegalArgumentException("'required' must be an array of strings.");
            }
            required = new LinkedHashSet<>();
            for ( JsonNode el : r ) {
                if ( !el.isTextual() ) {
                    throw new IllegalArgumentException("'required' must contain only strings.");
                }
                required.add(el.asText());
            }
        }

        // nested additionalProperties (root handled separately via wrapper ap)
        DocumentSchema.AdditionalProperties nodeAp = DocumentSchema.AdditionalProperties.INHERIT;
        if ( !isRoot && objSpec.has("additionalProperties") ) {
            nodeAp = readNodeAPOrThrow(objSpec.get("additionalProperties"));
        }

        Integer minProps = objSpec.has("minProperties") ? objSpec.get("minProperties").asInt() : null;
        Integer maxProps = objSpec.has("maxProperties") ? objSpec.get("maxProperties").asInt() : null;

        Map<String, DocumentSchema.Node> props = new LinkedHashMap<>();
        if ( objSpec.has( "properties" ) ) {
            JsonNode propsNode = objSpec.get( "properties" );
            if ( !propsNode.isObject() ) {
                throw new IllegalArgumentException( "'properties' must be an object" );
            }
            propsNode.fields().forEachRemaining( e -> props.put( e.getKey(), readNode( e.getValue(), /*isRoot=*/false ) ) );
        }

        return new DocumentSchema.ObjectNode( props, required, nodeAp, minProps, maxProps );
    }

    private static DocumentSchema.Node readNode( JsonNode spec, boolean isRoot ) {

        // string shorthand for scalars
        if ( spec.isTextual() ) {
            PolyType pt = JsonTypeTokens.toPolyType( spec.asText() );
            return new DocumentSchema.ScalarNode(
                    List.of(pt),
                    null, null, null,
                    null, null, null,
                    null, null
            );
        }

        if ( !spec.isObject() ) {
            throw new IllegalArgumentException( "Property spec must be string or object" );
        }

        ObjectNode o = (ObjectNode) spec;

        // composition
        if ( o.has("anyOf") ) {
            JsonNode arr = o.get("anyOf");
            if ( !arr.isArray() ) {
                throw new IllegalArgumentException("'anyOf' must be an array.");
            }
            List<DocumentSchema.Node> opts = new ArrayList<>();
            for ( JsonNode el : arr ) {
                opts.add(readNode(el, false));
            }
            return new DocumentSchema.AnyOfNode(opts);
        }
        if ( o.has("oneOf") ) {
            JsonNode arr = o.get("oneOf");
            if ( !arr.isArray() ) {
                throw new IllegalArgumentException("'oneOf' must be an array.");
            }
            List<DocumentSchema.Node> opts = new ArrayList<>();
            for ( JsonNode el : arr ) {
                opts.add(readNode(el, false));
            }
            return new DocumentSchema.OneOfNode(opts);
        }
        if ( o.has("allOf") ) {
            JsonNode arr = o.get("allOf");
            if ( !arr.isArray() ) {
                throw new IllegalArgumentException("'allOf' must be an array.");
            }
            List<DocumentSchema.Node> opts = new ArrayList<>();
            for ( JsonNode el : arr ) {
                opts.add(readNode(el, false));
            }
            return new DocumentSchema.AllOfNode(opts);
        }
        if ( o.has("not") ) {
            return new DocumentSchema.NotNode(readNode(o.get("not"), false));
        }

        // object node
        if ( o.has( "type" ) && o.get( "type" ).isTextual() ) {
            String typeText = o.get( "type" ).asText().trim().toLowerCase( Locale.ROOT );
            if ( typeText.equals( "object" ) ) {
                return readObjectNode( o, /*isRoot=*/false );
            }
            if ( typeText.equals( "array" ) ) {
                return readArrayNode( o );
            }

            // scalar node with textual type
            PolyType pt = JsonTypeTokens.toPolyType( typeText );
            return readScalarNode( List.of(pt), o );
        }

        // allow object node inference by properties
        if ( o.has( "properties" ) ) {
            return readObjectNode( o, /*isRoot=*/false );
        }

        // allow array node inference by items
        if ( o.has("items") ) {
            return readArrayNode(o);
        }

        // scalar node with type array
        if ( o.has("type") && o.get("type").isArray() ) {
            List<PolyType> pts = new ArrayList<>();
            for ( JsonNode el : o.get("type") ) {
                if ( !el.isTextual() ) {
                    throw new IllegalArgumentException("type array must contain only strings.");
                }
                String tok = el.asText().trim().toLowerCase(Locale.ROOT);
                if ( tok.equals("object") || tok.equals("array") ) {
                    throw new IllegalArgumentException("type unions containing object/array are not supported; use anyOf/oneOf instead.");
                }
                pts.add(JsonTypeTokens.toPolyType(tok));
            }
            if ( pts.isEmpty() ) {
                throw new IllegalArgumentException("type array must be non-empty.");
            }
            return readScalarNode( pts, o );
        }

        throw new IllegalArgumentException( "Missing or unsupported schema node: " + o );
    }

    private static DocumentSchema.ScalarNode readScalarNode( List<PolyType> types, ObjectNode o ) {

        Integer minLength = o.has("minLength") ? o.get("minLength").asInt() : null;
        Integer maxLength = o.has("maxLength") ? o.get("maxLength").asInt() : null;
        String pattern = o.has("pattern") ? o.get("pattern").asText(null) : null;

        BigDecimal minimum = o.has("minimum") && o.get("minimum").isNumber()
                ? o.get("minimum").decimalValue()
                : null;

        BigDecimal maximum = o.has("maximum") && o.get("maximum").isNumber()
                ? o.get("maximum").decimalValue()
                : null;

        BigDecimal multipleOf = o.has("multipleOf") && o.get("multipleOf").isNumber()
                ? o.get("multipleOf").decimalValue()
                : null;

        JsonNode constValue = o.get("const");

        List<JsonNode> enumValues = null;
        if ( o.has("enum") ) {
            JsonNode en = o.get("enum");
            if ( !en.isArray() ) {
                throw new IllegalArgumentException("'enum' must be an array.");
            }
            enumValues = new ArrayList<>();
            for ( JsonNode el : en ) {
                enumValues.add(el);
            }
        }

        return new DocumentSchema.ScalarNode(types, minLength, maxLength, pattern, minimum, maximum, multipleOf, constValue, enumValues);
    }

    private static DocumentSchema.ArrayNode readArrayNode( ObjectNode arrSpec ) {
        if ( !arrSpec.has( "items" ) ) {
            throw new IllegalArgumentException( "Array spec requires 'items'" );
        }
        DocumentSchema.Node items = readNode( arrSpec.get( "items" ), /*isRoot=*/false );
        Integer minItems = arrSpec.has( "minItems" ) ? arrSpec.get( "minItems" ).asInt() : null;
        Integer maxItems = arrSpec.has( "maxItems" ) ? arrSpec.get( "maxItems" ).asInt() : null;
        Boolean unique = arrSpec.has( "uniqueItems" ) ? arrSpec.get( "uniqueItems" ).asBoolean() : null;
        return new DocumentSchema.ArrayNode( items, minItems, maxItems, unique );
    }

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

    private static DocumentSchema.AdditionalProperties readNodeAPOrThrow( JsonNode n ) {
        if ( n == null ) {
            return DocumentSchema.AdditionalProperties.INHERIT;
        }
        if ( n.isBoolean() ) {
            return n.asBoolean() ? DocumentSchema.AdditionalProperties.ALLOW : DocumentSchema.AdditionalProperties.FORBID;
        }
        if ( n.isTextual() ) {
            String s = n.asText().trim();
            if ( s.equalsIgnoreCase("inherit") ) {
                return DocumentSchema.AdditionalProperties.INHERIT;
            }
            if ( s.equalsIgnoreCase("allow") || s.equalsIgnoreCase("true") ) {
                return DocumentSchema.AdditionalProperties.ALLOW;
            }
            if ( s.equalsIgnoreCase("forbid") || s.equalsIgnoreCase("false") ) {
                return DocumentSchema.AdditionalProperties.FORBID;
            }
        }
        throw new IllegalArgumentException( "'additionalProperties' must be boolean or one of 'INHERIT'/'ALLOW'/'FORBID'." );
    }

}
