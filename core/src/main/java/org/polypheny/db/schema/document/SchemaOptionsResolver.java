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
 * Resolves schema-related options for document CREATE and ALTER statements.
 */
public final class SchemaOptionsResolver {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();


    private SchemaOptionsResolver() {
    }


    public enum AlterMode {
        REPLACE, PATCH
    }


    public static final class Rename {

        public final String from;
        public final String to;


        public Rename( String from, String to ) {
            this.from = from;
            this.to = to;
        }


        @Override
        public String toString() {
            return from + "→" + to;
        }

    }


    public static final class Coercion {

        public final String target;
        public final String onFailure;


        public Coercion( String target, String onFailure ) {
            this.target = target;
            this.onFailure = onFailure;
        }

    }


    public static final class Resolved {

        public final DocumentSchema schema;
        public final EnforcementMode mode;
        public final AlterMode alterMode;
        public final List<Rename> renames;
        public final Map<String, JsonNode> defaults;
        public final Map<String, Coercion> coercions;
        public final boolean pruneExtras;
        public final boolean dryRun;


        public Resolved( DocumentSchema schema, EnforcementMode mode, AlterMode alterMode, List<Rename> renames, Map<String, JsonNode> defaults, Map<String, Coercion> coercions, boolean pruneExtras, boolean dryRun ) {
            this.schema = schema;
            this.mode = mode;
            this.alterMode = alterMode;
            this.renames = renames;
            this.defaults = defaults;
            this.coercions = coercions;
            this.pruneExtras = pruneExtras;
            this.dryRun = dryRun;
        }

    }


    public static Resolved resolve( PolyValue options ) {
        Resolved resolved = parseCommon( options, false );
        if ( resolved.schema == null ) {
            throw new IllegalArgumentException( "CREATE requires 'docSchema' object." );
        }
        return resolved;
    }


    public static Resolved resolveAlter( PolyValue options ) {
        return parseCommon( options, true );
    }


    private static ObjectNode requireObjectNode( JsonNode node ) {
        JsonNode current = node;

        for ( int i = 0; i < 3 && current != null; i++ ) {
            if ( current instanceof ObjectNode objectNode ) {
                return objectNode;
            }

            if ( current.isTextual() ) {
                try {
                    current = OBJECT_MAPPER.readTree( current.asText() );
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
        if ( options == null ) {
            if ( schemaOptional ) {
                return new Resolved( null, null, AlterMode.REPLACE, List.of(), Map.of(), Map.of(), false, false );
            }
            throw new IllegalArgumentException( "Missing options." );
        }

        final ObjectNode root;
        try {
            JsonNode rawOptions = OBJECT_MAPPER.readTree( options.toJson() );
            root = requireObjectNode( rawOptions );
        } catch ( Exception e ) {
            throw new IllegalArgumentException( "Invalid options payload: " + e.getMessage(), e );
        }

        EnforcementMode mode = readEnforcementMode( root );
        AlterMode alterMode = readAlterMode( root );
        List<Rename> renames = readRenames( root );
        Map<String, JsonNode> defaults = readDefaults( root );
        Map<String, Coercion> coercions = readCoercions( root );
        boolean pruneExtras = root.has( "pruneExtras" ) && root.get( "pruneExtras" ).asBoolean( false );
        boolean dryRun = root.has( "dryRun" ) && root.get( "dryRun" ).asBoolean( false );

        DocumentSchema schema = readSchema( root, schemaOptional, alterMode );

        return new Resolved( schema, mode, alterMode, renames, defaults, coercions, pruneExtras, dryRun );
    }


    private static EnforcementMode readEnforcementMode( ObjectNode root ) {
        if ( !root.has( "validationAction" ) ) {
            return null;
        }

        String validationAction = root.get( "validationAction" ).asText( "" );

        return switch ( validationAction.toLowerCase( Locale.ROOT ) ) {
            case "error", "strict" -> EnforcementMode.STRICT;
            case "warn" -> EnforcementMode.WARN;
            case "off" -> EnforcementMode.OFF;
            default -> throw new IllegalArgumentException( "Unknown validationAction: " + validationAction );
        };
    }


    private static AlterMode readAlterMode( ObjectNode root ) {
        if ( !root.has( "mode" ) ) {
            return AlterMode.REPLACE;
        }

        String mode = root.get( "mode" ).asText( "" );
        return "patch".equalsIgnoreCase( mode ) ? AlterMode.PATCH : AlterMode.REPLACE;
    }


    private static List<Rename> readRenames( ObjectNode root ) {
        List<Rename> renames = new ArrayList<>();

        JsonNode renamesNode = root.get( "renames" );
        if ( renamesNode == null || !renamesNode.isArray() ) {
            return renames;
        }

        for ( JsonNode renameNode : renamesNode ) {
            if ( renameNode.has( "from" ) && renameNode.has( "to" ) ) {
                renames.add( new Rename( renameNode.get( "from" ).asText(), renameNode.get( "to" ).asText() ) );
            }
        }

        return renames;
    }


    private static Map<String, JsonNode> readDefaults( ObjectNode root ) {
        Map<String, JsonNode> defaults = new HashMap<>();

        JsonNode defaultsNode = root.get( "defaults" );
        if ( defaultsNode == null || !defaultsNode.isObject() ) {
            return defaults;
        }

        defaultsNode.fields().forEachRemaining( entry -> defaults.put( entry.getKey(), entry.getValue() ) );
        return defaults;
    }


    private static Map<String, Coercion> readCoercions( ObjectNode root ) {
        Map<String, Coercion> coercions = new HashMap<>();

        JsonNode coercionsNode = root.get( "coercions" );
        if ( coercionsNode == null || !coercionsNode.isObject() ) {
            return coercions;
        }

        coercionsNode.fields().forEachRemaining( entry -> {
            String path = entry.getKey();
            JsonNode coercionNode = entry.getValue();

            coercions.put( path, new Coercion( coercionNode.has( "target" ) ? coercionNode.get( "target" ).asText() : "text", coercionNode.has( "onFailure" ) ? coercionNode.get( "onFailure" ).asText( "error" ) : "error" ) );
        } );

        return coercions;
    }


    private static DocumentSchema readSchema( ObjectNode root, boolean schemaOptional, AlterMode alterMode ) {
        if ( !root.has( "docSchema" ) ) {
            if ( schemaOptional ) {
                return null;
            }
            throw new IllegalArgumentException( "Missing 'docSchema'." );
        }

        JsonNode schemaNode = root.get( "docSchema" );
        if ( !schemaNode.isObject() ) {
            throw new IllegalArgumentException( "'docSchema' must be an object" );
        }

        ObjectNode schemaObject = (ObjectNode) schemaNode;
        DocumentSchema.ObjectNode rootNode = readObjectNode( schemaObject, true );

        DocumentSchema.AdditionalProperties additionalProperties;
        if ( schemaOptional && alterMode == AlterMode.PATCH && !schemaObject.has( "additionalProperties" ) ) {
            additionalProperties = null;
        } else {
            additionalProperties = readRootAPOrThrow( schemaObject );
        }

        return new DocumentSchema( rootNode, additionalProperties );
    }


    private static DocumentSchema.ObjectNode readObjectNode( ObjectNode objectSpec, boolean isRoot ) {
        JsonNode typeNode = objectSpec.get( "type" );
        if ( typeNode != null && typeNode.isTextual() ) {
            String type = typeNode.asText( "" ).trim().toLowerCase( Locale.ROOT );
            if ( !type.isEmpty() && !"object".equals( type ) ) {
                throw new IllegalArgumentException( "Object node expected, found type: " + type );
            }
        }

        Set<String> required = null;
        JsonNode requiredNode = objectSpec.get( "required" );
        if ( requiredNode != null ) {
            if ( !requiredNode.isArray() ) {
                throw new IllegalArgumentException( "'required' must be an array of strings." );
            }

            required = new LinkedHashSet<>();
            for ( JsonNode requiredEntry : requiredNode ) {
                if ( !requiredEntry.isTextual() ) {
                    throw new IllegalArgumentException( "'required' must contain only strings." );
                }
                required.add( requiredEntry.asText() );
            }
        }

        DocumentSchema.AdditionalProperties additionalProperties = DocumentSchema.AdditionalProperties.INHERIT;
        if ( !isRoot && objectSpec.has( "additionalProperties" ) ) {
            additionalProperties = readNodeAPOrThrow( objectSpec.get( "additionalProperties" ) );
        }

        Integer minProperties = objectSpec.has( "minProperties" ) ? objectSpec.get( "minProperties" ).asInt() : null;
        Integer maxProperties = objectSpec.has( "maxProperties" ) ? objectSpec.get( "maxProperties" ).asInt() : null;

        Map<String, DocumentSchema.Node> properties = new LinkedHashMap<>();
        JsonNode propertiesNode = objectSpec.get( "properties" );
        if ( propertiesNode != null ) {
            if ( !propertiesNode.isObject() ) {
                throw new IllegalArgumentException( "'properties' must be an object" );
            }

            propertiesNode.fields().forEachRemaining( entry -> properties.put( entry.getKey(), readNode( entry.getValue() ) ) );
        }

        return new DocumentSchema.ObjectNode( properties, required, additionalProperties, minProperties, maxProperties );
    }


    private static DocumentSchema.Node readNode( JsonNode spec ) {
        if ( spec.isTextual() ) {
            PolyType polyType = JsonTypeTokens.toPolyType( spec.asText() );
            return new DocumentSchema.ScalarNode( List.of( polyType ), null, null, null, null, null, null, null, null );
        }

        if ( !spec.isObject() ) {
            throw new IllegalArgumentException( "Property spec must be string or object" );
        }

        ObjectNode objectSpec = (ObjectNode) spec;

        JsonNode anyOfNode = objectSpec.get( "anyOf" );
        if ( anyOfNode != null ) {
            if ( !anyOfNode.isArray() ) {
                throw new IllegalArgumentException( "'anyOf' must be an array." );
            }

            List<DocumentSchema.Node> options = new ArrayList<>();
            for ( JsonNode optionNode : anyOfNode ) {
                options.add( readNode( optionNode ) );
            }
            return new DocumentSchema.AnyOfNode( options );
        }

        JsonNode oneOfNode = objectSpec.get( "oneOf" );
        if ( oneOfNode != null ) {
            if ( !oneOfNode.isArray() ) {
                throw new IllegalArgumentException( "'oneOf' must be an array." );
            }

            List<DocumentSchema.Node> options = new ArrayList<>();
            for ( JsonNode optionNode : oneOfNode ) {
                options.add( readNode( optionNode ) );
            }
            return new DocumentSchema.OneOfNode( options );
        }

        JsonNode allOfNode = objectSpec.get( "allOf" );
        if ( allOfNode != null ) {
            if ( !allOfNode.isArray() ) {
                throw new IllegalArgumentException( "'allOf' must be an array." );
            }

            List<DocumentSchema.Node> options = new ArrayList<>();
            for ( JsonNode optionNode : allOfNode ) {
                options.add( readNode( optionNode ) );
            }
            return new DocumentSchema.AllOfNode( options );
        }

        JsonNode notNode = objectSpec.get( "not" );
        if ( notNode != null ) {
            return new DocumentSchema.NotNode( readNode( notNode ) );
        }

        JsonNode typeNode = objectSpec.get( "type" );
        if ( typeNode != null && typeNode.isTextual() ) {
            String typeText = typeNode.asText().trim().toLowerCase( Locale.ROOT );

            if ( "object".equals( typeText ) ) {
                return readObjectNode( objectSpec, false );
            }

            if ( "array".equals( typeText ) ) {
                return readArrayNode( objectSpec );
            }

            PolyType polyType = JsonTypeTokens.toPolyType( typeText );
            return readScalarNode( List.of( polyType ), objectSpec );
        }

        if ( objectSpec.has( "properties" ) ) {
            return readObjectNode( objectSpec, false );
        }

        if ( objectSpec.has( "items" ) ) {
            return readArrayNode( objectSpec );
        }

        if ( typeNode != null && typeNode.isArray() ) {
            List<PolyType> polyTypes = new ArrayList<>();

            for ( JsonNode typeEntry : typeNode ) {
                if ( !typeEntry.isTextual() ) {
                    throw new IllegalArgumentException( "type array must contain only strings." );
                }

                String typeToken = typeEntry.asText().trim().toLowerCase( Locale.ROOT );
                if ( "object".equals( typeToken ) || "array".equals( typeToken ) ) {
                    throw new IllegalArgumentException( "type unions containing object/array are not supported; use anyOf/oneOf instead." );
                }

                polyTypes.add( JsonTypeTokens.toPolyType( typeToken ) );
            }

            if ( polyTypes.isEmpty() ) {
                throw new IllegalArgumentException( "type array must be non-empty." );
            }

            return readScalarNode( polyTypes, objectSpec );
        }

        throw new IllegalArgumentException( "Missing or unsupported schema node: " + objectSpec );
    }


    private static DocumentSchema.ScalarNode readScalarNode( List<PolyType> types, ObjectNode objectSpec ) {
        Integer minLength = objectSpec.has( "minLength" ) ? objectSpec.get( "minLength" ).asInt() : null;
        Integer maxLength = objectSpec.has( "maxLength" ) ? objectSpec.get( "maxLength" ).asInt() : null;
        String pattern = objectSpec.has( "pattern" ) ? objectSpec.get( "pattern" ).asText( null ) : null;

        BigDecimal minimum = objectSpec.has( "minimum" ) && objectSpec.get( "minimum" ).isNumber() ? objectSpec.get( "minimum" ).decimalValue() : null;

        BigDecimal maximum = objectSpec.has( "maximum" ) && objectSpec.get( "maximum" ).isNumber() ? objectSpec.get( "maximum" ).decimalValue() : null;

        BigDecimal multipleOf = objectSpec.has( "multipleOf" ) && objectSpec.get( "multipleOf" ).isNumber() ? objectSpec.get( "multipleOf" ).decimalValue() : null;

        JsonNode constValue = objectSpec.get( "const" );

        List<JsonNode> enumValues = null;
        JsonNode enumNode = objectSpec.get( "enum" );
        if ( enumNode != null ) {
            if ( !enumNode.isArray() ) {
                throw new IllegalArgumentException( "'enum' must be an array." );
            }

            enumValues = new ArrayList<>();
            for ( JsonNode enumEntry : enumNode ) {
                enumValues.add( enumEntry );
            }
        }

        return new DocumentSchema.ScalarNode( types, minLength, maxLength, pattern, minimum, maximum, multipleOf, constValue, enumValues );
    }


    private static DocumentSchema.ArrayNode readArrayNode( ObjectNode arraySpec ) {
        if ( !arraySpec.has( "items" ) ) {
            throw new IllegalArgumentException( "Array spec requires 'items'" );
        }

        DocumentSchema.Node items = readNode( arraySpec.get( "items" ) );
        Integer minItems = arraySpec.has( "minItems" ) ? arraySpec.get( "minItems" ).asInt() : null;
        Integer maxItems = arraySpec.has( "maxItems" ) ? arraySpec.get( "maxItems" ).asInt() : null;
        Boolean uniqueItems = arraySpec.has( "uniqueItems" ) ? arraySpec.get( "uniqueItems" ).asBoolean() : null;

        return new DocumentSchema.ArrayNode( items, minItems, maxItems, uniqueItems );
    }


    private static DocumentSchema.AdditionalProperties readRootAPOrThrow( ObjectNode objectNode ) {
        if ( !objectNode.has( "additionalProperties" ) ) {
            throw new IllegalArgumentException( "Top-level 'additionalProperties' must be specified (true/false or ALLOW/FORBID)." );
        }

        JsonNode additionalPropertiesNode = objectNode.get( "additionalProperties" );
        if ( additionalPropertiesNode.isBoolean() ) {
            return additionalPropertiesNode.asBoolean() ? DocumentSchema.AdditionalProperties.ALLOW : DocumentSchema.AdditionalProperties.FORBID;
        }

        if ( additionalPropertiesNode.isTextual() ) {
            String value = additionalPropertiesNode.asText();

            if ( "FORBID".equalsIgnoreCase( value ) || "false".equalsIgnoreCase( value ) ) {
                return DocumentSchema.AdditionalProperties.FORBID;
            }

            if ( "ALLOW".equalsIgnoreCase( value ) || "true".equalsIgnoreCase( value ) ) {
                return DocumentSchema.AdditionalProperties.ALLOW;
            }
        }

        throw new IllegalArgumentException( "'additionalProperties' must be boolean or 'FORBID'/'ALLOW'" );
    }


    private static DocumentSchema.AdditionalProperties readNodeAPOrThrow( JsonNode node ) {
        if ( node == null ) {
            return DocumentSchema.AdditionalProperties.INHERIT;
        }

        if ( node.isBoolean() ) {
            return node.asBoolean() ? DocumentSchema.AdditionalProperties.ALLOW : DocumentSchema.AdditionalProperties.FORBID;
        }

        if ( node.isTextual() ) {
            String value = node.asText().trim();

            if ( value.equalsIgnoreCase( "inherit" ) ) {
                return DocumentSchema.AdditionalProperties.INHERIT;
            }

            if ( value.equalsIgnoreCase( "allow" ) || value.equalsIgnoreCase( "true" ) ) {
                return DocumentSchema.AdditionalProperties.ALLOW;
            }

            if ( value.equalsIgnoreCase( "forbid" ) || value.equalsIgnoreCase( "false" ) ) {
                return DocumentSchema.AdditionalProperties.FORBID;
            }
        }

        throw new IllegalArgumentException( "'additionalProperties' must be boolean or one of 'INHERIT'/'ALLOW'/'FORBID'." );
    }

}