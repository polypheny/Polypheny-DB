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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.polypheny.db.type.PolyType;

/**
 * Canonical representation of a document collection schema.
 * The schema is modeled as an abstract syntax tree with three node kinds:
 * {@code ObjectNode} — a JSON object with named properties.
 * {@code ArrayNode} — a JSON array with homogeneous item type and optional constraints.
 * {@code ScalarNode} — a leaf node backed by a Polypheny {@code PolyType}.
 * The root-level {@code additionalProperties} controls whether undeclared properties are allowed.
 */
public final class DocumentSchema {

    /**
     * Root-level policy for handling undeclared properties.
     * {@code ALLOW} — extra properties are accepted.
     * {@code FORBID} — extra properties are rejected.
     * This flag applies during validation at all object levels.
     */
    public enum AdditionalProperties {ALLOW, FORBID}


    @JsonSerialize(using = NodeSerDeSer.Serializer.class)
    @JsonDeserialize(using = NodeSerDeSer.Deserializer.class)
    public sealed interface Node permits ObjectNode, ArrayNode, ScalarNode {

    }


    public static final class ScalarNode implements Node {

        public final PolyType type;


        /**
         * Leaf node representing a scalar value, parameterized by a Polypheny {@code PolyType}.
         */
        @JsonCreator
        public ScalarNode( @JsonProperty("type") PolyType type ) {
            this.type = Objects.requireNonNull( type, "type" );
        }


        public static ScalarNode of( PolyType t ) {
            return new ScalarNode( t );
        }


        @Override
        public String toString() {
            return "Scalar(" + type + ")";
        }

    }


    /**
     * Node representing a JSON array.
     * Contains the {@code items} schema and optional constraints such as {@code minItems} and {@code uniqueItems}.
     */
    public static final class ArrayNode implements Node {

        public final Node items;
        public final Integer minItems;
        public final Boolean uniqueItems;


        @JsonCreator
        public ArrayNode( @JsonProperty("items") Node items, @JsonProperty("minItems") Integer minItems, @JsonProperty("uniqueItems") Boolean uniqueItems ) {
            this.items = Objects.requireNonNull( items, "items" );
            this.minItems = minItems;
            this.uniqueItems = uniqueItems;
        }


        @Override
        public String toString() {
            return "Array(items=" + items + ")";
        }

    }


    /**
     * Node representing a JSON object with a map of named properties.
     * Property order is preserved to provide stable serialization and UI presentation.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class ObjectNode implements Node {

        public final Map<String, Node> properties;


        @JsonCreator
        public ObjectNode( @JsonProperty("properties") Map<String, Node> properties ) {
            this.properties = properties == null ? Map.of() : Map.copyOf( properties );
        }


        @Override
        public String toString() {
            return "Object(props=" + properties.keySet() + ")";
        }

    }


    private final ObjectNode root;
    private final AdditionalProperties additionalProperties; // ROOT-LEVEL ONLY


    /**
     * Creates a new schema instance.
     *
     * @param root the root schema node; must not be {@code null}
     * @param additionalProperties the root-level additional properties policy
     */
    @JsonCreator
    public DocumentSchema( @JsonProperty("root") ObjectNode root, @JsonProperty("additionalProperties") AdditionalProperties additionalProperties ) {
        this.root = Objects.requireNonNull( root, "root" );
        this.additionalProperties = Objects.requireNonNull( additionalProperties, "Root 'additionalProperties' must be specified (ALLOW or FORBID)" );
    }


    @JsonProperty("root")
    public ObjectNode root() {
        return root;
    }


    @JsonProperty("additionalProperties")
    public AdditionalProperties additionalProperties() {
        return additionalProperties;
    }


    /**
     * Validates the consistency of the schema.
     */
    public void validateOrThrow() {
        validateObject( root );
    }


    /**
     * Validates the consistency of the schema.
     */
    private static void validateObject( ObjectNode obj ) {
        for ( Node n : obj.properties.values() ) {
            if ( n instanceof ObjectNode o ) {
                validateObject( o );
            } else if ( n instanceof ArrayNode a ) {
                validateArray( a );
            } else if ( n instanceof ScalarNode s ) {
                validateScalar( s );
            }
        }
    }


    /**
     * Validates the consistency of the schema.
     */
    private static void validateArray( ArrayNode a ) {
        if ( a.items == null ) {
            throw new IllegalArgumentException( "Schema invalid: array 'items' must be specified." );
        }
        if ( a.items instanceof ObjectNode o ) {
            validateObject( o );
        } else if ( a.items instanceof ArrayNode an ) {
            validateArray( an );
        } else if ( a.items instanceof ScalarNode s ) {
            validateScalar( s );
        }
    }


    /**
     * Validates the consistency of the schema.
     */
    private static void validateScalar( ScalarNode s ) {
        if ( s.type == null ) {
            throw new IllegalArgumentException( "Scalar type must be specified" );
        }
    }


    /**
     * Jackson helper to serialize / deserializer the nodes
     */
    static final class NodeSerDeSer {

        static final class Serializer extends JsonSerializer<Node> {

            @Override
            public void serialize( Node value, JsonGenerator gen, SerializerProvider sp ) throws IOException {
                if ( value instanceof ScalarNode s ) {
                    gen.writeStartObject();
                    gen.writeStringField( "type", JsonTypeTokens.toJsonToken( s.type ) );
                    gen.writeEndObject();
                } else if ( value instanceof ArrayNode a ) {
                    gen.writeStartObject();
                    gen.writeFieldName( "items" );
                    sp.defaultSerializeValue( a.items, gen );
                    if ( a.minItems != null ) {
                        gen.writeNumberField( "minItems", a.minItems );
                    }
                    if ( a.uniqueItems != null ) {
                        gen.writeBooleanField( "uniqueItems", a.uniqueItems );
                    }
                    gen.writeEndObject();
                } else if ( value instanceof ObjectNode o ) {
                    gen.writeStartObject();
                    gen.writeObjectFieldStart( "properties" );
                    for ( Map.Entry<String, Node> e : o.properties.entrySet() ) {
                        gen.writeFieldName( e.getKey() );
                        sp.defaultSerializeValue( e.getValue(), gen );
                    }
                    gen.writeEndObject(); // properties
                    gen.writeEndObject();
                } else {
                    throw new IllegalStateException( "Unknown node kind: " + value );
                }
            }

        }


        /**
         * Reads a Node from its schema-shaped JSON.
         */
        static final class Deserializer extends JsonDeserializer<Node> {

            /**
             * Reconstructs a Node by inspecting properties -> items -> type.
             */
            @Override
            public Node deserialize( JsonParser p, DeserializationContext ctxt ) throws IOException {
                JsonNode n = p.readValueAsTree();
                if ( !n.isObject() ) {
                    throw new IOException( "DocumentSchema.Node must be a JSON object" );
                }

                JsonNode props = n.get( "properties" );
                if ( props != null && props.isObject() ) {
                    Map<String, Node> map = new LinkedHashMap<>();
                    for ( Iterator<Entry<String, JsonNode>> it = props.fields(); it.hasNext(); ) {
                        Map.Entry<String, JsonNode> e = it.next();
                        Node child = p.getCodec().treeToValue( e.getValue(), Node.class );
                        map.put( e.getKey(), child );
                    }
                    return new ObjectNode( map );
                }

                JsonNode items = n.get( "items" );
                if ( items != null ) {
                    Node item = p.getCodec().treeToValue( items, Node.class );
                    Integer minItems = n.has( "minItems" ) && n.get( "minItems" ).canConvertToInt() ? n.get( "minItems" ).intValue() : null;
                    Boolean unique = n.has( "uniqueItems" ) ? n.get( "uniqueItems" ).asBoolean() : null;
                    return new ArrayNode( item, minItems, unique );
                }

                JsonNode t = n.get( "type" );
                if ( t == null || !t.isTextual() ) {
                    throw new IOException( "Scalar node requires textual 'type'" );
                }
                PolyType pt = JsonTypeTokens.toPolyType( t.asText() );
                return new ScalarNode( pt );
            }

        }

    }

}
