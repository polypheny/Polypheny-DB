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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import org.polypheny.db.type.PolyType;

/**
 * Canonical representation of a document collection schema.
 *
 * <p>This is a JSON-Schema-inspired (but intentionally smaller) dialect that is
 * optimized for PolyDBMS enforcement + evolution.</p>
 *
 * <p>Key dialect features implemented here:</p>
 * <ul>
 *   <li>Per-object {@code required} (if omitted, defaults to "all declared properties required" for backward compatibility)</li>
 *   <li>Per-object {@code additionalProperties} override (INHERIT/ALLOW/FORBID)</li>
 *   <li>Scalar union types via {@code type: ["text","null"]}</li>
 *   <li>Composition: {@code anyOf}/{@code oneOf}/{@code allOf}/{@code not}</li>
 *   <li>More constraints: enum/const/multipleOf, maxItems, min/maxProperties</li>
 * </ul>
 */
public final class DocumentSchema {

    /**
     * Policy for handling undeclared properties on object nodes.
     *
     * <ul>
     *   <li>{@code INHERIT}: inherit from parent (root inherits from schema wrapper)</li>
     *   <li>{@code ALLOW}: accept unknown properties</li>
     *   <li>{@code FORBID}: reject unknown properties</li>
     * </ul>
     */
    public enum AdditionalProperties {INHERIT, ALLOW, FORBID}


    @JsonSerialize(using = NodeSerDeSer.Serializer.class)
    @JsonDeserialize(using = NodeSerDeSer.Deserializer.class)
    public sealed interface Node permits ObjectNode, ArrayNode, ScalarNode, AnyOfNode, OneOfNode, AllOfNode, NotNode {

    }

    // -----------------------------------------------------------------------------------------
    // Scalar
    // -----------------------------------------------------------------------------------------

    public static final class ScalarNode implements Node {

        /**
         * Backward-compatible single-type view (first element of {@link #types}).
         * Prefer using {@link #types}.
         */
        public final PolyType type;

        /**
         * Allowed scalar types (union). Must be non-empty.
         */
        public final List<PolyType> types;

        // ---- Scalar constraints (subset) ----
        // Strings
        public final Integer minLength;
        public final Integer maxLength;
        public final String pattern;

        // Numbers
        public final BigDecimal minimum;
        public final BigDecimal maximum;
        public final BigDecimal multipleOf;

        // Equality / set constraints
        public final JsonNode constValue;          // optional
        public final List<JsonNode> enumValues;    // optional


        @JsonCreator
        public ScalarNode(
                @JsonProperty("types") List<PolyType> types,
                @JsonProperty("minLength") Integer minLength,
                @JsonProperty("maxLength") Integer maxLength,
                @JsonProperty("pattern") String pattern,
                @JsonProperty("minimum") BigDecimal minimum,
                @JsonProperty("maximum") BigDecimal maximum,
                @JsonProperty("multipleOf") BigDecimal multipleOf,
                @JsonProperty("const") JsonNode constValue,
                @JsonProperty("enum") List<JsonNode> enumValues ) {

            List<PolyType> t = (types == null || types.isEmpty()) ? null : List.copyOf(types);
            this.types = Objects.requireNonNull(t, "types");
            this.type = this.types.get(0);

            this.minLength = minLength;
            this.maxLength = maxLength;
            this.pattern = pattern;

            this.minimum = minimum;
            this.maximum = maximum;
            this.multipleOf = multipleOf;

            this.constValue = constValue;
            this.enumValues = enumValues == null ? null : List.copyOf(enumValues);
        }


        public static ScalarNode of( PolyType t ) {
            return new ScalarNode( List.of(t), null, null, null, null, null, null, null, null );
        }


        @Override
        public String toString() {
            return "Scalar(" + types + ")";
        }

    }

    // -----------------------------------------------------------------------------------------
    // Array
    // -----------------------------------------------------------------------------------------

    /**
     * Node representing a JSON array.
     * Contains the {@code items} schema and optional constraints such as {@code minItems}, {@code maxItems}
     * and {@code uniqueItems}.
     */
    public static final class ArrayNode implements Node {

        public final Node items;
        public final Integer minItems;
        public final Integer maxItems;
        public final Boolean uniqueItems;


        @JsonCreator
        public ArrayNode(
                @JsonProperty("items") Node items,
                @JsonProperty("minItems") Integer minItems,
                @JsonProperty("maxItems") Integer maxItems,
                @JsonProperty("uniqueItems") Boolean uniqueItems ) {

            this.items = Objects.requireNonNull( items, "items" );
            this.minItems = minItems;
            this.maxItems = maxItems;
            this.uniqueItems = uniqueItems;
        }


        @Override
        public String toString() {
            return "Array(items=" + items + ")";
        }

    }

    // -----------------------------------------------------------------------------------------
    // Object
    // -----------------------------------------------------------------------------------------

    /**
     * Node representing a JSON object with a map of named properties.
     * Property order is preserved to provide stable serialization and UI presentation.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class ObjectNode implements Node {

        public final Map<String, Node> properties;

        /**
         * Required properties for this object.
         * <ul>
         *   <li>If {@code null}: dialect-default = all declared properties are required (backward compatible)</li>
         *   <li>If non-null: only these properties are required (may be empty)</li>
         * </ul>
         */
        public final Set<String> required;

        /**
         * Per-object additionalProperties policy. If INHERIT, uses parent policy.
         */
        public final AdditionalProperties additionalProperties;

        public final Integer minProperties;
        public final Integer maxProperties;


        @JsonCreator
        public ObjectNode(
                @JsonProperty("properties") Map<String, Node> properties,
                @JsonProperty("required") Set<String> required,
                @JsonProperty("additionalProperties") AdditionalProperties additionalProperties,
                @JsonProperty("minProperties") Integer minProperties,
                @JsonProperty("maxProperties") Integer maxProperties ) {

            this.properties = properties == null ? Map.of() : Map.copyOf( properties );
            this.required = required == null ? null : Set.copyOf(required);
            this.additionalProperties = additionalProperties == null ? AdditionalProperties.INHERIT : additionalProperties;
            this.minProperties = minProperties;
            this.maxProperties = maxProperties;
        }


        public Set<String> effectiveRequired() {
            return required != null ? required : properties.keySet();
        }


        @Override
        public String toString() {
            return "Object(props=" + properties.keySet() + ")";
        }

    }

    // -----------------------------------------------------------------------------------------
    // Composition
    // -----------------------------------------------------------------------------------------

    public static final class AnyOfNode implements Node {
        public final List<Node> anyOf;

        @JsonCreator
        public AnyOfNode(@JsonProperty("anyOf") List<Node> anyOf) {
            this.anyOf = anyOf == null ? List.of() : List.copyOf(anyOf);
        }
    }

    public static final class OneOfNode implements Node {
        public final List<Node> oneOf;

        @JsonCreator
        public OneOfNode(@JsonProperty("oneOf") List<Node> oneOf) {
            this.oneOf = oneOf == null ? List.of() : List.copyOf(oneOf);
        }
    }

    public static final class AllOfNode implements Node {
        public final List<Node> allOf;

        @JsonCreator
        public AllOfNode(@JsonProperty("allOf") List<Node> allOf) {
            this.allOf = allOf == null ? List.of() : List.copyOf(allOf);
        }
    }

    public static final class NotNode implements Node {
        public final Node not;

        @JsonCreator
        public NotNode(@JsonProperty("not") Node not) {
            this.not = Objects.requireNonNull(not, "not");
        }
    }

    // -----------------------------------------------------------------------------------------
    // Schema wrapper
    // -----------------------------------------------------------------------------------------

    private final ObjectNode root;

    /**
     * Root-level additionalProperties policy (default policy for all object nodes that INHERIT).
     * May be {@code null} only for PATCH schema fragments (resolved during merge).
     */
    private final AdditionalProperties additionalProperties;


    @JsonCreator
    public DocumentSchema(
            @JsonProperty("root") ObjectNode root,
            @JsonProperty("additionalProperties") AdditionalProperties additionalProperties ) {

        this.root = Objects.requireNonNull( root, "root" );
        this.additionalProperties = additionalProperties; // may be null for PATCH fragments
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
        validateNode( root );
    }

    private static void validateNode( Node n ) {
        if ( n instanceof ObjectNode o ) {
            validateObject( o );
        } else if ( n instanceof ArrayNode a ) {
            validateArray( a );
        } else if ( n instanceof ScalarNode s ) {
            validateScalar( s );
        } else if ( n instanceof AnyOfNode ao ) {
            if ( ao.anyOf.isEmpty() ) {
                throw new IllegalArgumentException("Schema invalid: anyOf must contain at least one subschema.");
            }
            ao.anyOf.forEach(DocumentSchema::validateNode);
        } else if ( n instanceof OneOfNode oo ) {
            if ( oo.oneOf.isEmpty() ) {
                throw new IllegalArgumentException("Schema invalid: oneOf must contain at least one subschema.");
            }
            oo.oneOf.forEach(DocumentSchema::validateNode);
        } else if ( n instanceof AllOfNode al ) {
            if ( al.allOf.isEmpty() ) {
                throw new IllegalArgumentException("Schema invalid: allOf must contain at least one subschema.");
            }
            al.allOf.forEach(DocumentSchema::validateNode);
        } else if ( n instanceof NotNode nn ) {
            validateNode(nn.not);
        } else {
            throw new IllegalStateException("Unknown node kind: " + n);
        }
    }

    private static void validateObject( ObjectNode obj ) {
        // required sanity (if explicitly specified)
        if ( obj.required != null ) {
            for ( String r : obj.required ) {
                if ( r == null || r.isBlank() ) {
                    throw new IllegalArgumentException("Schema invalid: required contains blank property name.");
                }
                if ( !obj.properties.containsKey( r ) ) {
                    throw new IllegalArgumentException("Schema invalid: required refers to undeclared property '" + r + "'");
                }
            }
        }

        if ( obj.minProperties != null && obj.minProperties < 0 ) {
            throw new IllegalArgumentException("minProperties must be >= 0");
        }
        if ( obj.maxProperties != null && obj.maxProperties < 0 ) {
            throw new IllegalArgumentException("maxProperties must be >= 0");
        }
        if ( obj.minProperties != null && obj.maxProperties != null && obj.minProperties > obj.maxProperties ) {
            throw new IllegalArgumentException("minProperties must be <= maxProperties");
        }

        for ( Node child : obj.properties.values() ) {
            validateNode( child );
        }
    }

    private static void validateArray( ArrayNode a ) {
        if ( a.items == null ) {
            throw new IllegalArgumentException( "Schema invalid: array 'items' must be specified." );
        }
        if ( a.minItems != null && a.minItems < 0 ) {
            throw new IllegalArgumentException("minItems must be >= 0");
        }
        if ( a.maxItems != null && a.maxItems < 0 ) {
            throw new IllegalArgumentException("maxItems must be >= 0");
        }
        if ( a.minItems != null && a.maxItems != null && a.minItems > a.maxItems ) {
            throw new IllegalArgumentException("minItems must be <= maxItems");
        }
        validateNode( a.items );
    }

    private static void validateScalar( ScalarNode s ) {

        if ( s.types == null || s.types.isEmpty() ) {
            throw new IllegalArgumentException("Scalar node must declare at least one type.");
        }

        boolean hasString = s.types.stream().anyMatch(DocumentSchema::isStringType);
        boolean hasNumber = s.types.stream().anyMatch(DocumentSchema::isNumberType);

        if ( (s.minLength != null || s.maxLength != null || s.pattern != null) && !hasString ) {
            throw new IllegalArgumentException("String constraints require a string type (TEXT/VARCHAR/CHAR) to be allowed.");
        }

        if ( (s.minimum != null || s.maximum != null || s.multipleOf != null) && !hasNumber ) {
            throw new IllegalArgumentException("Numeric constraints require a numeric type to be allowed.");
        }

        if ( s.minLength != null && s.minLength < 0 ) {
            throw new IllegalArgumentException("minLength must be >= 0");
        }
        if ( s.maxLength != null && s.maxLength < 0 ) {
            throw new IllegalArgumentException("maxLength must be >= 0");
        }
        if ( s.minLength != null && s.maxLength != null && s.minLength > s.maxLength ) {
            throw new IllegalArgumentException("minLength must be <= maxLength");
        }

        if ( s.minimum != null && s.maximum != null && s.minimum.compareTo(s.maximum) > 0 ) {
            throw new IllegalArgumentException("minimum must be <= maximum");
        }

        if ( s.multipleOf != null && s.multipleOf.compareTo(BigDecimal.ZERO) <= 0 ) {
            throw new IllegalArgumentException("multipleOf must be > 0");
        }

        if ( s.constValue != null && s.enumValues != null ) {
            boolean ok = s.enumValues.stream().anyMatch(ev -> Objects.equals(ev, s.constValue));
            if ( !ok ) {
                throw new IllegalArgumentException("Schema invalid: const must be one of enum values when both are specified.");
            }
        }
    }

    private static boolean isStringType( PolyType t ) {
        return t == PolyType.TEXT || t == PolyType.VARCHAR || t == PolyType.CHAR;
    }

    private static boolean isNumberType( PolyType t ) {
        // Keep consistent with existing dialect which treated DOUBLE as "number".
        // (If your JsonTypeTokens maps "number" to DOUBLE, this aligns.)
        return t == PolyType.DOUBLE
                || t == PolyType.DECIMAL
                || t == PolyType.FLOAT
                || t == PolyType.REAL
                || t == PolyType.INTEGER
                || t == PolyType.BIGINT
                || t == PolyType.SMALLINT
                || t == PolyType.TINYINT;
    }

    // -----------------------------------------------------------------------------------------
    // Jackson Node SerDe
    // -----------------------------------------------------------------------------------------

    static final class NodeSerDeSer {

        static final class Serializer extends JsonSerializer<Node> {

            @Override
            public void serialize( Node value, JsonGenerator gen, SerializerProvider sp ) throws IOException {
                if ( value instanceof ScalarNode s ) {
                    gen.writeStartObject();

                    // type: string or array
                    if ( s.types.size() == 1 ) {
                        gen.writeStringField( "type", JsonTypeTokens.toJsonToken( s.types.get(0) ) );
                    } else {
                        gen.writeArrayFieldStart("type");
                        for ( PolyType t : s.types ) {
                            gen.writeString( JsonTypeTokens.toJsonToken( t ) );
                        }
                        gen.writeEndArray();
                    }

                    // String constraints
                    if ( s.minLength != null ) {
                        gen.writeNumberField( "minLength", s.minLength );
                    }
                    if ( s.maxLength != null ) {
                        gen.writeNumberField( "maxLength", s.maxLength );
                    }
                    if ( s.pattern != null ) {
                        gen.writeStringField( "pattern", s.pattern );
                    }

                    // Numeric constraints
                    if ( s.minimum != null ) {
                        gen.writeNumberField( "minimum", s.minimum );
                    }
                    if ( s.maximum != null ) {
                        gen.writeNumberField( "maximum", s.maximum );
                    }
                    if ( s.multipleOf != null ) {
                        gen.writeNumberField( "multipleOf", s.multipleOf );
                    }

                    if ( s.constValue != null ) {
                        gen.writeFieldName("const");
                        gen.writeTree(s.constValue);
                    }
                    if ( s.enumValues != null ) {
                        gen.writeArrayFieldStart("enum");
                        for ( JsonNode ev : s.enumValues ) {
                            gen.writeTree(ev);
                        }
                        gen.writeEndArray();
                    }

                    gen.writeEndObject();
                    return;
                }

                if ( value instanceof ArrayNode a ) {
                    gen.writeStartObject();
                    gen.writeStringField("type", "array");
                    gen.writeFieldName( "items" );
                    sp.defaultSerializeValue( a.items, gen );
                    if ( a.minItems != null ) {
                        gen.writeNumberField( "minItems", a.minItems );
                    }
                    if ( a.maxItems != null ) {
                        gen.writeNumberField( "maxItems", a.maxItems );
                    }
                    if ( a.uniqueItems != null ) {
                        gen.writeBooleanField( "uniqueItems", a.uniqueItems );
                    }
                    gen.writeEndObject();
                    return;
                }

                if ( value instanceof ObjectNode o ) {
                    gen.writeStartObject();
                    gen.writeStringField("type", "object");

                    // object attributes
                    if ( o.additionalProperties != null && o.additionalProperties != AdditionalProperties.INHERIT ) {
                        gen.writeStringField("additionalProperties", o.additionalProperties.name());
                    }
                    if ( o.minProperties != null ) {
                        gen.writeNumberField("minProperties", o.minProperties);
                    }
                    if ( o.maxProperties != null ) {
                        gen.writeNumberField("maxProperties", o.maxProperties);
                    }
                    if ( o.required != null ) {
                        gen.writeArrayFieldStart("required");
                        for ( String r : o.required ) {
                            gen.writeString(r);
                        }
                        gen.writeEndArray();
                    }

                    gen.writeObjectFieldStart( "properties" );
                    for ( Map.Entry<String, Node> e : o.properties.entrySet() ) {
                        gen.writeFieldName( e.getKey() );
                        sp.defaultSerializeValue( e.getValue(), gen );
                    }
                    gen.writeEndObject(); // properties
                    gen.writeEndObject();
                    return;
                }

                if ( value instanceof AnyOfNode ao ) {
                    gen.writeStartObject();
                    gen.writeArrayFieldStart("anyOf");
                    for ( Node n : ao.anyOf ) {
                        sp.defaultSerializeValue(n, gen);
                    }
                    gen.writeEndArray();
                    gen.writeEndObject();
                    return;
                }

                if ( value instanceof OneOfNode oo ) {
                    gen.writeStartObject();
                    gen.writeArrayFieldStart("oneOf");
                    for ( Node n : oo.oneOf ) {
                        sp.defaultSerializeValue(n, gen);
                    }
                    gen.writeEndArray();
                    gen.writeEndObject();
                    return;
                }

                if ( value instanceof AllOfNode al ) {
                    gen.writeStartObject();
                    gen.writeArrayFieldStart("allOf");
                    for ( Node n : al.allOf ) {
                        sp.defaultSerializeValue(n, gen);
                    }
                    gen.writeEndArray();
                    gen.writeEndObject();
                    return;
                }

                if ( value instanceof NotNode nn ) {
                    gen.writeStartObject();
                    gen.writeFieldName("not");
                    sp.defaultSerializeValue(nn.not, gen);
                    gen.writeEndObject();
                    return;
                }

                throw new IllegalStateException( "Unknown node kind: " + value );
            }

        }


        /**
         * Reads a Node from its schema-shaped JSON.
         */
        static final class Deserializer extends JsonDeserializer<Node> {

            @Override
            public Node deserialize( JsonParser p, DeserializationContext ctxt ) throws IOException {
                JsonNode n = p.readValueAsTree();
                if ( !n.isObject() ) {
                    throw new IOException( "DocumentSchema.Node must be a JSON object" );
                }

                // composition first
                if ( n.has("anyOf") ) {
                    JsonNode arr = n.get("anyOf");
                    if ( !arr.isArray() ) {
                        throw new IOException("'anyOf' must be an array");
                    }
                    List<Node> opts = new ArrayList<>();
                    for ( JsonNode el : arr ) {
                        opts.add( p.getCodec().treeToValue( el, Node.class ) );
                    }
                    return new AnyOfNode(opts);
                }
                if ( n.has("oneOf") ) {
                    JsonNode arr = n.get("oneOf");
                    if ( !arr.isArray() ) {
                        throw new IOException("'oneOf' must be an array");
                    }
                    List<Node> opts = new ArrayList<>();
                    for ( JsonNode el : arr ) {
                        opts.add( p.getCodec().treeToValue( el, Node.class ) );
                    }
                    return new OneOfNode(opts);
                }
                if ( n.has("allOf") ) {
                    JsonNode arr = n.get("allOf");
                    if ( !arr.isArray() ) {
                        throw new IOException("'allOf' must be an array");
                    }
                    List<Node> opts = new ArrayList<>();
                    for ( JsonNode el : arr ) {
                        opts.add( p.getCodec().treeToValue( el, Node.class ) );
                    }
                    return new AllOfNode(opts);
                }
                if ( n.has("not") ) {
                    JsonNode child = n.get("not");
                    Node cn = p.getCodec().treeToValue(child, Node.class);
                    return new NotNode(cn);
                }

                JsonNode props = n.get( "properties" );
                if ( props != null && props.isObject() ) {
                    Map<String, Node> map = new LinkedHashMap<>();
                    for ( Iterator<Entry<String, JsonNode>> it = props.fields(); it.hasNext(); ) {
                        Map.Entry<String, JsonNode> e = it.next();
                        Node child = p.getCodec().treeToValue( e.getValue(), Node.class );
                        map.put( e.getKey(), child );
                    }

                    // object attrs
                    Set<String> required = null;
                    if ( n.has("required") && n.get("required").isArray() ) {
                        required = new LinkedHashSet<>();
                        for ( JsonNode r : n.get("required") ) {
                            if ( r.isTextual() ) {
                                required.add(r.asText());
                            }
                        }
                    }

                    AdditionalProperties ap = AdditionalProperties.INHERIT;
                    if ( n.has("additionalProperties") ) {
                        JsonNode apn = n.get("additionalProperties");
                        if ( apn.isBoolean() ) {
                            ap = apn.asBoolean() ? AdditionalProperties.ALLOW : AdditionalProperties.FORBID;
                        } else if ( apn.isTextual() ) {
                            String s = apn.asText().trim().toUpperCase(Locale.ROOT);
                            if ( "ALLOW".equals(s) || "TRUE".equals(s) ) {
                                ap = AdditionalProperties.ALLOW;
                            } else if ( "FORBID".equals(s) || "FALSE".equals(s) ) {
                                ap = AdditionalProperties.FORBID;
                            } else if ( "INHERIT".equals(s) ) {
                                ap = AdditionalProperties.INHERIT;
                            } else {
                                throw new IOException("Invalid additionalProperties: " + apn);
                            }
                        }
                    }

                    Integer minProps = n.has("minProperties") && n.get("minProperties").canConvertToInt() ? n.get("minProperties").intValue() : null;
                    Integer maxProps = n.has("maxProperties") && n.get("maxProperties").canConvertToInt() ? n.get("maxProperties").intValue() : null;

                    return new ObjectNode(map, required, ap, minProps, maxProps);
                }

                JsonNode items = n.get( "items" );
                if ( items != null ) {
                    Node item = p.getCodec().treeToValue( items, Node.class );
                    Integer minItems = n.has( "minItems" ) && n.get( "minItems" ).canConvertToInt() ? n.get( "minItems" ).intValue() : null;
                    Integer maxItems = n.has( "maxItems" ) && n.get( "maxItems" ).canConvertToInt() ? n.get( "maxItems" ).intValue() : null;
                    Boolean unique = n.has( "uniqueItems" ) ? n.get( "uniqueItems" ).asBoolean() : null;
                    return new ArrayNode( item, minItems, maxItems, unique );
                }

                // scalar
                JsonNode t = n.get( "type" );
                if ( t == null ) {
                    throw new IOException( "Scalar node requires 'type'" );
                }

                List<PolyType> types = new ArrayList<>();
                if ( t.isTextual() ) {
                    types.add( JsonTypeTokens.toPolyType( t.asText() ) );
                } else if ( t.isArray() ) {
                    for ( JsonNode el : t ) {
                        if ( !el.isTextual() ) {
                            throw new IOException("Scalar node requires textual type tokens");
                        }
                        types.add( JsonTypeTokens.toPolyType( el.asText() ) );
                    }
                } else {
                    throw new IOException( "Scalar node requires textual or array 'type'" );
                }

                Integer minLength = n.has( "minLength" ) && n.get( "minLength" ).canConvertToInt() ? n.get( "minLength" ).intValue() : null;
                Integer maxLength = n.has( "maxLength" ) && n.get( "maxLength" ).canConvertToInt() ? n.get( "maxLength" ).intValue() : null;
                String pattern = n.has( "pattern" ) && n.get( "pattern" ).isTextual() ? n.get( "pattern" ).asText() : null;

                BigDecimal minimum = n.has( "minimum" ) && n.get( "minimum" ).isNumber() ? n.get( "minimum" ).decimalValue() : null;
                BigDecimal maximum = n.has( "maximum" ) && n.get( "maximum" ).isNumber() ? n.get( "maximum" ).decimalValue() : null;
                BigDecimal multipleOf = n.has( "multipleOf" ) && n.get( "multipleOf" ).isNumber() ? n.get( "multipleOf" ).decimalValue() : null;

                JsonNode constValue = n.get("const");
                List<JsonNode> enumValues = null;
                if ( n.has("enum") && n.get("enum").isArray() ) {
                    enumValues = new ArrayList<>();
                    for ( JsonNode ev : n.get("enum") ) {
                        enumValues.add(ev);
                    }
                }

                return new ScalarNode( types, minLength, maxLength, pattern, minimum, maximum, multipleOf, constValue, enumValues );
            }

        }

    }

}
