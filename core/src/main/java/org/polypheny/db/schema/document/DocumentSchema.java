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
 * Canonical document schema representation.
 */
public final class DocumentSchema {

    public enum AdditionalProperties {
        INHERIT, ALLOW, FORBID
    }


    @JsonSerialize(using = NodeSerDeSer.Serializer.class)
    @JsonDeserialize(using = NodeSerDeSer.Deserializer.class)
    public sealed interface Node permits ObjectNode, ArrayNode, ScalarNode, AnyOfNode, OneOfNode, AllOfNode, NotNode {

    }


    public static final class ScalarNode implements Node {

        public final PolyType type;
        public final List<PolyType> types;

        public final Integer minLength;
        public final Integer maxLength;
        public final String pattern;

        public final BigDecimal minimum;
        public final BigDecimal maximum;
        public final BigDecimal multipleOf;

        public final JsonNode constValue;
        public final List<JsonNode> enumValues;


        @JsonCreator
        public ScalarNode( @JsonProperty("types") List<PolyType> types, @JsonProperty("minLength") Integer minLength, @JsonProperty("maxLength") Integer maxLength, @JsonProperty("pattern") String pattern, @JsonProperty("minimum") BigDecimal minimum, @JsonProperty("maximum") BigDecimal maximum, @JsonProperty("multipleOf") BigDecimal multipleOf, @JsonProperty("const") JsonNode constValue, @JsonProperty("enum") List<JsonNode> enumValues ) {

            List<PolyType> resolvedTypes = types == null || types.isEmpty() ? null : List.copyOf( types );

            this.types = Objects.requireNonNull( resolvedTypes, "types" );
            this.type = this.types.get( 0 );

            this.minLength = minLength;
            this.maxLength = maxLength;
            this.pattern = pattern;

            this.minimum = minimum;
            this.maximum = maximum;
            this.multipleOf = multipleOf;

            this.constValue = constValue;
            this.enumValues = enumValues == null ? null : List.copyOf( enumValues );
        }


        public static ScalarNode of( PolyType type ) {
            return new ScalarNode( List.of( type ), null, null, null, null, null, null, null, null );
        }


        @Override
        public String toString() {
            return "Scalar(" + types + ")";
        }

    }


    public static final class ArrayNode implements Node {

        public final Node items;
        public final Integer minItems;
        public final Integer maxItems;
        public final Boolean uniqueItems;


        @JsonCreator
        public ArrayNode( @JsonProperty("items") Node items, @JsonProperty("minItems") Integer minItems, @JsonProperty("maxItems") Integer maxItems, @JsonProperty("uniqueItems") Boolean uniqueItems ) {

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


    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class ObjectNode implements Node {

        public final Map<String, Node> properties;
        public final Set<String> required;
        public final AdditionalProperties additionalProperties;
        public final Integer minProperties;
        public final Integer maxProperties;


        @JsonCreator
        public ObjectNode( @JsonProperty("properties") Map<String, Node> properties, @JsonProperty("required") Set<String> required, @JsonProperty("additionalProperties") AdditionalProperties additionalProperties, @JsonProperty("minProperties") Integer minProperties, @JsonProperty("maxProperties") Integer maxProperties ) {

            this.properties = properties == null ? Map.of() : Map.copyOf( properties );
            this.required = required == null ? null : Set.copyOf( required );
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


    public static final class AnyOfNode implements Node {

        public final List<Node> anyOf;


        @JsonCreator
        public AnyOfNode( @JsonProperty("anyOf") List<Node> anyOf ) {
            this.anyOf = anyOf == null ? List.of() : List.copyOf( anyOf );
        }

    }


    public static final class OneOfNode implements Node {

        public final List<Node> oneOf;


        @JsonCreator
        public OneOfNode( @JsonProperty("oneOf") List<Node> oneOf ) {
            this.oneOf = oneOf == null ? List.of() : List.copyOf( oneOf );
        }

    }


    public static final class AllOfNode implements Node {

        public final List<Node> allOf;


        @JsonCreator
        public AllOfNode( @JsonProperty("allOf") List<Node> allOf ) {
            this.allOf = allOf == null ? List.of() : List.copyOf( allOf );
        }

    }


    public static final class NotNode implements Node {

        public final Node not;


        @JsonCreator
        public NotNode( @JsonProperty("not") Node not ) {
            this.not = Objects.requireNonNull( not, "not" );
        }

    }


    private final ObjectNode root;

    /**
     * May be null only for PATCH schema fragments.
     */
    private final AdditionalProperties additionalProperties;


    @JsonCreator
    public DocumentSchema( @JsonProperty("root") ObjectNode root, @JsonProperty("additionalProperties") AdditionalProperties additionalProperties ) {

        this.root = Objects.requireNonNull( root, "root" );
        this.additionalProperties = additionalProperties;
    }


    @JsonProperty("root")
    public ObjectNode root() {
        return root;
    }


    @JsonProperty("additionalProperties")
    public AdditionalProperties additionalProperties() {
        return additionalProperties;
    }


    public void validateOrThrow() {
        validateNode( root );
    }


    private static void validateNode( Node node ) {
        if ( node instanceof ObjectNode objectNode ) {
            validateObject( objectNode );
        } else if ( node instanceof ArrayNode arrayNode ) {
            validateArray( arrayNode );
        } else if ( node instanceof ScalarNode scalarNode ) {
            validateScalar( scalarNode );
        } else if ( node instanceof AnyOfNode anyOfNode ) {
            validateCompositeNode( anyOfNode.anyOf, "anyOf" );
        } else if ( node instanceof OneOfNode oneOfNode ) {
            validateCompositeNode( oneOfNode.oneOf, "oneOf" );
        } else if ( node instanceof AllOfNode allOfNode ) {
            validateCompositeNode( allOfNode.allOf, "allOf" );
        } else if ( node instanceof NotNode notNode ) {
            validateNode( notNode.not );
        } else {
            throw new IllegalStateException( "Unknown node kind: " + node );
        }
    }


    private static void validateCompositeNode( List<Node> nodes, String name ) {
        if ( nodes.isEmpty() ) {
            throw new IllegalArgumentException( "Schema invalid: " + name + " must contain at least one subschema." );
        }

        for ( Node node : nodes ) {
            validateNode( node );
        }
    }


    private static void validateObject( ObjectNode objectNode ) {
        if ( objectNode.required != null ) {
            for ( String requiredProperty : objectNode.required ) {
                if ( requiredProperty == null || requiredProperty.isBlank() ) {
                    throw new IllegalArgumentException( "Schema invalid: required contains blank property name." );
                }

                if ( !objectNode.properties.containsKey( requiredProperty ) ) {
                    throw new IllegalArgumentException( "Schema invalid: required refers to undeclared property '" + requiredProperty + "'" );
                }
            }
        }

        if ( objectNode.minProperties != null && objectNode.minProperties < 0 ) {
            throw new IllegalArgumentException( "minProperties must be >= 0" );
        }

        if ( objectNode.maxProperties != null && objectNode.maxProperties < 0 ) {
            throw new IllegalArgumentException( "maxProperties must be >= 0" );
        }

        if ( objectNode.minProperties != null && objectNode.maxProperties != null && objectNode.minProperties > objectNode.maxProperties ) {
            throw new IllegalArgumentException( "minProperties must be <= maxProperties" );
        }

        for ( Node childNode : objectNode.properties.values() ) {
            validateNode( childNode );
        }
    }


    private static void validateArray( ArrayNode arrayNode ) {
        if ( arrayNode.items == null ) {
            throw new IllegalArgumentException( "Schema invalid: array 'items' must be specified." );
        }

        if ( arrayNode.minItems != null && arrayNode.minItems < 0 ) {
            throw new IllegalArgumentException( "minItems must be >= 0" );
        }

        if ( arrayNode.maxItems != null && arrayNode.maxItems < 0 ) {
            throw new IllegalArgumentException( "maxItems must be >= 0" );
        }

        if ( arrayNode.minItems != null && arrayNode.maxItems != null && arrayNode.minItems > arrayNode.maxItems ) {
            throw new IllegalArgumentException( "minItems must be <= maxItems" );
        }

        validateNode( arrayNode.items );
    }


    private static void validateScalar( ScalarNode scalarNode ) {
        if ( scalarNode.types == null || scalarNode.types.isEmpty() ) {
            throw new IllegalArgumentException( "Scalar node must declare at least one type." );
        }

        boolean hasStringType = scalarNode.types.stream().anyMatch( DocumentSchema::isStringType );
        boolean hasNumberType = scalarNode.types.stream().anyMatch( DocumentSchema::isNumberType );

        if ( (scalarNode.minLength != null || scalarNode.maxLength != null || scalarNode.pattern != null) && !hasStringType ) {
            throw new IllegalArgumentException( "String constraints require a string type (TEXT/VARCHAR/CHAR) to be allowed." );
        }

        if ( (scalarNode.minimum != null || scalarNode.maximum != null || scalarNode.multipleOf != null) && !hasNumberType ) {
            throw new IllegalArgumentException( "Numeric constraints require a numeric type to be allowed." );
        }

        if ( scalarNode.minLength != null && scalarNode.minLength < 0 ) {
            throw new IllegalArgumentException( "minLength must be >= 0" );
        }

        if ( scalarNode.maxLength != null && scalarNode.maxLength < 0 ) {
            throw new IllegalArgumentException( "maxLength must be >= 0" );
        }

        if ( scalarNode.minLength != null && scalarNode.maxLength != null && scalarNode.minLength > scalarNode.maxLength ) {
            throw new IllegalArgumentException( "minLength must be <= maxLength" );
        }

        if ( scalarNode.minimum != null && scalarNode.maximum != null && scalarNode.minimum.compareTo( scalarNode.maximum ) > 0 ) {
            throw new IllegalArgumentException( "minimum must be <= maximum" );
        }

        if ( scalarNode.multipleOf != null && scalarNode.multipleOf.compareTo( BigDecimal.ZERO ) <= 0 ) {
            throw new IllegalArgumentException( "multipleOf must be > 0" );
        }

        if ( scalarNode.constValue != null && scalarNode.enumValues != null ) {
            boolean containsConst = scalarNode.enumValues.stream().anyMatch( enumValue -> Objects.equals( enumValue, scalarNode.constValue ) );

            if ( !containsConst ) {
                throw new IllegalArgumentException( "Schema invalid: const must be one of enum values when both are specified." );
            }
        }
    }


    private static boolean isStringType( PolyType type ) {
        return type == PolyType.TEXT || type == PolyType.VARCHAR || type == PolyType.CHAR;
    }


    private static boolean isNumberType( PolyType type ) {
        return type == PolyType.DOUBLE || type == PolyType.DECIMAL || type == PolyType.FLOAT || type == PolyType.REAL || type == PolyType.INTEGER || type == PolyType.BIGINT || type == PolyType.SMALLINT || type == PolyType.TINYINT;
    }


    static final class NodeSerDeSer {

        private NodeSerDeSer() {
        }


        static final class Serializer extends JsonSerializer<Node> {

            @Override
            public void serialize( Node value, JsonGenerator generator, SerializerProvider serializerProvider ) throws IOException {
                if ( value instanceof ScalarNode scalarNode ) {
                    writeScalarNode( scalarNode, generator );
                    return;
                }

                if ( value instanceof ArrayNode arrayNode ) {
                    writeArrayNode( arrayNode, generator, serializerProvider );
                    return;
                }

                if ( value instanceof ObjectNode objectNode ) {
                    writeObjectNode( objectNode, generator, serializerProvider );
                    return;
                }

                if ( value instanceof AnyOfNode anyOfNode ) {
                    writeNodeArrayField( "anyOf", anyOfNode.anyOf, generator, serializerProvider );
                    return;
                }

                if ( value instanceof OneOfNode oneOfNode ) {
                    writeNodeArrayField( "oneOf", oneOfNode.oneOf, generator, serializerProvider );
                    return;
                }

                if ( value instanceof AllOfNode allOfNode ) {
                    writeNodeArrayField( "allOf", allOfNode.allOf, generator, serializerProvider );
                    return;
                }

                if ( value instanceof NotNode notNode ) {
                    generator.writeStartObject();
                    generator.writeFieldName( "not" );
                    serializerProvider.defaultSerializeValue( notNode.not, generator );
                    generator.writeEndObject();
                    return;
                }

                throw new IllegalStateException( "Unknown node kind: " + value );
            }


            private static void writeScalarNode( ScalarNode scalarNode, JsonGenerator generator ) throws IOException {
                generator.writeStartObject();

                if ( scalarNode.types.size() == 1 ) {
                    generator.writeStringField( "type", JsonTypeTokens.toJsonToken( scalarNode.types.get( 0 ) ) );
                } else {
                    generator.writeArrayFieldStart( "type" );
                    for ( PolyType type : scalarNode.types ) {
                        generator.writeString( JsonTypeTokens.toJsonToken( type ) );
                    }
                    generator.writeEndArray();
                }

                if ( scalarNode.minLength != null ) {
                    generator.writeNumberField( "minLength", scalarNode.minLength );
                }
                if ( scalarNode.maxLength != null ) {
                    generator.writeNumberField( "maxLength", scalarNode.maxLength );
                }
                if ( scalarNode.pattern != null ) {
                    generator.writeStringField( "pattern", scalarNode.pattern );
                }
                if ( scalarNode.minimum != null ) {
                    generator.writeNumberField( "minimum", scalarNode.minimum );
                }
                if ( scalarNode.maximum != null ) {
                    generator.writeNumberField( "maximum", scalarNode.maximum );
                }
                if ( scalarNode.multipleOf != null ) {
                    generator.writeNumberField( "multipleOf", scalarNode.multipleOf );
                }
                if ( scalarNode.constValue != null ) {
                    generator.writeFieldName( "const" );
                    generator.writeTree( scalarNode.constValue );
                }
                if ( scalarNode.enumValues != null ) {
                    generator.writeArrayFieldStart( "enum" );
                    for ( JsonNode enumValue : scalarNode.enumValues ) {
                        generator.writeTree( enumValue );
                    }
                    generator.writeEndArray();
                }

                generator.writeEndObject();
            }


            private static void writeArrayNode( ArrayNode arrayNode, JsonGenerator generator, SerializerProvider serializerProvider ) throws IOException {
                generator.writeStartObject();
                generator.writeStringField( "type", "array" );
                generator.writeFieldName( "items" );
                serializerProvider.defaultSerializeValue( arrayNode.items, generator );

                if ( arrayNode.minItems != null ) {
                    generator.writeNumberField( "minItems", arrayNode.minItems );
                }
                if ( arrayNode.maxItems != null ) {
                    generator.writeNumberField( "maxItems", arrayNode.maxItems );
                }
                if ( arrayNode.uniqueItems != null ) {
                    generator.writeBooleanField( "uniqueItems", arrayNode.uniqueItems );
                }

                generator.writeEndObject();
            }


            private static void writeObjectNode( ObjectNode objectNode, JsonGenerator generator, SerializerProvider serializerProvider ) throws IOException {
                generator.writeStartObject();
                generator.writeStringField( "type", "object" );

                if ( objectNode.additionalProperties != null && objectNode.additionalProperties != AdditionalProperties.INHERIT ) {
                    generator.writeStringField( "additionalProperties", objectNode.additionalProperties.name() );
                }
                if ( objectNode.minProperties != null ) {
                    generator.writeNumberField( "minProperties", objectNode.minProperties );
                }
                if ( objectNode.maxProperties != null ) {
                    generator.writeNumberField( "maxProperties", objectNode.maxProperties );
                }
                if ( objectNode.required != null ) {
                    generator.writeArrayFieldStart( "required" );
                    for ( String requiredProperty : objectNode.required ) {
                        generator.writeString( requiredProperty );
                    }
                    generator.writeEndArray();
                }

                generator.writeObjectFieldStart( "properties" );
                for ( Map.Entry<String, Node> entry : objectNode.properties.entrySet() ) {
                    generator.writeFieldName( entry.getKey() );
                    serializerProvider.defaultSerializeValue( entry.getValue(), generator );
                }
                generator.writeEndObject();
                generator.writeEndObject();
            }


            private static void writeNodeArrayField( String fieldName, List<Node> nodes, JsonGenerator generator, SerializerProvider serializerProvider ) throws IOException {
                generator.writeStartObject();
                generator.writeArrayFieldStart( fieldName );
                for ( Node node : nodes ) {
                    serializerProvider.defaultSerializeValue( node, generator );
                }
                generator.writeEndArray();
                generator.writeEndObject();
            }

        }


        static final class Deserializer extends JsonDeserializer<Node> {

            @Override
            public Node deserialize( JsonParser parser, DeserializationContext context ) throws IOException {
                JsonNode node = parser.readValueAsTree();
                if ( !node.isObject() ) {
                    throw new IOException( "DocumentSchema.Node must be a JSON object" );
                }

                Node compositionNode = tryDeserializeCompositionNode( node, parser );
                if ( compositionNode != null ) {
                    return compositionNode;
                }

                JsonNode propertiesNode = node.get( "properties" );
                if ( propertiesNode != null && propertiesNode.isObject() ) {
                    return deserializeObjectNode( node, propertiesNode, parser );
                }

                JsonNode itemsNode = node.get( "items" );
                if ( itemsNode != null ) {
                    return deserializeArrayNode( node, itemsNode, parser );
                }

                return deserializeScalarNode( node );
            }


            private static Node tryDeserializeCompositionNode( JsonNode node, JsonParser parser ) throws IOException {
                if ( node.has( "anyOf" ) ) {
                    return new AnyOfNode( readNodeArray( node.get( "anyOf" ), "anyOf", parser ) );
                }

                if ( node.has( "oneOf" ) ) {
                    return new OneOfNode( readNodeArray( node.get( "oneOf" ), "oneOf", parser ) );
                }

                if ( node.has( "allOf" ) ) {
                    return new AllOfNode( readNodeArray( node.get( "allOf" ), "allOf", parser ) );
                }

                if ( node.has( "not" ) ) {
                    Node childNode = parser.getCodec().treeToValue( node.get( "not" ), Node.class );
                    return new NotNode( childNode );
                }

                return null;
            }


            private static List<Node> readNodeArray( JsonNode node, String fieldName, JsonParser parser ) throws IOException {
                if ( !node.isArray() ) {
                    throw new IOException( "'" + fieldName + "' must be an array" );
                }

                List<Node> nodes = new ArrayList<>();
                for ( JsonNode element : node ) {
                    nodes.add( parser.getCodec().treeToValue( element, Node.class ) );
                }
                return nodes;
            }


            private static ObjectNode deserializeObjectNode( JsonNode node, JsonNode propertiesNode, JsonParser parser ) throws IOException {
                Map<String, Node> properties = new LinkedHashMap<>();

                for ( Iterator<Entry<String, JsonNode>> iterator = propertiesNode.fields(); iterator.hasNext(); ) {
                    Map.Entry<String, JsonNode> entry = iterator.next();
                    Node childNode = parser.getCodec().treeToValue( entry.getValue(), Node.class );
                    properties.put( entry.getKey(), childNode );
                }

                Set<String> required = readRequiredProperties( node );
                AdditionalProperties additionalProperties = readAdditionalProperties( node );
                Integer minProperties = readIntegerField( node, "minProperties" );
                Integer maxProperties = readIntegerField( node, "maxProperties" );

                return new ObjectNode( properties, required, additionalProperties, minProperties, maxProperties );
            }


            private static Set<String> readRequiredProperties( JsonNode node ) {
                if ( !node.has( "required" ) || !node.get( "required" ).isArray() ) {
                    return null;
                }

                Set<String> required = new LinkedHashSet<>();
                for ( JsonNode requiredNode : node.get( "required" ) ) {
                    if ( requiredNode.isTextual() ) {
                        required.add( requiredNode.asText() );
                    }
                }
                return required;
            }


            private static AdditionalProperties readAdditionalProperties( JsonNode node ) throws IOException {
                AdditionalProperties additionalProperties = AdditionalProperties.INHERIT;

                if ( !node.has( "additionalProperties" ) ) {
                    return additionalProperties;
                }

                JsonNode additionalPropertiesNode = node.get( "additionalProperties" );
                if ( additionalPropertiesNode.isBoolean() ) {
                    return additionalPropertiesNode.asBoolean() ? AdditionalProperties.ALLOW : AdditionalProperties.FORBID;
                }

                if ( additionalPropertiesNode.isTextual() ) {
                    String value = additionalPropertiesNode.asText().trim().toUpperCase( Locale.ROOT );

                    if ( "ALLOW".equals( value ) || "TRUE".equals( value ) ) {
                        return AdditionalProperties.ALLOW;
                    }
                    if ( "FORBID".equals( value ) || "FALSE".equals( value ) ) {
                        return AdditionalProperties.FORBID;
                    }
                    if ( "INHERIT".equals( value ) ) {
                        return AdditionalProperties.INHERIT;
                    }

                    throw new IOException( "Invalid additionalProperties: " + additionalPropertiesNode );
                }

                return additionalProperties;
            }


            private static ArrayNode deserializeArrayNode( JsonNode node, JsonNode itemsNode, JsonParser parser ) throws IOException {
                Node itemNode = parser.getCodec().treeToValue( itemsNode, Node.class );
                Integer minItems = readIntegerField( node, "minItems" );
                Integer maxItems = readIntegerField( node, "maxItems" );
                Boolean uniqueItems = node.has( "uniqueItems" ) ? node.get( "uniqueItems" ).asBoolean() : null;

                return new ArrayNode( itemNode, minItems, maxItems, uniqueItems );
            }


            private static ScalarNode deserializeScalarNode( JsonNode node ) throws IOException {
                JsonNode typeNode = node.get( "type" );
                if ( typeNode == null ) {
                    throw new IOException( "Scalar node requires 'type'" );
                }

                List<PolyType> types = readScalarTypes( typeNode );

                Integer minLength = readIntegerField( node, "minLength" );
                Integer maxLength = readIntegerField( node, "maxLength" );
                String pattern = node.has( "pattern" ) && node.get( "pattern" ).isTextual() ? node.get( "pattern" ).asText() : null;

                BigDecimal minimum = readDecimalField( node, "minimum" );
                BigDecimal maximum = readDecimalField( node, "maximum" );
                BigDecimal multipleOf = readDecimalField( node, "multipleOf" );

                JsonNode constValue = node.get( "const" );
                List<JsonNode> enumValues = readEnumValues( node );

                return new ScalarNode( types, minLength, maxLength, pattern, minimum, maximum, multipleOf, constValue, enumValues );
            }


            private static List<PolyType> readScalarTypes( JsonNode typeNode ) throws IOException {
                List<PolyType> types = new ArrayList<>();

                if ( typeNode.isTextual() ) {
                    types.add( JsonTypeTokens.toPolyType( typeNode.asText() ) );
                    return types;
                }

                if ( typeNode.isArray() ) {
                    for ( JsonNode typeElement : typeNode ) {
                        if ( !typeElement.isTextual() ) {
                            throw new IOException( "Scalar node requires textual type tokens" );
                        }
                        types.add( JsonTypeTokens.toPolyType( typeElement.asText() ) );
                    }
                    return types;
                }

                throw new IOException( "Scalar node requires textual or array 'type'" );
            }


            private static Integer readIntegerField( JsonNode node, String fieldName ) {
                return node.has( fieldName ) && node.get( fieldName ).canConvertToInt() ? node.get( fieldName ).intValue() : null;
            }


            private static BigDecimal readDecimalField( JsonNode node, String fieldName ) {
                return node.has( fieldName ) && node.get( fieldName ).isNumber() ? node.get( fieldName ).decimalValue() : null;
            }


            private static List<JsonNode> readEnumValues( JsonNode node ) {
                if ( !node.has( "enum" ) || !node.get( "enum" ).isArray() ) {
                    return null;
                }

                List<JsonNode> enumValues = new ArrayList<>();
                for ( JsonNode enumValue : node.get( "enum" ) ) {
                    enumValues.add( enumValue );
                }
                return enumValues;
            }

        }

    }

}