package org.polypheny.db.schema.document;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.polypheny.db.type.PolyType;

public final class DocumentSchema {

    public enum AdditionalProperties { ALLOW, FORBID }

    //
    // Polymorphic Node with custom (de)serializer.
    //
    @JsonSerialize(using = NodeSerde.Serializer.class)
    @JsonDeserialize(using = NodeSerde.Deserializer.class)
    public sealed interface Node permits ObjectNode, ArrayNode, ScalarNode {}

    /** Scalar = just a PolyType. */
    public static final class ScalarNode implements Node {
        public final PolyType type;

        @JsonCreator
        public ScalarNode(@JsonProperty("type") PolyType type) {
            this.type = Objects.requireNonNull(type, "type");
        }
        public static ScalarNode of(PolyType t) { return new ScalarNode(t); }
        @Override public String toString() { return "Scalar(" + type + ")"; }
    }

    public static final class ArrayNode implements Node {
        public final Node items;
        public final Integer minItems;
        public final Boolean uniqueItems;

        @JsonCreator
        public ArrayNode(
                @JsonProperty("items") Node items,
                @JsonProperty("minItems") Integer minItems,
                @JsonProperty("uniqueItems") Boolean uniqueItems) {
            this.items = Objects.requireNonNull(items, "items");
            this.minItems = minItems;
            this.uniqueItems = uniqueItems;
        }
        @Override public String toString() { return "Array(items=" + items + ")"; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true) // tolerate legacy/extra fields
    public static final class ObjectNode implements Node {
        public final Map<String, Node> properties;
        public final AdditionalProperties additionalProperties;

        @JsonCreator
        public ObjectNode(
                @JsonProperty("properties") Map<String, Node> properties,
                @JsonProperty("additionalProperties") AdditionalProperties additionalProperties) {
            this.properties = properties == null ? Map.of() : Map.copyOf(properties);
            this.additionalProperties = additionalProperties == null
                    ? AdditionalProperties.FORBID   // exact-by-default
                    : additionalProperties;
        }
        @Override public String toString() { return "Object(props=" + properties.keySet() + ", add=" + additionalProperties + ")"; }
    }

    private final ObjectNode root;

    @JsonCreator
    public DocumentSchema(@JsonProperty("root") ObjectNode root) {
        this.root = Objects.requireNonNull(root, "root");
    }

    @JsonProperty("root")
    public ObjectNode root() { return root; }

    public void validateOrThrow() {
        validateObject(root);
    }

    private static void validateObject(ObjectNode obj) {
        for (Node n : obj.properties.values()) {
            if (n instanceof ObjectNode o) validateObject(o);
            else if (n instanceof ArrayNode a) validateArray(a);
            else if (n instanceof ScalarNode s) validateScalar(s);
        }
    }

    private static void validateArray(ArrayNode a) {
        if (a.items == null) throw new IllegalArgumentException("Schema invalid: array 'items' must be specified.");
        if (a.items instanceof ObjectNode o) validateObject(o);
        else if (a.items instanceof ArrayNode an) validateArray(an);
        else if (a.items instanceof ScalarNode s) validateScalar(s);
    }

    private static void validateScalar(ScalarNode s) {
        if (s.type == null) throw new IllegalArgumentException("Scalar type must be specified");
    }

    // ----------------------------------------------------------------------
    // Node (de)serializer – shape-based
    // ----------------------------------------------------------------------
    static final class NodeSerde {

        static final class Serializer extends JsonSerializer<Node> {
            @Override
            public void serialize(Node value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
                if (value instanceof ScalarNode s) {
                    gen.writeStartObject();
                    gen.writeStringField("type", s.type.name());
                    gen.writeEndObject();
                } else if (value instanceof ArrayNode a) {
                    gen.writeStartObject();
                    gen.writeFieldName("items");
                    serializers.defaultSerializeValue(a.items, gen);
                    if (a.minItems != null) gen.writeNumberField("minItems", a.minItems);
                    if (a.uniqueItems != null) gen.writeBooleanField("uniqueItems", a.uniqueItems);
                    gen.writeEndObject();
                } else if (value instanceof ObjectNode o) {
                    gen.writeStartObject();
                    gen.writeObjectFieldStart("properties");
                    for (Map.Entry<String, Node> e : o.properties.entrySet()) {
                        gen.writeFieldName(e.getKey());
                        serializers.defaultSerializeValue(e.getValue(), gen);
                    }
                    gen.writeEndObject(); // properties
                    // store as string for backwards compat (FORBID/ALLOW)
                    gen.writeStringField("additionalProperties", o.additionalProperties.name());
                    gen.writeEndObject();
                } else {
                    throw new IllegalStateException("Unknown node kind: " + value);
                }
            }
        }

        static final class Deserializer extends JsonDeserializer<Node> {
            @Override
            public Node deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
                JsonNode n = p.readValueAsTree();
                if (!n.isObject()) {
                    throw new IOException("DocumentSchema.Node must be a JSON object");
                }

                // Object node?
                JsonNode props = n.get("properties");
                if (props != null && props.isObject()) {
                    Map<String, Node> map = new LinkedHashMap<>();
                    for ( Iterator<Entry<String, JsonNode>> it = props.fields(); it.hasNext(); ) {
                        Map.Entry<String, JsonNode> e = it.next();
                        // recurse via this deserializer
                        Node child = p.getCodec().treeToValue(e.getValue(), Node.class);
                        map.put(e.getKey(), child);
                    }
                    AdditionalProperties ap = readAP(n.get("additionalProperties"));
                    return new ObjectNode(map, ap);
                }

                // Array node?
                JsonNode items = n.get("items");
                if (items != null) {
                    Node item = p.getCodec().treeToValue(items, Node.class);
                    Integer minItems = n.has("minItems") && n.get("minItems").canConvertToInt() ? n.get("minItems").intValue() : null;
                    Boolean unique = n.has("uniqueItems") ? n.get("uniqueItems").asBoolean() : null;
                    return new ArrayNode(item, minItems, unique);
                }

                // Scalar node
                JsonNode t = n.get("type");
                if (t == null || !t.isTextual()) {
                    throw new IOException("Scalar node requires textual 'type'");
                }
                PolyType pt = parsePolyTypeRelaxed(t.asText());
                return new ScalarNode(pt);
            }

            private static AdditionalProperties readAP(JsonNode ap) {
                if (ap == null) return AdditionalProperties.FORBID;
                if (ap.isTextual()) {
                    String s = ap.asText("");
                    return "ALLOW".equalsIgnoreCase(s) || "true".equalsIgnoreCase(s) ? AdditionalProperties.ALLOW : AdditionalProperties.FORBID;
                }
                if (ap.isBoolean()) {
                    return ap.asBoolean(false) ? AdditionalProperties.ALLOW : AdditionalProperties.FORBID;
                }
                return AdditionalProperties.FORBID;
            }

            private static PolyType parsePolyTypeRelaxed(String raw) {
                if (raw == null) return PolyType.ANY;
                String s = raw.trim();
                // strip legacy (precision, scale)
                int i = s.indexOf('(');
                if (i >= 0) s = s.substring(0, i);
                String t = s.toUpperCase( Locale.ROOT);

                // accept friendly tokens
                if (t.equals("TEXT") || t.equals("STRING")) return PolyType.TEXT;
                if (t.equals("NUMBER") || t.equals("NUMERIC")) return PolyType.DOUBLE;
                if (t.equals("BOOLEAN") || t.equals("BOOL")) return PolyType.BOOLEAN;
                if (t.equals("DATE")) return PolyType.DATE;
                if (t.equals("TIMESTAMP") || t.equals("DATETIME")) return PolyType.TIMESTAMP;
                if (t.equals("BINARY") || t.equals("BLOB")) return PolyType.BINARY;
                if (t.equals("ANY")) return PolyType.ANY;

                // legacy SQL-ish tokens map to closest PolyType
                switch (t) {
                    case "CHAR":
                    case "VARCHAR":
                    case "JSON":
                        return PolyType.TEXT;
                    case "TINYINT":
                    case "SMALLINT":
                    case "INT":
                    case "INTEGER":
                    case "BIGINT":
                        return PolyType.INTEGER;
                    case "DECIMAL":
                    case "FLOAT":
                    case "REAL":
                    case "DOUBLE":
                        return PolyType.DOUBLE;
                    case "VARBINARY":
                    case "FILE":
                    case "IMAGE":
                    case "VIDEO":
                    case "AUDIO":
                        return PolyType.BINARY;
                    case "TIME":
                        return PolyType.TIMESTAMP;
                }

                // last resort: PolyType.valueOf for exact enum names
                try {
                    return PolyType.valueOf(t);
                } catch (IllegalArgumentException iae) {
                    throw new IllegalArgumentException("Unknown scalar type token: " + raw);
                }
            }
        }
    }
}
