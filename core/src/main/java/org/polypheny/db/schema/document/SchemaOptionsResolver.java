package org.polypheny.db.schema.document;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;

public final class SchemaOptionsResolver {

    private SchemaOptionsResolver() {}
    private static final ObjectMapper M = new ObjectMapper();

    public enum AlterMode { REPLACE, PATCH }

    public static final class Rename { public final String from,to; public Rename(String f,String t){from=f;to=t;} @Override public String toString(){return from+"→"+to;} }
    public static final class Coercion { public final String target,onFailure; public Coercion(String t,String o){target=t;onFailure=o;} }

    public static final class Resolved {
        public final DocumentSchema schema;
        public final EnforcementMode mode;
        public final AlterMode alterMode;
        public final List<Rename> renames;
        public final Map<String, JsonNode> defaults;
        public final Map<String, Coercion> coercions;
        public final boolean pruneExtras, dryRun;

        public Resolved(DocumentSchema s, EnforcementMode m, AlterMode a, List<Rename> r,
                Map<String, JsonNode> d, Map<String, Coercion> c, boolean p, boolean dr) {
            schema=s; mode=m; alterMode=a; renames=r; defaults=d; coercions=c; pruneExtras=p; dryRun=dr;
        }
    }

    public static Resolved resolve(PolyValue options) {
        var r = parseCommon(options, false);
        if (r.schema == null) throw new IllegalArgumentException("CREATE requires 'docSchema' object.");
        return r;
    }
    public static Resolved resolveAlter(PolyValue options) { return parseCommon(options, true); }

    private static ObjectNode requireObjectNode(JsonNode first) {
        JsonNode n = first;
        // unwrap up to 3 times if it's a JSON string containing JSON
        for (int i = 0; i < 3 && n != null; i++) {
            if (n instanceof ObjectNode obj) return obj;
            if (n.isTextual()) {
                try {
                    n = M.readTree(n.asText());
                    continue;
                } catch (Exception ignored) {
                    break;
                }
            }
            break;
        }
        throw new IllegalArgumentException("Options must be a JSON object.");
    }

    private static Resolved parseCommon(PolyValue options, boolean schemaOptional) {
        final ObjectNode root;
        if (options == null) {
            if (schemaOptional) return new Resolved(null, null, AlterMode.REPLACE, List.of(), Map.of(), Map.of(), false, false);
            throw new IllegalArgumentException("Missing options.");
        }
        try {
            JsonNode raw = M.readTree(options.toJson());
            root = requireObjectNode(raw);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid options payload: " + e.getMessage(), e);
        }

        if (root.has("validator") || root.has("$jsonSchema"))
            throw new IllegalArgumentException("Use 'docSchema' instead of 'validator.$jsonSchema'.");

        EnforcementMode mode = null;
        if (root.has("validationAction")) {
            String s = root.get("validationAction").asText("");
            mode = switch (s.toLowerCase(Locale.ROOT)) {
                case "error", "strict" -> EnforcementMode.STRICT;
                case "warn"            -> EnforcementMode.WARN;
                case "off"             -> EnforcementMode.OFF;
                default -> throw new IllegalArgumentException("Unknown validationAction: " + s);
            };
        }

        AlterMode alterMode = AlterMode.REPLACE;
        if (root.has("mode"))
            alterMode = "patch".equalsIgnoreCase(root.get("mode").asText("")) ? AlterMode.PATCH : AlterMode.REPLACE;

        List<Rename> renames = new ArrayList<>();
        if (root.has("renames") && root.get("renames").isArray()) {
            for (JsonNode r : root.get("renames"))
                if (r.has("from") && r.has("to")) renames.add(new Rename(r.get("from").asText(), r.get("to").asText()));
        }

        Map<String, JsonNode> defaults = new HashMap<>();
        if (root.has("defaults") && root.get("defaults").isObject())
            root.get("defaults").fields().forEachRemaining(e -> defaults.put(e.getKey(), e.getValue()));

        Map<String, Coercion> coercions = new HashMap<>();
        if (root.has("coercions") && root.get("coercions").isObject()) {
            root.get("coercions").fields().forEachRemaining(e -> {
                String path = e.getKey();
                JsonNode spec = e.getValue();
                coercions.put(path, new Coercion(
                        spec.has("target") ? spec.get("target").asText() : "text",
                        spec.has("onFailure") ? spec.get("onFailure").asText("error") : "error"));
            });
        }

        boolean pruneExtras = root.has("pruneExtras") && root.get("pruneExtras").asBoolean(false);
        boolean dryRun      = root.has("dryRun")      && root.get("dryRun").asBoolean(false);

        DocumentSchema schema = null;
        if (root.has("docSchema")) {
            JsonNode ds = root.get("docSchema");
            if (!ds.isObject()) throw new IllegalArgumentException("'docSchema' must be an object");
            schema = new DocumentSchema(readObjectNode((ObjectNode) ds));
        } else if (!schemaOptional) {
            throw new IllegalArgumentException("Missing 'docSchema'.");
        }

        return new Resolved(schema, mode, alterMode, renames, defaults, coercions, pruneExtras, dryRun);
    }

    // ---------- Recursive readers ----------

    private static DocumentSchema.ObjectNode readObjectNode(ObjectNode objSpec) {
        if (objSpec.has("type")) {
            String t = objSpec.get("type").asText("").trim().toLowerCase(Locale.ROOT);
            if (!t.isEmpty() && !t.equals("object"))
                throw new IllegalArgumentException("Object node expected, found type: " + t);
        }

        if (objSpec.has("required"))
            throw new IllegalArgumentException("This dialect does not support 'required'. All declared properties are required.");

        Map<String, DocumentSchema.Node> props = new LinkedHashMap<>();
        if (objSpec.has("properties")) {
            JsonNode propsNode = objSpec.get("properties");
            if (!propsNode.isObject()) throw new IllegalArgumentException("'properties' must be an object");
            propsNode.fields().forEachRemaining(e -> props.put(e.getKey(), readNode(e.getValue())));
        }

        DocumentSchema.AdditionalProperties ap = readAP(objSpec);
        return new DocumentSchema.ObjectNode(props, ap);
    }

    private static DocumentSchema.Node readNode(JsonNode spec) {
        if (spec.isTextual()) {
            PolyType pt = mapInputTypeToPoly(spec.asText());
            return new DocumentSchema.ScalarNode(pt);
        }
        if (!spec.isObject()) throw new IllegalArgumentException("Property spec must be string or object");

        ObjectNode o = (ObjectNode) spec;
        if (o.has("type") && o.get("type").isTextual()) {
            String typeText = o.get("type").asText().trim().toLowerCase(Locale.ROOT);
            if (typeText.equals("object")) return readObjectNode(o);
            if (typeText.equals("array"))  return readArrayNode(o);
            PolyType pt = mapInputTypeToPoly(typeText);
            return new DocumentSchema.ScalarNode(pt);
        }

        if (o.has("properties")) return readObjectNode(o);
        throw new IllegalArgumentException("Missing or unsupported 'type' in property spec: " + o);
    }

    private static DocumentSchema.ArrayNode readArrayNode(ObjectNode arrSpec) {
        if (!arrSpec.has("items")) throw new IllegalArgumentException("Array spec requires 'items'");
        DocumentSchema.Node items = readNode(arrSpec.get("items"));
        Integer minItems = arrSpec.has("minItems") ? arrSpec.get("minItems").asInt() : null;
        Boolean unique   = arrSpec.has("uniqueItems") ? arrSpec.get("uniqueItems").asBoolean() : null;
        return new DocumentSchema.ArrayNode(items, minItems, unique);
    }

    private static DocumentSchema.AdditionalProperties readAP(ObjectNode o) {
        if (!o.has("additionalProperties")) return DocumentSchema.AdditionalProperties.FORBID;
        JsonNode n = o.get("additionalProperties");
        if (n.isBoolean()) return n.asBoolean() ? DocumentSchema.AdditionalProperties.ALLOW : DocumentSchema.AdditionalProperties.FORBID;
        if (n.isTextual()) return "FORBID".equalsIgnoreCase(n.asText()) ? DocumentSchema.AdditionalProperties.FORBID : DocumentSchema.AdditionalProperties.ALLOW;
        throw new IllegalArgumentException("'additionalProperties' must be boolean or 'FORBID'/'ALLOW'");
    }

    // ---------- Mapping from friendly tokens (and legacy) to PolyType ----------

    private static PolyType mapInputTypeToPoly(String raw) {
        if (raw == null) return PolyType.ANY;
        String s = raw.trim();
        // strip any legacy (p,s) suffix: VARCHAR(50), DECIMAL(10,2), etc.
        int paren = s.indexOf('(');
        if (paren >= 0) s = s.substring(0, paren);
        String t = s.toLowerCase(Locale.ROOT);

        // Friendly dialect
        switch (t) {
            case "text":
            case "string":
                return PolyType.TEXT;

            case "number":
            case "numeric":
                // choose a wide numeric so validator accepts int/long/double
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
        switch (t) {
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

            // floating / decimal -> keep wide to allow ints as well
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

        throw new IllegalArgumentException("Unknown type token: " + raw);
    }
}
