package org.polypheny.db.schema.document;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Strict resolver that ONLY accepts PolyType short-form:
 *
 *   { "validator": { "$jsonSchema": {
 *        "required": { "fieldA": "VARCHAR(30)", "fieldB": "INTEGER", ... },
 *        "additionalProperties": false|true
 *     }},
 *     "validationAction": "error" | "warn" | "strict" | "off"
 *   }
 *
 * Any other shape (required array, "properties" block, bsonType, shorthand outside $jsonSchema) is rejected.
 */
public final class SchemaOptionsResolver {

    private SchemaOptionsResolver() {}
    private static final ObjectMapper M = new ObjectMapper();

    public enum AlterMode { REPLACE, PATCH }

    public static final class Rename {
        public final String from;
        public final String to;
        public Rename(String from, String to) { this.from = from; this.to = to; }
        @Override public String toString() { return from + "→" + to; }
    }

    public static final class Coercion {
        public final String target;       // e.g. "INTEGER", "NUMBER", "DATE"
        public final String onFailure;    // "null" | "drop" | "error"
        public Coercion(String target, String onFailure) { this.target = target; this.onFailure = onFailure; }
    }

    public static final class Resolved {
        public final DocumentSchema schema;     // may be null (enforcement-only)
        public final EnforcementMode mode;      // may be null (keep previous)
        public final AlterMode alterMode;       // REPLACE or PATCH

        // migration hints (accepted, not executed here)
        public final List<Rename> renames;
        public final Map<String, JsonNode> defaults;
        public final Map<String, Coercion> coercions;
        public final boolean pruneExtras;
        public final boolean dryRun;

        public Resolved(DocumentSchema schema,
                EnforcementMode mode,
                AlterMode alterMode,
                List<Rename> renames,
                Map<String, JsonNode> defaults,
                Map<String, Coercion> coercions,
                boolean pruneExtras,
                boolean dryRun) {
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

    /** CREATE path (requires a schema). */
    public static Resolved resolve(String optionsJson) {
        var r = parseCommon(optionsJson, false);
        if (r.schema == null) throw new IllegalArgumentException("CREATE requires validator.$jsonSchema");
        return r;
    }

    /** ALTER path (schema optional, enforcement optional). */
    public static Resolved resolveAlter(String optionsJson) {
        return parseCommon(optionsJson, true);
    }

    private static Resolved parseCommon(String optionsJson, boolean schemaOptional) {
        if (optionsJson == null || optionsJson.isBlank()) {
            if (schemaOptional) {
                return new Resolved(null, null, AlterMode.REPLACE, List.of(), Map.of(), Map.of(), false, false);
            }
            throw new IllegalArgumentException("Missing options JSON.");
        }

        final ObjectNode root;
        try {
            JsonNode n = M.readTree(optionsJson);
            if (!(n instanceof ObjectNode r)) throw new IllegalArgumentException("Options JSON must be an object");
            root = r;
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid options JSON: " + e.getMessage(), e);
        }

        // validationAction
        EnforcementMode mode = null;
        if (root.has("validationAction")) {
            String s = root.get("validationAction").asText("");
            mode = switch (s.toLowerCase(Locale.ROOT)) {
                case "error", "strict" -> EnforcementMode.STRICT;
                case "warn"            -> EnforcementMode.WARN;
                case "off"             -> EnforcementMode.OFF;
                default                -> throw new IllegalArgumentException("Unknown validationAction: " + s);
            };
        }

        // alter mode
        AlterMode alterMode = AlterMode.REPLACE;
        if (root.has("mode")) {
            String m = root.get("mode").asText("");
            alterMode = "patch".equalsIgnoreCase(m) ? AlterMode.PATCH : AlterMode.REPLACE;
        }

        // migration hints (just parse & pass through)
        List<Rename> renames = new ArrayList<>();
        if (root.has("renames") && root.get("renames").isArray()) {
            for (JsonNode r : root.get("renames")) {
                if (r.has("from") && r.has("to")) {
                    renames.add(new Rename(r.get("from").asText(), r.get("to").asText()));
                }
            }
        }

        Map<String, JsonNode> defaults = new HashMap<>();
        if (root.has("defaults") && root.get("defaults").isObject()) {
            root.get("defaults").fields().forEachRemaining(e -> defaults.put(e.getKey(), e.getValue()));
        }

        Map<String, Coercion> coercions = new HashMap<>();
        if (root.has("coercions") && root.get("coercions").isObject()) {
            root.get("coercions").fields().forEachRemaining(e -> {
                String path = e.getKey();
                JsonNode spec = e.getValue();
                String target = spec.has("target") ? spec.get("target").asText() : "STRING";
                String onFail = spec.has("onFailure") ? spec.get("onFailure").asText("error") : "error";
                coercions.put(path, new Coercion(target, onFail));
            });
        }

        boolean pruneExtras = root.has("pruneExtras") && root.get("pruneExtras").asBoolean(false);
        boolean dryRun = root.has("dryRun") && root.get("dryRun").asBoolean(false);

        // validator.$jsonSchema (short form)
        DocumentSchema schema = null;
        if (root.has("validator") && root.get("validator").isObject()) {
            JsonNode validator = root.get("validator");
            if (validator.has("$jsonSchema") && validator.get("$jsonSchema").isObject()) {
                ObjectNode js = (ObjectNode) validator.get("$jsonSchema");
                schema = buildShortForm(js);
            } else if (!schemaOptional) {
                throw new IllegalArgumentException("Expected 'validator.$jsonSchema' object.");
            }
        } else if (!schemaOptional) {
            throw new IllegalArgumentException("Expected 'validator' object.");
        }

        return new Resolved(schema, mode, alterMode, renames, defaults, coercions, pruneExtras, dryRun);
    }

    // --- Short-form builder: required { path: "POLYTYPE" }, additionalProperties ---
    private static DocumentSchema buildShortForm(ObjectNode js) {
        if (js.has("properties")) {
            throw new IllegalArgumentException("Unsupported: '$jsonSchema.properties'. Use short-form 'required' map.");
        }
        if (!js.has("required") || !js.get("required").isObject()) {
            throw new IllegalArgumentException("Short-form requires '$jsonSchema.required' object mapping field -> PolyType string.");
        }

        ObjectNode reqObj = (ObjectNode) js.get("required");
        Set<String> required = new HashSet<>();
        Map<String, DocumentSchema.FieldType> types = new HashMap<>();

        reqObj.fields().forEachRemaining(e -> {
            String field = e.getKey();
            String typeText = e.getValue().asText();
            String base = PolyTypeView.parse(typeText).base();
            types.put(field, mapPolyTypeToFieldType(base));
            required.add(field);
        });

        DocumentSchema.AdditionalProperties ap = DocumentSchema.AdditionalProperties.ALLOW;
        if (js.has("additionalProperties")) {
            JsonNode apNode = js.get("additionalProperties");
            if (apNode.isBoolean()) {
                ap = apNode.asBoolean() ? DocumentSchema.AdditionalProperties.ALLOW : DocumentSchema.AdditionalProperties.FORBID;
            } else if (apNode.isTextual()) {
                ap = "FORBID".equalsIgnoreCase(apNode.asText())
                        ? DocumentSchema.AdditionalProperties.FORBID
                        : DocumentSchema.AdditionalProperties.ALLOW;
            }
        }

        return new DocumentSchema(required, types, ap);
    }

    private record PolyTypeView(String base) {
        private static final Pattern SIG = Pattern.compile("^\\s*([A-Za-z_][A-Za-z0-9_]*)\\s*(?:\\((\\d+)(?:,(\\d+))?\\))?\\s*$");
        static PolyTypeView parse(String typeText) {
            Matcher m = SIG.matcher(typeText == null ? "" : typeText.trim());
            return new PolyTypeView(m.matches() ? m.group(1).toUpperCase(Locale.ROOT)
                    : (typeText == null ? "" : typeText).toUpperCase(Locale.ROOT));
        }
    }

    private static DocumentSchema.FieldType mapPolyTypeToFieldType(String polyBase) {
        switch (polyBase) {
            case "BOOLEAN": return DocumentSchema.FieldType.BOOLEAN;
            case "TINYINT":
            case "SMALLINT":
            case "INTEGER":
            case "BIGINT":  return DocumentSchema.FieldType.INTEGER;
            case "REAL":
            case "FLOAT":
            case "DOUBLE":
            case "DECIMAL": return DocumentSchema.FieldType.NUMBER;
            case "CHAR":
            case "VARCHAR":
            case "TEXT":
            case "JSON":
            case "JSONB":   return DocumentSchema.FieldType.STRING;
            case "BINARY":
            case "VARBINARY": return DocumentSchema.FieldType.BINARY;
            case "DATE":    return DocumentSchema.FieldType.DATE;
            case "TIME":
            case "TIMESTAMP": return DocumentSchema.FieldType.TIMESTAMP;
            case "ARRAY":   return DocumentSchema.FieldType.ARRAY;
            case "OBJECT":
            case "MAP":     return DocumentSchema.FieldType.OBJECT;
            default:        return DocumentSchema.FieldType.STRING;
        }
    }
}
