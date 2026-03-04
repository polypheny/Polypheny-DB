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
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.Decimal128;
import org.polypheny.db.schema.document.DocumentSchema.AllOfNode;
import org.polypheny.db.schema.document.DocumentSchema.AnyOfNode;
import org.polypheny.db.schema.document.DocumentSchema.ArrayNode;
import org.polypheny.db.schema.document.DocumentSchema.Node;
import org.polypheny.db.schema.document.DocumentSchema.NotNode;
import org.polypheny.db.schema.document.DocumentSchema.ObjectNode;
import org.polypheny.db.schema.document.DocumentSchema.OneOfNode;
import org.polypheny.db.schema.document.DocumentSchema.ScalarNode;
import org.polypheny.db.type.PolyType;

/**
 * Validates BSON documents against a {@link DocumentSchema}.
 *
 * <p>Supports:
 * required, per-object additionalProperties, scalar union types, anyOf/oneOf/allOf/not,
 * and extended constraints (enum/const/multipleOf/maxItems/min/maxProperties).</p>
 */
public final class SchemaValidator {

    private SchemaValidator() {
    }

    public record Violation( String path, String code, String message ) { }

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
        DocumentSchema.AdditionalProperties rootAp =
                schema.additionalProperties() != null ? schema.additionalProperties() : DocumentSchema.AdditionalProperties.ALLOW;
        validateNode( "$", schema.root(), doc, out, rootAp );
        return new ValidationResult( out.isEmpty(), out );
    }

    public static boolean conformsTo( DocumentSchema schema, BsonDocument doc ) {
        return validate( schema, doc ).ok();
    }

    // -----------------------------------------------------------------------------------------
    // Generic dispatch
    // -----------------------------------------------------------------------------------------

    private static void validateNode(
            String path,
            Node schemaNode,
            BsonValue value,
            List<Violation> out,
            DocumentSchema.AdditionalProperties inheritedAp ) {

        if ( schemaNode instanceof ObjectNode on ) {
            if ( !(value instanceof BsonDocument bd) ) {
                out.add( v( path, "TYPE", "Expected object, got " + bsonTypeName( value ) ) );
            } else {
                validateObject(path, on, bd, out, inheritedAp);
            }
            return;
        }

        if ( schemaNode instanceof ArrayNode an ) {
            if ( !(value instanceof BsonArray ba) ) {
                out.add( v( path, "TYPE", "Expected array, got " + bsonTypeName( value ) ) );
            } else {
                validateArray(path, an, ba, out, inheritedAp);
            }
            return;
        }

        if ( schemaNode instanceof ScalarNode sn ) {
            validateScalar(path, sn, value, out);
            return;
        }

        if ( schemaNode instanceof AnyOfNode ao ) {
            validateAnyOf(path, ao, value, out, inheritedAp);
            return;
        }

        if ( schemaNode instanceof OneOfNode oo ) {
            validateOneOf(path, oo, value, out, inheritedAp);
            return;
        }

        if ( schemaNode instanceof AllOfNode al ) {
            validateAllOf(path, al, value, out, inheritedAp);
            return;
        }

        if ( schemaNode instanceof NotNode nn ) {
            validateNot(path, nn, value, out, inheritedAp);
            return;
        }

        out.add( v( path, "INTERNAL", "Unknown schema node" ) );
    }

    // -----------------------------------------------------------------------------------------
    // Object
    // -----------------------------------------------------------------------------------------

    private static void validateObject(
            String path,
            ObjectNode schemaNode,
            BsonDocument doc,
            List<Violation> out,
            DocumentSchema.AdditionalProperties inheritedAp ) {

        DocumentSchema.AdditionalProperties effectiveAp =
                (schemaNode.additionalProperties == null || schemaNode.additionalProperties == DocumentSchema.AdditionalProperties.INHERIT)
                        ? inheritedAp
                        : schemaNode.additionalProperties;

        // min/maxProperties apply to the actual object size (including extras)
        if ( schemaNode.minProperties != null && doc.size() < schemaNode.minProperties ) {
            out.add( v( path, "MIN_PROPERTIES", "Expected at least " + schemaNode.minProperties + " properties" ) );
        }
        if ( schemaNode.maxProperties != null && doc.size() > schemaNode.maxProperties ) {
            out.add( v( path, "MAX_PROPERTIES", "Expected at most " + schemaNode.maxProperties + " properties" ) );
        }

        // required (if omitted -> dialect default = all declared properties required)
        Set<String> required = schemaNode.effectiveRequired();
        for ( String key : required ) {
            if ( !doc.containsKey( key ) ) {
                out.add( v( pathDot(path, key), "REQUIRED_MISSING", "Required field is missing" ) );
            }
        }

        // Validate declared properties when present
        for ( Map.Entry<String, Node> e : schemaNode.properties.entrySet() ) {
            String key = e.getKey();
            Node child = e.getValue();
            if ( !doc.containsKey(key) ) {
                continue; // optional missing OK
            }
            BsonValue bv = doc.get(key);
            validateNode( pathDot(path, key), child, bv, out, effectiveAp );
        }

        // additional properties
        if ( effectiveAp == DocumentSchema.AdditionalProperties.FORBID ) {
            for ( String k : doc.keySet() ) {
                if ( !schemaNode.properties.containsKey( k ) ) {
                    out.add( v( pathDot( path, k ), "ADDITIONAL_PROPERTY", "Unexpected field" ) );
                }
            }
        }
    }

    // -----------------------------------------------------------------------------------------
    // Array
    // -----------------------------------------------------------------------------------------

    private static void validateArray(
            String path,
            ArrayNode schema,
            BsonArray arr,
            List<Violation> out,
            DocumentSchema.AdditionalProperties inheritedAp ) {

        if ( schema.minItems != null && arr.size() < schema.minItems ) {
            out.add( v( path, "MIN_ITEMS", "Expected at least " + schema.minItems + " items" ) );
        }
        if ( schema.maxItems != null && arr.size() > schema.maxItems ) {
            out.add( v( path, "MAX_ITEMS", "Expected at most " + schema.maxItems + " items" ) );
        }
        if ( Boolean.TRUE.equals( schema.uniqueItems ) ) {
            Set<String> uniq = new HashSet<>();
            for ( int i = 0; i < arr.size(); i++ ) {
                String key = bsonToJsonNode(arr.get(i)).toString();
                if ( !uniq.add( key ) ) {
                    out.add( v( pathDot( path, Integer.toString( i ) ), "UNIQUE", "Duplicate array item" ) );
                }
            }
        }

        for ( int i = 0; i < arr.size(); i++ ) {
            BsonValue v = arr.get( i );
            String ip = pathDot( path, Integer.toString( i ) );
            validateNode( ip, schema.items, v, out, inheritedAp );
        }
    }

    // -----------------------------------------------------------------------------------------
    // Scalar
    // -----------------------------------------------------------------------------------------

    private static void validateScalar(
            String path,
            ScalarNode schema,
            BsonValue v,
            List<Violation> out ) {

        if ( v == null ) {
            out.add( v( path, "TYPE", "Value is null" ) );
            return;
        }

        // type check (union)
        if ( !matchesAnyType(v, schema.types) ) {
            out.add( v( path, "TYPE_MISMATCH", "Expected " + schema.types + " but got " + bsonTypeName( v ) ) );
            return;
        }

        // const/enum check first (works for all scalar types, including null)
        if ( schema.constValue != null ) {
            JsonNode inst = bsonToJsonNode(v);
            if ( !schema.constValue.equals(inst) ) {
                out.add( v(path, "CONST", "Value does not match const") );
                return;
            }
        }

        if ( schema.enumValues != null ) {
            JsonNode inst = bsonToJsonNode(v);
            boolean ok = false;
            for ( JsonNode allowed : schema.enumValues ) {
                if ( allowed.equals(inst) ) {
                    ok = true;
                    break;
                }
            }
            if ( !ok ) {
                out.add( v(path, "ENUM", "Value is not in enum") );
                return;
            }
        }

        // ---- String constraints (only if instance is string) ----
        if ( (schema.minLength != null || schema.maxLength != null || schema.pattern != null) && (v instanceof BsonString bs) ) {
            String s = bs.getValue();
            int len = s.codePointCount(0, s.length());

            if ( schema.minLength != null && len < schema.minLength ) {
                out.add(v(path, "MIN_LENGTH", "Expected length >= " + schema.minLength + " but was " + len));
            }

            if ( schema.maxLength != null && len > schema.maxLength ) {
                out.add(v(path, "MAX_LENGTH", "Expected length <= " + schema.maxLength + " but was " + len));
            }

            if ( schema.pattern != null ) {
                if (!Pattern.compile(schema.pattern).matcher(s).find()) {
                    out.add(v(path, "PATTERN", "Value does not match pattern"));
                }
            }
        }

        // ---- Numeric constraints (only if instance is numeric) ----
        if ( schema.minimum != null || schema.maximum != null || schema.multipleOf != null ) {
            BigDecimal num = asBigDecimal(v);
            if ( num != null ) {
                if ( schema.minimum != null && num.compareTo(schema.minimum) < 0 ) {
                    out.add(v(path, "MINIMUM", "Expected >= " + schema.minimum + " but was " + num));
                }
                if ( schema.maximum != null && num.compareTo(schema.maximum) > 0 ) {
                    out.add(v(path, "MAXIMUM", "Expected <= " + schema.maximum + " but was " + num));
                }
                if ( schema.multipleOf != null ) {
                    if ( !isMultipleOf(num, schema.multipleOf) ) {
                        out.add(v(path, "MULTIPLE_OF", "Expected multipleOf " + schema.multipleOf + " but was " + num));
                    }
                }
            }
        }
    }

    private static boolean matchesAnyType(BsonValue v, List<PolyType> allowed) {
        if ( allowed == null || allowed.isEmpty() ) {
            return true;
        }
        for ( PolyType t : allowed ) {
            if ( matchesPolyType(v, t) ) {
                return true;
            }
        }
        return false;
    }

    private static boolean matchesPolyType(BsonValue v, PolyType t) {
        if ( t == PolyType.NULL ) {
            return v == null || v.isNull();
        }
        return JsonTypeTokens.matchesJson(v, t);
    }

    private static boolean isMultipleOf(BigDecimal value, BigDecimal step) {
        if ( step == null || step.compareTo(BigDecimal.ZERO) == 0 ) {
            return true;
        }

        // Try exact remainder first
        try {
            BigDecimal rem = value.remainder(step);
            if ( rem.compareTo(BigDecimal.ZERO) == 0 ) {
                return true;
            }
        } catch ( ArithmeticException ignored ) {
            // fall through
        }

        // Fallback: scale-aware comparison
        int scale = Math.max(value.scale(), step.scale());
        BigDecimal scaledValue = value.setScale(scale, RoundingMode.HALF_UP);
        BigDecimal scaledStep = step.setScale(scale, RoundingMode.HALF_UP);
        BigDecimal rem2 = scaledValue.remainder(scaledStep);
        return rem2.compareTo(BigDecimal.ZERO) == 0;
    }

    private static BigDecimal asBigDecimal(BsonValue v) {
        if ( v instanceof BsonInt32 i ) {
            return BigDecimal.valueOf(i.getValue());
        }
        if ( v instanceof BsonInt64 l ) {
            return BigDecimal.valueOf(l.getValue());
        }
        if ( v instanceof BsonDouble d ) {
            return BigDecimal.valueOf(d.getValue());
        }
        if ( v instanceof BsonDecimal128 dec ) {
            Decimal128 d = dec.getValue();
            return d == null ? null : d.bigDecimalValue();
        }
        return null;
    }

    // -----------------------------------------------------------------------------------------
    // Composition
    // -----------------------------------------------------------------------------------------

    private static void validateAnyOf(String path, AnyOfNode node, BsonValue value, List<Violation> out, DocumentSchema.AdditionalProperties inheritedAp) {
        for ( Node opt : node.anyOf ) {
            List<Violation> tmp = new ArrayList<>();
            validateNode(path, opt, value, tmp, inheritedAp);
            if ( tmp.isEmpty() ) {
                return; // anyOf: first success is enough
            }
        }
        out.add(v(path, "ANY_OF", "Value does not match anyOf options"));
    }

    private static void validateOneOf(String path, OneOfNode node, BsonValue value, List<Violation> out, DocumentSchema.AdditionalProperties inheritedAp) {
        int ok = 0;
        for ( Node opt : node.oneOf ) {
            List<Violation> tmp = new ArrayList<>();
            validateNode(path, opt, value, tmp, inheritedAp);
            if ( tmp.isEmpty() ) {
                ok++;
            }
        }
        if ( ok != 1 ) {
            out.add(v(path, "ONE_OF", "Value must match exactly one option but matched " + ok));
        }
    }

    private static void validateAllOf(String path, AllOfNode node, BsonValue value, List<Violation> out, DocumentSchema.AdditionalProperties inheritedAp) {
        for ( Node opt : node.allOf ) {
            validateNode(path, opt, value, out, inheritedAp);
        }
    }

    private static void validateNot(String path, NotNode node, BsonValue value, List<Violation> out, DocumentSchema.AdditionalProperties inheritedAp) {
        List<Violation> tmp = new ArrayList<>();
        validateNode(path, node.not, value, tmp, inheritedAp);
        if ( tmp.isEmpty() ) {
            out.add(v(path, "NOT", "Value must not match schema"));
        }
    }

    // -----------------------------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------------------------

    private static Violation v( String path, String code, String msg ) {
        return new Violation( path, code, msg );
    }

    private static String pathDot( String base, String next ) {
        return base.equals( "$" ) ? "$." + next : base + "." + next;
    }

    private static String bsonTypeName( BsonValue v ) {
        return v == null ? "NULL" : v.getBsonType().name();
    }

    /**
     * Converts a BsonValue to a Jackson JsonNode for enum/const comparisons.
     * Covers the most common BSON scalar types + objects/arrays.
     */
    private static JsonNode bsonToJsonNode(BsonValue v) {
        if ( v == null || v instanceof BsonNull || v.isNull() ) {
            return JsonNodeFactory.instance.nullNode();
        }
        if ( v instanceof BsonString s ) {
            return JsonNodeFactory.instance.textNode(s.getValue());
        }
        if ( v instanceof BsonBoolean b ) {
            return JsonNodeFactory.instance.booleanNode(b.getValue());
        }
        if ( v instanceof BsonInt32 i ) {
            return JsonNodeFactory.instance.numberNode(i.getValue());
        }
        if ( v instanceof BsonInt64 l ) {
            return JsonNodeFactory.instance.numberNode(l.getValue());
        }
        if ( v instanceof BsonDouble d ) {
            return JsonNodeFactory.instance.numberNode(d.getValue());
        }
        if (v instanceof BsonDecimal128 dec) {
            Decimal128 d = dec.getValue();
            BigDecimal bd = (d == null) ? BigDecimal.ZERO : d.bigDecimalValue();
            return JsonNodeFactory.instance.numberNode(bd);
        }
        if ( v instanceof BsonDateTime dt ) {
            return JsonNodeFactory.instance.numberNode(dt.getValue());
        }
        if ( v instanceof BsonObjectId oid ) {
            return JsonNodeFactory.instance.textNode(oid.getValue().toHexString());
        }
        if ( v instanceof BsonBinary bin ) {
            return JsonNodeFactory.instance.binaryNode(bin.getData());
        }
        if ( v instanceof BsonArray arr ) {
            com.fasterxml.jackson.databind.node.ArrayNode an = JsonNodeFactory.instance.arrayNode();
            for ( BsonValue el : arr ) {
                an.add(bsonToJsonNode(el));
            }
            return an;
        }
        if ( v instanceof BsonDocument doc ) {
            com.fasterxml.jackson.databind.node.ObjectNode on = JsonNodeFactory.instance.objectNode();
            for ( String k : doc.keySet() ) {
                on.set(k, bsonToJsonNode(doc.get(k)));
            }
            return on;
        }
        return JsonNodeFactory.instance.textNode(String.valueOf(v));
    }

    /**
     * Validate a JSON string against a {@link DocumentSchema}.
     *
     * @param stripId if true, removes "_id" before validation (mirrors insert enforcement behavior)
     */
    public static ValidationResult validateJson( DocumentSchema schema, String json, boolean stripId ) {
        BsonDocument doc = BsonDocument.parse( json );
        if ( stripId && doc.containsKey( "_id" ) ) {
            BsonDocument clone = doc.clone();
            clone.remove( "_id" );
            doc = clone;
        }
        return validate( schema, doc );
    }

    /** Validate JSON string, stripping "_id" by default. */
    public static ValidationResult validateJson( DocumentSchema schema, String json ) {
        return validateJson( schema, json, true );
    }

    /** Validate a Jackson JsonNode, stripping "_id" by default. */
    public static ValidationResult validateJson( DocumentSchema schema, JsonNode jsonNode ) {
        return validateJson( schema, jsonNode.toString(), true );
    }

}
