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
 * Validates BSON documents against a DocumentSchema.
 */
public final class SchemaValidator {

    private SchemaValidator() {
    }


    public record Violation(String path, String code, String message) {

    }


    public record ValidationResult(boolean ok, List<Violation> violations) {

        public String compactSummary( int maxItems ) {
            if ( ok || violations.isEmpty() ) {
                return "ok";
            }

            int effectiveMaxItems = Math.max( 1, maxItems );

            String summary = violations.stream().limit( effectiveMaxItems ).map( violation -> violation.code + "@" + violation.path + "(" + violation.message + ")" ).collect( Collectors.joining( "; " ) );

            if ( violations.size() <= effectiveMaxItems ) {
                return summary;
            }

            return summary + " … +" + (violations.size() - effectiveMaxItems) + " more";
        }

    }


    public static ValidationResult validate( DocumentSchema schema, BsonDocument doc ) {
        List<Violation> violations = new ArrayList<>();
        DocumentSchema.AdditionalProperties inheritedAdditionalProperties = defaultAdditionalProperties( schema.additionalProperties() );

        validateNode( "$", schema.root(), doc, violations, inheritedAdditionalProperties );
        return new ValidationResult( violations.isEmpty(), violations );
    }


    public static boolean conformsTo( DocumentSchema schema, BsonDocument doc ) {
        return validate( schema, doc ).ok();
    }


    public static ValidationResult validateNodeValue( Node node, BsonValue value, DocumentSchema.AdditionalProperties inheritedAdditionalProperties ) {

        List<Violation> violations = new ArrayList<>();
        DocumentSchema.AdditionalProperties effectiveInheritedAdditionalProperties = defaultAdditionalProperties( inheritedAdditionalProperties );

        validateNode( "$", node, value, violations, effectiveInheritedAdditionalProperties );
        return new ValidationResult( violations.isEmpty(), violations );
    }


    private static void validateNode( String path, Node schemaNode, BsonValue value, List<Violation> violations, DocumentSchema.AdditionalProperties inheritedAdditionalProperties ) {

        if ( schemaNode instanceof ObjectNode objectNode ) {
            validateObjectNode( path, objectNode, value, violations, inheritedAdditionalProperties );
        } else if ( schemaNode instanceof ArrayNode arrayNode ) {
            validateArrayNode( path, arrayNode, value, violations, inheritedAdditionalProperties );
        } else if ( schemaNode instanceof ScalarNode scalarNode ) {
            validateScalar( path, scalarNode, value, violations );
        } else if ( schemaNode instanceof AnyOfNode anyOfNode ) {
            validateAnyOf( path, anyOfNode, value, violations, inheritedAdditionalProperties );
        } else if ( schemaNode instanceof OneOfNode oneOfNode ) {
            validateOneOf( path, oneOfNode, value, violations, inheritedAdditionalProperties );
        } else if ( schemaNode instanceof AllOfNode allOfNode ) {
            validateAllOf( path, allOfNode, value, violations, inheritedAdditionalProperties );
        } else if ( schemaNode instanceof NotNode notNode ) {
            validateNot( path, notNode, value, violations, inheritedAdditionalProperties );
        } else {
            violations.add( violation( path, "INTERNAL", "Unknown schema node" ) );
        }
    }


    private static void validateObjectNode( String path, ObjectNode schemaNode, BsonValue value, List<Violation> violations, DocumentSchema.AdditionalProperties inheritedAdditionalProperties ) {

        if ( value instanceof BsonDocument bsonDocument ) {
            validateObject( path, schemaNode, bsonDocument, violations, inheritedAdditionalProperties );
            return;
        }

        violations.add( violation( path, "TYPE", "Expected object, got " + bsonTypeName( value ) ) );
    }


    private static void validateArrayNode( String path, ArrayNode schemaNode, BsonValue value, List<Violation> violations, DocumentSchema.AdditionalProperties inheritedAdditionalProperties ) {

        if ( value instanceof BsonArray bsonArray ) {
            validateArray( path, schemaNode, bsonArray, violations, inheritedAdditionalProperties );
            return;
        }

        violations.add( violation( path, "TYPE", "Expected array, got " + bsonTypeName( value ) ) );
    }


    private static void validateObject( String path, ObjectNode schemaNode, BsonDocument document, List<Violation> violations, DocumentSchema.AdditionalProperties inheritedAdditionalProperties ) {

        DocumentSchema.AdditionalProperties effectiveAdditionalProperties = resolveAdditionalProperties( schemaNode.additionalProperties, inheritedAdditionalProperties );

        validateObjectSize( path, schemaNode, document, violations );
        validateRequiredProperties( path, schemaNode, document, violations );
        validateDeclaredProperties( path, schemaNode, document, violations, effectiveAdditionalProperties );

        if ( effectiveAdditionalProperties == DocumentSchema.AdditionalProperties.FORBID ) {
            validateForbiddenAdditionalProperties( path, schemaNode, document, violations );
        }
    }


    private static void validateObjectSize( String path, ObjectNode schemaNode, BsonDocument document, List<Violation> violations ) {

        if ( schemaNode.minProperties != null && document.size() < schemaNode.minProperties ) {
            violations.add( violation( path, "MIN_PROPERTIES", "Expected at least " + schemaNode.minProperties + " properties" ) );
        }

        if ( schemaNode.maxProperties != null && document.size() > schemaNode.maxProperties ) {
            violations.add( violation( path, "MAX_PROPERTIES", "Expected at most " + schemaNode.maxProperties + " properties" ) );
        }
    }


    private static void validateRequiredProperties( String path, ObjectNode schemaNode, BsonDocument document, List<Violation> violations ) {

        Set<String> requiredProperties = schemaNode.effectiveRequired();

        for ( String key : requiredProperties ) {
            if ( !document.containsKey( key ) ) {
                violations.add( violation( pathDot( path, key ), "REQUIRED_MISSING", "Required field is missing" ) );
            }
        }
    }


    private static void validateDeclaredProperties( String path, ObjectNode schemaNode, BsonDocument document, List<Violation> violations, DocumentSchema.AdditionalProperties inheritedAdditionalProperties ) {

        for ( Map.Entry<String, Node> entry : schemaNode.properties.entrySet() ) {
            String key = entry.getKey();

            if ( !document.containsKey( key ) ) {
                continue;
            }

            Node childNode = entry.getValue();
            BsonValue childValue = document.get( key );

            validateNode( pathDot( path, key ), childNode, childValue, violations, inheritedAdditionalProperties );
        }
    }


    private static void validateForbiddenAdditionalProperties( String path, ObjectNode schemaNode, BsonDocument document, List<Violation> violations ) {

        for ( String key : document.keySet() ) {
            if ( !schemaNode.properties.containsKey( key ) ) {
                violations.add( violation( pathDot( path, key ), "ADDITIONAL_PROPERTY", "Unexpected field" ) );
            }
        }
    }


    private static void validateArray( String path, ArrayNode schemaNode, BsonArray array, List<Violation> violations, DocumentSchema.AdditionalProperties inheritedAdditionalProperties ) {

        validateArraySize( path, schemaNode, array, violations );

        if ( Boolean.TRUE.equals( schemaNode.uniqueItems ) ) {
            validateUniqueItems( path, array, violations );
        }

        validateArrayItems( path, schemaNode, array, violations, inheritedAdditionalProperties );
    }


    private static void validateArraySize( String path, ArrayNode schemaNode, BsonArray array, List<Violation> violations ) {

        if ( schemaNode.minItems != null && array.size() < schemaNode.minItems ) {
            violations.add( violation( path, "MIN_ITEMS", "Expected at least " + schemaNode.minItems + " items" ) );
        }

        if ( schemaNode.maxItems != null && array.size() > schemaNode.maxItems ) {
            violations.add( violation( path, "MAX_ITEMS", "Expected at most " + schemaNode.maxItems + " items" ) );
        }
    }


    private static void validateUniqueItems( String path, BsonArray array, List<Violation> violations ) {

        Set<String> uniqueValues = new HashSet<>();

        for ( int i = 0; i < array.size(); i++ ) {
            String key = bsonToJsonNode( array.get( i ) ).toString();

            if ( !uniqueValues.add( key ) ) {
                violations.add( violation( pathDot( path, Integer.toString( i ) ), "UNIQUE", "Duplicate array item" ) );
            }
        }
    }


    private static void validateArrayItems( String path, ArrayNode schemaNode, BsonArray array, List<Violation> violations, DocumentSchema.AdditionalProperties inheritedAdditionalProperties ) {

        for ( int i = 0; i < array.size(); i++ ) {
            String itemPath = pathDot( path, Integer.toString( i ) );
            BsonValue itemValue = array.get( i );

            validateNode( itemPath, schemaNode.items, itemValue, violations, inheritedAdditionalProperties );
        }
    }


    private static void validateScalar( String path, ScalarNode schemaNode, BsonValue value, List<Violation> violations ) {

        if ( value == null ) {
            violations.add( violation( path, "TYPE", "Value is null" ) );
            return;
        }

        if ( !matchesAnyType( value, schemaNode.types ) ) {
            violations.add( violation( path, "TYPE_MISMATCH", "Expected " + schemaNode.types + " but got " + bsonTypeName( value ) ) );
            return;
        }

        if ( !matchesConstValue( schemaNode, value ) ) {
            violations.add( violation( path, "CONST", "Value does not match const" ) );
            return;
        }

        if ( !matchesEnumValue( schemaNode, value ) ) {
            violations.add( violation( path, "ENUM", "Value is not in enum" ) );
            return;
        }

        validateStringConstraints( path, schemaNode, value, violations );
        validateNumericConstraints( path, schemaNode, value, violations );
    }


    private static boolean matchesConstValue( ScalarNode schemaNode, BsonValue value ) {
        if ( schemaNode.constValue == null ) {
            return true;
        }

        JsonNode instanceValue = bsonToJsonNode( value );
        return schemaNode.constValue.equals( instanceValue );
    }


    private static boolean matchesEnumValue( ScalarNode schemaNode, BsonValue value ) {
        if ( schemaNode.enumValues == null ) {
            return true;
        }

        JsonNode instanceValue = bsonToJsonNode( value );

        for ( JsonNode allowedValue : schemaNode.enumValues ) {
            if ( allowedValue.equals( instanceValue ) ) {
                return true;
            }
        }

        return false;
    }


    private static void validateStringConstraints( String path, ScalarNode schemaNode, BsonValue value, List<Violation> violations ) {

        boolean hasStringConstraints = schemaNode.minLength != null || schemaNode.maxLength != null || schemaNode.pattern != null;

        if ( !hasStringConstraints || !(value instanceof BsonString bsonString) ) {
            return;
        }

        String stringValue = bsonString.getValue();
        int length = stringValue.codePointCount( 0, stringValue.length() );

        if ( schemaNode.minLength != null && length < schemaNode.minLength ) {
            violations.add( violation( path, "MIN_LENGTH", "Expected length >= " + schemaNode.minLength + " but was " + length ) );
        }

        if ( schemaNode.maxLength != null && length > schemaNode.maxLength ) {
            violations.add( violation( path, "MAX_LENGTH", "Expected length <= " + schemaNode.maxLength + " but was " + length ) );
        }

        if ( schemaNode.pattern != null && !Pattern.compile( schemaNode.pattern ).matcher( stringValue ).find() ) {
            violations.add( violation( path, "PATTERN", "Value does not match pattern" ) );
        }
    }


    private static void validateNumericConstraints( String path, ScalarNode schemaNode, BsonValue value, List<Violation> violations ) {

        boolean hasNumericConstraints = schemaNode.minimum != null || schemaNode.maximum != null || schemaNode.multipleOf != null;

        if ( !hasNumericConstraints ) {
            return;
        }

        BigDecimal numericValue = asBigDecimal( value );
        if ( numericValue == null ) {
            return;
        }

        if ( schemaNode.minimum != null && numericValue.compareTo( schemaNode.minimum ) < 0 ) {
            violations.add( violation( path, "MINIMUM", "Expected >= " + schemaNode.minimum + " but was " + numericValue ) );
        }

        if ( schemaNode.maximum != null && numericValue.compareTo( schemaNode.maximum ) > 0 ) {
            violations.add( violation( path, "MAXIMUM", "Expected <= " + schemaNode.maximum + " but was " + numericValue ) );
        }

        if ( schemaNode.multipleOf != null && !isMultipleOf( numericValue, schemaNode.multipleOf ) ) {
            violations.add( violation( path, "MULTIPLE_OF", "Expected multipleOf " + schemaNode.multipleOf + " but was " + numericValue ) );
        }
    }


    private static boolean matchesAnyType( BsonValue value, List<PolyType> allowedTypes ) {
        if ( allowedTypes == null || allowedTypes.isEmpty() ) {
            return true;
        }

        for ( PolyType allowedType : allowedTypes ) {
            if ( matchesPolyType( value, allowedType ) ) {
                return true;
            }
        }

        return false;
    }


    private static boolean matchesPolyType( BsonValue value, PolyType polyType ) {
        if ( polyType == PolyType.NULL ) {
            return value == null || value.isNull();
        }

        return JsonTypeTokens.matchesJson( value, polyType );
    }


    private static boolean isMultipleOf( BigDecimal value, BigDecimal step ) {
        if ( step == null || step.compareTo( BigDecimal.ZERO ) == 0 ) {
            return true;
        }

        try {
            BigDecimal remainder = value.remainder( step );
            if ( remainder.compareTo( BigDecimal.ZERO ) == 0 ) {
                return true;
            }
        } catch ( ArithmeticException ignored ) {
            // Fall through to the scaled comparison below.
        }

        int scale = Math.max( value.scale(), step.scale() );
        BigDecimal scaledValue = value.setScale( scale, RoundingMode.HALF_UP );
        BigDecimal scaledStep = step.setScale( scale, RoundingMode.HALF_UP );
        BigDecimal scaledRemainder = scaledValue.remainder( scaledStep );

        return scaledRemainder.compareTo( BigDecimal.ZERO ) == 0;
    }


    private static BigDecimal asBigDecimal( BsonValue value ) {
        if ( value instanceof BsonInt32 bsonInt32 ) {
            return BigDecimal.valueOf( bsonInt32.getValue() );
        }

        if ( value instanceof BsonInt64 bsonInt64 ) {
            return BigDecimal.valueOf( bsonInt64.getValue() );
        }

        if ( value instanceof BsonDouble bsonDouble ) {
            return BigDecimal.valueOf( bsonDouble.getValue() );
        }

        if ( value instanceof BsonDecimal128 bsonDecimal128 ) {
            Decimal128 decimal128 = bsonDecimal128.getValue();
            return decimal128 == null ? null : decimal128.bigDecimalValue();
        }

        return null;
    }


    private static void validateAnyOf( String path, AnyOfNode schemaNode, BsonValue value, List<Violation> violations, DocumentSchema.AdditionalProperties inheritedAdditionalProperties ) {

        for ( Node option : schemaNode.anyOf ) {
            List<Violation> optionViolations = new ArrayList<>();
            validateNode( path, option, value, optionViolations, inheritedAdditionalProperties );

            if ( optionViolations.isEmpty() ) {
                return;
            }
        }

        violations.add( violation( path, "ANY_OF", "Value does not match anyOf options" ) );
    }


    private static void validateOneOf( String path, OneOfNode schemaNode, BsonValue value, List<Violation> violations, DocumentSchema.AdditionalProperties inheritedAdditionalProperties ) {

        int matchCount = 0;

        for ( Node option : schemaNode.oneOf ) {
            List<Violation> optionViolations = new ArrayList<>();
            validateNode( path, option, value, optionViolations, inheritedAdditionalProperties );

            if ( optionViolations.isEmpty() ) {
                matchCount++;
            }
        }

        if ( matchCount != 1 ) {
            violations.add( violation( path, "ONE_OF", "Value must match exactly one option but matched " + matchCount ) );
        }
    }


    private static void validateAllOf( String path, AllOfNode schemaNode, BsonValue value, List<Violation> violations, DocumentSchema.AdditionalProperties inheritedAdditionalProperties ) {

        for ( Node option : schemaNode.allOf ) {
            validateNode( path, option, value, violations, inheritedAdditionalProperties );
        }
    }


    private static void validateNot( String path, NotNode schemaNode, BsonValue value, List<Violation> violations, DocumentSchema.AdditionalProperties inheritedAdditionalProperties ) {

        List<Violation> nestedViolations = new ArrayList<>();
        validateNode( path, schemaNode.not, value, nestedViolations, inheritedAdditionalProperties );

        if ( nestedViolations.isEmpty() ) {
            violations.add( violation( path, "NOT", "Value must not match schema" ) );
        }
    }


    private static DocumentSchema.AdditionalProperties defaultAdditionalProperties(
            DocumentSchema.AdditionalProperties additionalProperties ) {

        return additionalProperties != null ? additionalProperties : DocumentSchema.AdditionalProperties.ALLOW;
    }


    private static DocumentSchema.AdditionalProperties resolveAdditionalProperties( DocumentSchema.AdditionalProperties localAdditionalProperties, DocumentSchema.AdditionalProperties inheritedAdditionalProperties ) {

        if ( localAdditionalProperties == null || localAdditionalProperties == DocumentSchema.AdditionalProperties.INHERIT ) {
            return inheritedAdditionalProperties;
        }

        return localAdditionalProperties;
    }


    private static Violation violation( String path, String code, String message ) {
        return new Violation( path, code, message );
    }


    private static String pathDot( String base, String next ) {
        return base.equals( "$" ) ? "$." + next : base + "." + next;
    }


    private static String bsonTypeName( BsonValue value ) {
        return value == null ? "NULL" : value.getBsonType().name();
    }


    /**
     * Converts a BSON value to a Jackson JsonNode for enum and const comparisons.
     */
    private static JsonNode bsonToJsonNode( BsonValue value ) {
        if ( value == null || value instanceof BsonNull || value.isNull() ) {
            return JsonNodeFactory.instance.nullNode();
        }

        if ( value instanceof BsonString bsonString ) {
            return JsonNodeFactory.instance.textNode( bsonString.getValue() );
        }

        if ( value instanceof BsonBoolean bsonBoolean ) {
            return JsonNodeFactory.instance.booleanNode( bsonBoolean.getValue() );
        }

        if ( value instanceof BsonInt32 bsonInt32 ) {
            return JsonNodeFactory.instance.numberNode( bsonInt32.getValue() );
        }

        if ( value instanceof BsonInt64 bsonInt64 ) {
            return JsonNodeFactory.instance.numberNode( bsonInt64.getValue() );
        }

        if ( value instanceof BsonDouble bsonDouble ) {
            return JsonNodeFactory.instance.numberNode( bsonDouble.getValue() );
        }

        if ( value instanceof BsonDecimal128 bsonDecimal128 ) {
            Decimal128 decimal128 = bsonDecimal128.getValue();
            BigDecimal decimalValue = decimal128 == null ? BigDecimal.ZERO : decimal128.bigDecimalValue();
            return JsonNodeFactory.instance.numberNode( decimalValue );
        }

        if ( value instanceof BsonDateTime bsonDateTime ) {
            return JsonNodeFactory.instance.numberNode( bsonDateTime.getValue() );
        }

        if ( value instanceof BsonObjectId bsonObjectId ) {
            return JsonNodeFactory.instance.textNode( bsonObjectId.getValue().toHexString() );
        }

        if ( value instanceof BsonBinary bsonBinary ) {
            return JsonNodeFactory.instance.binaryNode( bsonBinary.getData() );
        }

        if ( value instanceof BsonArray bsonArray ) {
            com.fasterxml.jackson.databind.node.ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();

            for ( BsonValue element : bsonArray ) {
                arrayNode.add( bsonToJsonNode( element ) );
            }

            return arrayNode;
        }

        if ( value instanceof BsonDocument bsonDocument ) {
            com.fasterxml.jackson.databind.node.ObjectNode objectNode = JsonNodeFactory.instance.objectNode();

            for ( String key : bsonDocument.keySet() ) {
                objectNode.set( key, bsonToJsonNode( bsonDocument.get( key ) ) );
            }

            return objectNode;
        }

        return JsonNodeFactory.instance.textNode( String.valueOf( value ) );
    }


    /**
     * Validates a JSON string against a DocumentSchema.
     */
    public static ValidationResult validateJson( DocumentSchema schema, String json, boolean stripId ) {
        BsonDocument document = BsonDocument.parse( json );

        if ( stripId && document.containsKey( "_id" ) ) {
            BsonDocument clone = document.clone();
            clone.remove( "_id" );
            document = clone;
        }

        return validate( schema, document );
    }


    public static ValidationResult validateJson( DocumentSchema schema, JsonNode jsonNode ) {
        return validateJson( schema, jsonNode.toString(), true );
    }

}