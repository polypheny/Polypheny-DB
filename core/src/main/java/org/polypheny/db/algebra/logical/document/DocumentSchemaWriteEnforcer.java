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

package org.polypheny.db.algebra.logical.document;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.document.DocumentSchema;
import org.polypheny.db.schema.document.DocumentSchema.AdditionalProperties;
import org.polypheny.db.schema.document.EnforcementMode;
import org.polypheny.db.schema.document.SchemaJson;
import org.polypheny.db.schema.document.SchemaMeta;
import org.polypheny.db.schema.document.SchemaValidator;
import org.polypheny.db.schema.document.SchemaValidator.ValidationResult;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DocumentSchemaWriteEnforcer {

    private static final Logger LOG = LoggerFactory.getLogger( DocumentSchemaWriteEnforcer.class );

    private static final int INSERT_SUMMARY_LIMIT = 3;
    private static final int MAX_LOG_DETAIL_LENGTH = 500;


    private DocumentSchemaWriteEnforcer() {
    }


    private record SchemaContext(DocumentSchema schema, EnforcementMode mode) {

    }


    private record ResolvedSchemaNode(DocumentSchema.Node node, AdditionalProperties inheritedAp) {

    }


    public static void enforce( Entity entity, AlgNode input, LogicalDocumentModify.Operation operation, Map<String, ? extends RexNode> updates, List<String> removes, Map<String, String> renames ) {

        Optional<SchemaContext> schemaContext = loadSchemaContext( entity );
        if ( schemaContext.isEmpty() ) {
            return;
        }

        SchemaContext context = schemaContext.get();
        DocumentSchema schema = context.schema();
        EnforcementMode mode = context.mode();

        if ( operation == LogicalDocumentModify.Operation.INSERT ) {
            preflightLiteralInsert( input, entity, schema, mode );
        }

        if ( operation == LogicalDocumentModify.Operation.UPDATE && updates != null && !updates.isEmpty() ) {
            validateUpdateTypes( entity, schema, mode, updates );
        }

        validateUpdateSpec( entity, schema, mode, updates, removes, renames );
        validateRequiredFieldsNotRemoved( entity, schema, mode, removes, renames );
    }


    /**
     * Schema metadata is stored for logical collections, so this resolves from the planner entity
     * back to the logical collection id when possible.
     */
    private static Optional<SchemaContext> loadSchemaContext( Entity entity ) {
        if ( entity == null ) {
            return Optional.empty();
        }

        Long namespaceId = tryGetNamespaceId( entity );
        if ( namespaceId == null ) {
            return Optional.empty();
        }

        String adjustedName = adjustNameForNamespace( entity.getName(), namespaceId );

        Catalog catalog = Catalog.getInstance();
        Optional<LogicalCollection> logicalCollection = catalog.getSnapshot().doc().getCollection( namespaceId, adjustedName );

        if ( logicalCollection.isPresent() ) {
            return loadFromIds( namespaceId, logicalCollection.get().id );
        }

        if ( entity instanceof LogicalCollection collection ) {
            return loadFromIds( collection.namespaceId, collection.id );
        }

        Long entityId = tryGetId( entity );
        if ( entityId != null ) {
            return loadFromIds( namespaceId, entityId );
        }

        return Optional.empty();
    }


    private static Optional<SchemaContext> loadFromIds( long namespaceId, long collectionId ) {
        Optional<SchemaMeta> schemaMeta = SchemaMeta.readCurrent( Catalog.getInstance(), namespaceId, collectionId );

        if ( schemaMeta.isEmpty() ) {
            return Optional.empty();
        }

        SchemaMeta meta = schemaMeta.get();
        if ( meta.schemaJson == null || meta.schemaJson.isBlank() ) {
            return Optional.empty();
        }

        EnforcementMode mode = resolveMode( meta );
        if ( mode == EnforcementMode.OFF ) {
            return Optional.empty();
        }

        DocumentSchema schema = parseSchemaOrThrow( meta.schemaJson );
        return Optional.of( new SchemaContext( schema, mode ) );
    }


    private static String adjustNameForNamespace( String name, long namespaceId ) {
        Catalog catalog = Catalog.getInstance();
        var namespace = catalog.getSnapshot().getNamespace( namespaceId ).orElseThrow();
        return namespace.caseSensitive ? name : name.toLowerCase( Locale.ROOT );
    }


    private static Long tryGetNamespaceId( Entity entity ) {
        Optional<Long> namespaceId = readLong( entity, "namespaceId", "getNamespaceId" );
        if ( namespaceId.isPresent() ) {
            return namespaceId.get();
        }

        if ( entity instanceof LogicalCollection collection ) {
            return collection.namespaceId;
        }

        return null;
    }


    private static Long tryGetId( Entity entity ) {
        return readLong( entity, "id", "getId" ).orElse( null );
    }


    private static Optional<Long> readLong( Object target, String... candidates ) {
        Class<?> targetClass = target.getClass();

        for ( String candidate : candidates ) {
            try {
                java.lang.reflect.Field field = targetClass.getDeclaredField( candidate );
                field.setAccessible( true );
                Object value = field.get( target );

                if ( value instanceof Number number ) {
                    return Optional.of( number.longValue() );
                }
            } catch ( Exception ignored ) {
                // Ignore and try the next candidate.
            }

            try {
                java.lang.reflect.Method method = targetClass.getMethod( candidate );
                Object value = method.invoke( target );

                if ( value instanceof Number number ) {
                    return Optional.of( number.longValue() );
                }
            } catch ( Exception ignored ) {
                // Ignore and try the next candidate.
            }
        }

        return Optional.empty();
    }


    private static EnforcementMode resolveMode( SchemaMeta meta ) {
        try {
            String mode = meta.enforcement == null ? "OFF" : meta.enforcement;
            return EnforcementMode.valueOf( mode.trim().toUpperCase( Locale.ROOT ) );
        } catch ( IllegalArgumentException ignored ) {
            return EnforcementMode.OFF;
        }
    }


    private static DocumentSchema parseSchemaOrThrow( String json ) {
        try {
            DocumentSchema schema = SchemaJson.parse( json );
            schema.validateOrThrow();
            return schema;
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Stored collection schema is invalid", e );
        }
    }


    /**
     * This only validates literal INSERT values visible at planning time.
     * Non-literal sources still need runtime enforcement.
     */
    private static void preflightLiteralInsert( AlgNode input, Entity entity, DocumentSchema schema, EnforcementMode mode ) {

        if ( mode == EnforcementMode.OFF ) {
            return;
        }

        if ( !(input instanceof LogicalDocumentValues values) ) {
            return;
        }

        List<PolyDocument> documents = values.getDocuments();
        for ( PolyDocument document : documents ) {
            BsonDocument rawDocument = BsonDocument.parse( document.toJson() );
            BsonDocument documentToCheck = stripIdForValidation( rawDocument );

            ValidationResult validationResult = SchemaValidator.validate( schema, documentToCheck );
            if ( !validationResult.ok() ) {
                String message = "Inserted document does not conform to the collection schema: " + validationResult.compactSummary( INSERT_SUMMARY_LIMIT );
                handleViolation( mode, message, entity.getName(), rawDocument );
            }
        }
    }


    private static BsonDocument stripIdForValidation( BsonDocument document ) {
        if ( document == null ) {
            return null;
        }

        if ( !document.containsKey( DocumentType.DOCUMENT_ID ) ) {
            return document;
        }

        BsonDocument clone = document.clone();
        clone.remove( DocumentType.DOCUMENT_ID );
        return clone;
    }


    private static void validateUpdateSpec( Entity entity, DocumentSchema schema, EnforcementMode mode, Map<String, ? extends RexNode> updates, List<String> removes, Map<String, String> renames ) {

        if ( schema.additionalProperties() == AdditionalProperties.ALLOW ) {
            return;
        }

        var allowedTopLevelFields = schema.root().properties.keySet();

        if ( updates != null ) {
            for ( String path : updates.keySet() ) {
                String topLevelField = topLevelSegment( path );
                if ( !allowedTopLevelFields.contains( topLevelField ) ) {
                    String message = "Update touches undeclared field '" + topLevelField + "'";
                    handleViolation( mode, message, entity.getName(), path );
                }
            }
        }

        if ( removes != null ) {
            for ( String path : removes ) {
                String topLevelField = topLevelSegment( path );
                if ( !allowedTopLevelFields.contains( topLevelField ) ) {
                    String message = "Remove touches undeclared field '" + topLevelField + "'";
                    handleViolation( mode, message, entity.getName(), path );
                }
            }
        }

        if ( renames != null ) {
            for ( Map.Entry<String, String> entry : renames.entrySet() ) {
                String sourceTopLevelField = topLevelSegment( entry.getKey() );
                String targetTopLevelField = topLevelSegment( entry.getValue() );

                if ( !allowedTopLevelFields.contains( sourceTopLevelField ) || !allowedTopLevelFields.contains( targetTopLevelField ) ) {
                    String message = "Rename between undeclared fields '" + sourceTopLevelField + "' -> '" + targetTopLevelField + "'";
                    handleViolation( mode, message, entity.getName(), entry );
                }
            }
        }
    }


    private static void validateRequiredFieldsNotRemoved( Entity entity, DocumentSchema schema, EnforcementMode mode, List<String> removes, Map<String, String> renames ) {

        if ( mode == EnforcementMode.OFF ) {
            return;
        }

        var requiredTopLevelFields = schema.root().effectiveRequired();

        if ( removes != null ) {
            for ( String path : removes ) {
                String topLevelField = topLevelSegment( path );
                if ( requiredTopLevelFields.contains( topLevelField ) ) {
                    String message = "Update removes required field '" + topLevelField + "'";
                    handleViolation( mode, message, entity.getName(), path );
                }
            }
        }

        if ( renames != null ) {
            for ( Map.Entry<String, String> entry : renames.entrySet() ) {
                String sourceTopLevelField = topLevelSegment( entry.getKey() );
                if ( requiredTopLevelFields.contains( sourceTopLevelField ) ) {
                    String message = "Update renames required field '" + sourceTopLevelField + "'";
                    handleViolation( mode, message, entity.getName(), entry );
                }
            }
        }
    }


    private static void validateUpdateTypes( Entity entity, DocumentSchema schema, EnforcementMode mode, Map<String, ? extends RexNode> updates ) {

        for ( Map.Entry<String, ? extends RexNode> entry : updates.entrySet() ) {
            String path = entry.getKey();
            RexNode expression = entry.getValue();

            if ( expression == null ) {
                continue;
            }

            Optional<ResolvedSchemaNode> resolvedSchemaNode = resolveNode( schema, path );
            if ( resolvedSchemaNode.isEmpty() ) {
                continue;
            }

            ResolvedSchemaNode resolvedNode = resolvedSchemaNode.get();
            DocumentSchema.Node schemaNode = resolvedNode.node();
            String updateOperator = inferUpdateOperator( expression );

            if ( "$set".equals( updateOperator ) ) {
                Optional<BsonValue> literalValue = tryExtractLiteralBsonValue( expression );
                if ( literalValue.isPresent() ) {
                    ValidationResult validationResult = SchemaValidator.validateNodeValue( schemaNode, literalValue.get(), resolvedNode.inheritedAp() );

                    if ( !validationResult.ok() ) {
                        String message = "Update value for field '" + path + "' does not conform to the collection schema: " + validationResult.compactSummary( INSERT_SUMMARY_LIMIT );
                        handleViolation( mode, message, entity.getName(), path );
                    }
                    continue;
                }
            }

            if ( !(schemaNode instanceof DocumentSchema.ScalarNode scalarNode) ) {
                continue;
            }

            List<PolyType> expectedTypes = scalarNode.types;
            PolyType actualType = inferScalarType( expression );

            if ( actualType == PolyType.DOCUMENT && isNumericOperator( updateOperator ) && expression instanceof RexCall call ) {
                actualType = inferNumericOperandType( call ).orElse( actualType );
            }

            if ( isNumericOperator( updateOperator ) ) {
                boolean targetNumeric = expectedTypes != null && expectedTypes.stream().anyMatch( DocumentSchemaWriteEnforcer::isNumericType );

                if ( !targetNumeric ) {
                    String message = "Update operator " + updateOperator + " cannot be applied to non-numeric field '" + path + "' (schema expects one of " + expectedTypes + ")";
                    handleViolation( mode, message, entity.getName(), path );
                    continue;
                }

                boolean operandNumeric;
                if ( expression instanceof RexCall call ) {
                    operandNumeric = inferNumericOperandType( call ).isPresent();
                } else {
                    operandNumeric = actualType != null && isNumericType( actualType );
                }

                if ( !operandNumeric ) {
                    String message = "Update operator " + updateOperator + " for field '" + path + "' requires a numeric value" + ", but expression is typed " + actualType;
                    handleViolation( mode, message, entity.getName(), path );
                    continue;
                }
            }

            if ( actualType != null && !isCompatibleScalarType( expression, actualType, expectedTypes ) ) {
                String message = "Update expression for field '" + path + "' has type " + actualType + ", but schema expects one of " + expectedTypes + " (operator: " + updateOperator + ")";
                handleViolation( mode, message, entity.getName(), path );
            }
        }
    }


    private static String inferUpdateOperator( RexNode expression ) {
        if ( !(expression instanceof RexCall call) || call.getOperator() == null || call.getOperator().getName() == null ) {
            return "$set";
        }

        String operatorName = call.getOperator().getName().trim().toLowerCase( Locale.ROOT );

        if ( operatorName.contains( "mql_update_min" ) || operatorName.equals( "$min" ) ) {
            return "$min";
        }
        if ( operatorName.contains( "mql_update_max" ) || operatorName.equals( "$max" ) ) {
            return "$max";
        }
        if ( operatorName.equals( "+" ) || operatorName.contains( "plus" ) || operatorName.contains( "add" ) ) {
            return "$inc";
        }
        if ( operatorName.equals( "*" ) || operatorName.contains( "multiply" ) || operatorName.contains( "times" ) ) {
            return "$mul";
        }

        return "$set";
    }


    private static boolean isNumericOperator( String updateOperator ) {
        return "$inc".equals( updateOperator ) || "$mul".equals( updateOperator ) || "$min".equals( updateOperator ) || "$max".equals( updateOperator );
    }


    private static PolyType inferScalarType( RexNode expression ) {
        if ( expression == null ) {
            return null;
        }

        PolyType type = null;
        try {
            type = expression.getType() != null ? expression.getType().getPolyType() : null;
        } catch ( Exception ignored ) {
            // Keep null.
        }

        if ( type == PolyType.DOCUMENT && expression instanceof RexLiteral literal ) {
            PolyValue value = literal.getValue();

            if ( value == null || value.isNull() ) {
                return PolyType.NULL;
            }

            if ( !value.isDocument() ) {
                PolyType valueType = value.getType();
                return switch ( valueType ) {
                    case CHAR, VARCHAR, TEXT -> PolyType.TEXT;
                    default -> valueType;
                };
            }

            return PolyType.DOCUMENT;
        }

        return type;
    }


    private static Optional<PolyType> inferNumericOperandType( RexCall call ) {
        for ( RexNode operand : call.getOperands() ) {
            PolyType operandType = inferScalarType( operand );
            if ( operandType != null && isNumericType( operandType ) ) {
                return Optional.of( operandType );
            }
        }

        return Optional.empty();
    }


    private static boolean isCompatibleScalarType( RexNode expression, PolyType actualType, List<PolyType> expectedTypes ) {

        if ( actualType == PolyType.DOCUMENT ) {
            return !(expression instanceof RexLiteral);
        }

        if ( expectedTypes == null || expectedTypes.isEmpty() ) {
            return true;
        }

        boolean allowsNull = expectedTypes.contains( PolyType.NULL );
        boolean allowsText = expectedTypes.stream().anyMatch( DocumentSchemaWriteEnforcer::isTextType );
        boolean allowsNumeric = expectedTypes.stream().anyMatch( DocumentSchemaWriteEnforcer::isNumericType );
        boolean allowsBoolean = expectedTypes.contains( PolyType.BOOLEAN );

        if ( actualType == PolyType.NULL ) {
            return allowsNull;
        }
        if ( isTextType( actualType ) ) {
            return allowsText;
        }
        if ( isNumericType( actualType ) ) {
            return allowsNumeric;
        }
        if ( actualType == PolyType.BOOLEAN ) {
            return allowsBoolean;
        }

        return expectedTypes.contains( actualType );
    }


    private static boolean isTextType( PolyType type ) {
        if ( type == null ) {
            return false;
        }

        return switch ( type ) {
            case TEXT, VARCHAR, CHAR -> true;
            default -> false;
        };
    }


    private static boolean isNumericType( PolyType type ) {
        if ( type == null ) {
            return false;
        }

        return switch ( type ) {
            case TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL, REAL, FLOAT, DOUBLE -> true;
            default -> false;
        };
    }


    private static String topLevelSegment( String path ) {
        if ( path == null ) {
            return "";
        }

        int dotIndex = path.indexOf( '.' );
        return dotIndex < 0 ? path : path.substring( 0, dotIndex );
    }


    /**
     * Resolves a dotted schema path through object properties and array items.
     */
    private static Optional<ResolvedSchemaNode> resolveNode( DocumentSchema schema, String path ) {
        DocumentSchema.Node currentNode = schema.root();
        AdditionalProperties inheritedAdditionalProperties = schema.additionalProperties() != null ? schema.additionalProperties() : AdditionalProperties.ALLOW;

        if ( path == null || path.isEmpty() ) {
            return Optional.empty();
        }

        String[] segments = path.split( "\\." );
        for ( String segment : segments ) {
            if ( currentNode instanceof DocumentSchema.ObjectNode objectNode ) {
                AdditionalProperties effectiveAdditionalProperties = effectiveAdditionalProperties( objectNode.additionalProperties, inheritedAdditionalProperties );

                DocumentSchema.Node nextNode = objectNode.properties.get( segment );
                if ( nextNode == null ) {
                    return Optional.empty();
                }

                currentNode = nextNode;
                inheritedAdditionalProperties = effectiveAdditionalProperties;
                continue;
            }

            if ( currentNode instanceof DocumentSchema.ArrayNode arrayNode ) {
                currentNode = arrayNode.items;

                if ( isNumericPathSegment( segment ) ) {
                    continue;
                }

                if ( currentNode instanceof DocumentSchema.ObjectNode objectNode ) {
                    AdditionalProperties effectiveAdditionalProperties = effectiveAdditionalProperties( objectNode.additionalProperties, inheritedAdditionalProperties );

                    DocumentSchema.Node nextNode = objectNode.properties.get( segment );
                    if ( nextNode == null ) {
                        return Optional.empty();
                    }

                    currentNode = nextNode;
                    inheritedAdditionalProperties = effectiveAdditionalProperties;
                    continue;
                }

                return Optional.empty();
            }

            return Optional.empty();
        }

        return Optional.of( new ResolvedSchemaNode( currentNode, inheritedAdditionalProperties ) );
    }


    private static AdditionalProperties effectiveAdditionalProperties( AdditionalProperties nodeAdditionalProperties, AdditionalProperties inheritedAdditionalProperties ) {

        if ( nodeAdditionalProperties == null || nodeAdditionalProperties == AdditionalProperties.INHERIT ) {
            return inheritedAdditionalProperties;
        }

        return nodeAdditionalProperties;
    }


    private static boolean isNumericPathSegment( String segment ) {
        if ( segment == null || segment.isEmpty() ) {
            return false;
        }

        for ( int i = 0; i < segment.length(); i++ ) {
            char ch = segment.charAt( i );
            if ( ch < '0' || ch > '9' ) {
                return false;
            }
        }

        return true;
    }


    private static Optional<BsonValue> tryExtractLiteralBsonValue( RexNode expression ) {
        if ( !(expression instanceof RexLiteral literal) ) {
            return Optional.empty();
        }

        PolyValue value = literal.getValue();
        if ( value == null || value.isNull() ) {
            return Optional.of( BsonNull.VALUE );
        }

        try {
            String json = value.toJson();
            BsonDocument wrapper = BsonDocument.parse( "{\"v\":" + json + "}" );
            return Optional.ofNullable( wrapper.get( "v" ) );
        } catch ( Exception ignored ) {
            return Optional.empty();
        }
    }


    private static void handleViolation( EnforcementMode mode, String message, String entityName, Object detail ) {

        switch ( mode ) {
            case STRICT -> throw new GenericRuntimeException( message );
            case WARN -> {
                if ( LOG.isWarnEnabled() ) {
                    LOG.warn( "{}; allowed due to WARN. Entity='{}' Detail={}", message, entityName, summarize( detail ) );
                }
            }
            case OFF -> {
                // Filtered earlier.
            }
        }
    }


    private static String summarize( Object value ) {
        try {
            String summary = String.valueOf( value );
            return summary.length() > MAX_LOG_DETAIL_LENGTH ? summary.substring( 0, MAX_LOG_DETAIL_LENGTH ) + "…" : summary;
        } catch ( Exception ignored ) {
            return "<unprintable>";
        }
    }

}