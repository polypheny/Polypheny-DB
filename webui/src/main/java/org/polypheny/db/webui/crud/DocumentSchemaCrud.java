package org.polypheny.db.webui.crud;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.javalin.http.Context;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.schema.document.DocumentSchema;
import org.polypheny.db.schema.document.EnforcementMode;
import org.polypheny.db.schema.document.SchemaJson;
import org.polypheny.db.schema.document.SchemaMeta;
import org.polypheny.db.schema.document.SchemaValidator;

/**
 * WebUI helper for the /validateDocuments route in HttpServer.
 * Validates JSON documents against the stored collection schema without inserting them.
 */
@Slf4j
public class DocumentSchemaCrud {

    private static final String OFF = "OFF";


    private DocumentSchemaCrud() {
    }


    public static final class ValidateDocumentsRequest {

        public String namespace;
        public String collection;
        public JsonNode documents;
        public Integer maxViolationsPerDocument;

    }


    /**
     * Plain DTO used for JSON serialization.
     * Do not expose SchemaValidator.Violation directly because the WebUI Jackson mapper
     * disables getter-based visibility and serializes Java records as empty objects.
     */
    public static final class ViolationDto {

        public String path;
        public String code;
        public String message;


        public ViolationDto() {
        }


        public ViolationDto( String path, String code, String message ) {
            this.path = path;
            this.code = code;
            this.message = message;
        }


        public static ViolationDto from( SchemaValidator.Violation violation ) {
            if ( violation == null ) {
                return new ViolationDto( "$", "UNKNOWN", "Unknown violation" );
            }
            return new ViolationDto( violation.path(), violation.code(), violation.message() );
        }

    }


    public static final class DocResult {

        public int index;
        public boolean ok;
        public List<ViolationDto> violations;
        public String parseError;


        public DocResult() {
        }


        public DocResult(
                int index,
                boolean ok,
                List<ViolationDto> violations,
                String parseError ) {
            this.index = index;
            this.ok = ok;
            this.violations = violations;
            this.parseError = parseError;
        }

    }


    public static final class ValidateDocumentsResponse {

        public boolean ok;
        public boolean allowed;
        public boolean hasSchema;
        public String enforcement;
        public List<DocResult> results;
        public String error;


        public ValidateDocumentsResponse() {
        }


        public ValidateDocumentsResponse(
                boolean ok,
                boolean allowed,
                boolean hasSchema,
                String enforcement,
                List<DocResult> results,
                String error ) {
            this.ok = ok;
            this.allowed = allowed;
            this.hasSchema = hasSchema;
            this.enforcement = enforcement;
            this.results = results;
            this.error = error;
        }

    }


    /**
     * Validates one or more JSON documents against the stored collection schema.
     */
    public static void validateDocuments( Context ctx ) {
        ValidateDocumentsRequest request;

        try {
            request = ctx.bodyAsClass( ValidateDocumentsRequest.class );
        } catch ( Exception ignored ) {
            ctx.status( 400 ).json( new ValidateDocumentsResponse(
                    false,
                    false,
                    false,
                    OFF,
                    List.of(),
                    "Invalid request payload." ) );
            return;
        }

        if ( request == null || request.namespace == null || request.collection == null ) {
            ctx.status( 400 ).json( new ValidateDocumentsResponse(
                    false,
                    false,
                    false,
                    OFF,
                    List.of(),
                    "Missing 'namespace' or 'collection'." ) );
            return;
        }

        int maxViolations = clamp( request.maxViolationsPerDocument, 1, 500, 25 );

        LogicalNamespace namespace;
        try {
            namespace = Catalog.snapshot().getNamespace( request.namespace ).orElseThrow();
        } catch ( Exception ignored ) {
            ctx.status( 400 ).json( new ValidateDocumentsResponse(
                    false,
                    false,
                    false,
                    OFF,
                    List.of(),
                    "Unknown namespace: " + request.namespace ) );
            return;
        }

        long namespaceId = namespace.id;
        String collectionName = adjustNameForNamespace( request.collection, namespace );

        Optional<LogicalCollection> collection =
                Catalog.getInstance().getSnapshot().doc().getCollection( namespaceId, collectionName );

        if ( collection.isEmpty() ) {
            ctx.status( 400 ).json( new ValidateDocumentsResponse(
                    false,
                    false,
                    false,
                    OFF,
                    List.of(),
                    "Unknown collection: " + request.collection ) );
            return;
        }

        DocumentSchema schema = null;
        EnforcementMode mode = EnforcementMode.OFF;
        boolean hasSchema = false;

        try {
            Optional<SchemaMeta> schemaMeta =
                    SchemaMeta.readCurrent( Catalog.getInstance(), namespaceId, collection.get().id );

            if ( schemaMeta.isPresent() ) {
                SchemaMeta meta = schemaMeta.get();
                mode = resolveMode( meta.enforcement );

                if ( meta.schemaJson != null && !meta.schemaJson.isBlank() ) {
                    schema = parseSchemaOrThrow( meta.schemaJson );
                    hasSchema = true;
                }
            }
        } catch ( Exception e ) {
            log.warn( "Failed to load schema meta for {}.{}", request.namespace, request.collection, e );
            ctx.status( 500 ).json( new ValidateDocumentsResponse(
                    false,
                    false,
                    false,
                    OFF,
                    List.of(),
                    "Could not load schema meta." ) );
            return;
        }

        if ( request.documents == null || request.documents.isNull() ) {
            ctx.status( 400 ).json( new ValidateDocumentsResponse(
                    false,
                    false,
                    hasSchema,
                    mode.name(),
                    List.of(),
                    "Missing 'documents'." ) );
            return;
        }

        List<JsonNode> documents = normalizeDocuments( request.documents );
        List<DocResult> results = validateDocuments( documents, schema, maxViolations );

        boolean allConform = results.stream().allMatch( result -> result.ok );
        boolean allowed = mode != EnforcementMode.STRICT || allConform;

        ctx.status( 200 ).json( new ValidateDocumentsResponse(
                allConform,
                allowed,
                hasSchema,
                mode.name(),
                results,
                null ) );
    }


    private static List<JsonNode> normalizeDocuments( JsonNode documentsNode ) {
        List<JsonNode> documents = new ArrayList<>();

        if ( documentsNode.isArray() ) {
            ((ArrayNode) documentsNode).forEach( documents::add );
        } else {
            documents.add( documentsNode );
        }

        return documents;
    }


    private static List<DocResult> validateDocuments(
            List<JsonNode> documents,
            DocumentSchema schema,
            int maxViolations ) {
        List<DocResult> results = new ArrayList<>();

        for ( int i = 0; i < documents.size(); i++ ) {
            JsonNode documentNode = documents.get( i );

            if ( documentNode == null || documentNode.isNull() || !documentNode.isObject() ) {
                results.add( new DocResult(
                        i,
                        false,
                        List.of( new ViolationDto( "$", "PARSE", "Expected a JSON object." ) ),
                        "Expected a JSON object." ) );
                continue;
            }

            if ( schema == null ) {
                results.add( new DocResult( i, true, List.of(), null ) );
                continue;
            }

            try {
                SchemaValidator.ValidationResult validationResult =
                        SchemaValidator.validateJson( schema, documentNode );

                List<ViolationDto> violations = new ArrayList<>();
                List<SchemaValidator.Violation> rawViolations = validationResult.violations() == null
                        ? List.of()
                        : validationResult.violations();

                int limit = Math.min( rawViolations.size(), maxViolations );
                for ( int j = 0; j < limit; j++ ) {
                    violations.add( ViolationDto.from( rawViolations.get( j ) ) );
                }

                results.add( new DocResult( i, validationResult.ok(), violations, null ) );
            } catch ( Exception e ) {
                String message = "Could not parse/validate document: " + e.getMessage();
                results.add( new DocResult(
                        i,
                        false,
                        List.of( new ViolationDto( "$", "PARSE", message ) ),
                        message ) );
            }
        }

        return results;
    }


    private static int clamp( Integer value, int min, int max, int fallback ) {
        if ( value == null ) {
            return fallback;
        }

        return Math.max( min, Math.min( max, value ) );
    }


    private static String adjustNameForNamespace( String name, LogicalNamespace namespace ) {
        if ( name == null ) {
            return null;
        }

        if ( namespace != null && !namespace.caseSensitive ) {
            return name.toLowerCase( Locale.ROOT );
        }

        return name;
    }


    private static EnforcementMode resolveMode( String enforcement ) {
        if ( enforcement == null ) {
            return EnforcementMode.OFF;
        }

        try {
            return EnforcementMode.valueOf( enforcement.trim().toUpperCase( Locale.ROOT ) );
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
            throw new RuntimeException( "Stored collection schema is invalid", e );
        }
    }

}
