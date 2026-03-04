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

@Slf4j
public class DocumentSchemaCrud {

    private DocumentSchemaCrud() {
    }

    // -----------------------------
    // Request / Response DTOs (POJOs)
    // -----------------------------

    public static final class ValidateDocumentsRequest {
        public String namespace;
        public String collection;
        public JsonNode documents;
        public Integer maxViolationsPerDocument;
    }

    public static final class DocResult {
        public int index;
        public boolean ok;
        public List<SchemaValidator.Violation> violations;
        public String parseError;

        public DocResult() {
        }

        public DocResult( int index, boolean ok, List<SchemaValidator.Violation> violations, String parseError ) {
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
                String error
        ) {
            this.ok = ok;
            this.allowed = allowed;
            this.hasSchema = hasSchema;
            this.enforcement = enforcement;
            this.results = results;
            this.error = error;
        }
    }

    // -----------------------------
    // Route handler
    // -----------------------------

    public static void validateDocuments( final Context ctx ) {
        final ValidateDocumentsRequest request;

        try {
            request = ctx.bodyAsClass( ValidateDocumentsRequest.class );
        } catch ( Exception e ) {
            ctx.status( 400 ).json( new ValidateDocumentsResponse(
                    false, false, false, "OFF", List.of(), "Invalid request payload."
            ) );
            return;
        }

        if ( request == null || request.namespace == null || request.collection == null ) {
            ctx.status( 400 ).json( new ValidateDocumentsResponse(
                    false, false, false, "OFF", List.of(), "Missing 'namespace' or 'collection'."
            ) );
            return;
        }

        final int maxViolations = clamp( request.maxViolationsPerDocument, 1, 500, 25 );

        final LogicalNamespace ns;
        try {
            ns = Catalog.snapshot().getNamespace( request.namespace ).orElseThrow();
        } catch ( Exception e ) {
            ctx.status( 400 ).json( new ValidateDocumentsResponse(
                    false, false, false, "OFF", List.of(), "Unknown namespace: " + request.namespace
            ) );
            return;
        }

        final long nsId = ns.id;
        final String collName = adjustNameForNamespace( request.collection, ns );

        final Optional<LogicalCollection> collOpt =
                Catalog.getInstance().getSnapshot().doc().getCollection( nsId, collName );

        if ( collOpt.isEmpty() ) {
            ctx.status( 400 ).json( new ValidateDocumentsResponse(
                    false, false, false, "OFF", List.of(), "Unknown collection: " + request.collection
            ) );
            return;
        }

        final LogicalCollection coll = collOpt.get();

        // Load schema meta + enforcement
        DocumentSchema schema = null;
        EnforcementMode mode = EnforcementMode.OFF;
        boolean hasSchema = false;

        try {
            Optional<SchemaMeta> metaOpt = SchemaMeta.readCurrent( Catalog.getInstance(), nsId, coll.id );
            if ( metaOpt.isPresent() ) {
                SchemaMeta meta = metaOpt.get();
                mode = resolveMode( meta.enforcement );

                if ( meta.schemaJson != null && !meta.schemaJson.isBlank() ) {
                    schema = parseSchemaOrThrow( meta.schemaJson );
                    hasSchema = true;
                }
            }
        } catch ( Exception e ) {
            log.warn( "Failed to load schema meta for {}.{}", request.namespace, request.collection, e );
            ctx.status( 500 ).json( new ValidateDocumentsResponse(
                    false, false, false, "OFF", List.of(), "Could not load schema meta."
            ) );
            return;
        }

        // Normalize documents input: object or array
        if ( request.documents == null || request.documents.isNull() ) {
            ctx.status( 400 ).json( new ValidateDocumentsResponse(
                    false, false, hasSchema, mode.name(), List.of(), "Missing 'documents'."
            ) );
            return;
        }

        final List<JsonNode> docs = new ArrayList<>();
        if ( request.documents.isArray() ) {
            ((ArrayNode) request.documents).forEach( docs::add );
        } else {
            docs.add( request.documents );
        }

        final List<DocResult> results = new ArrayList<>();

        for ( int i = 0; i < docs.size(); i++ ) {
            JsonNode dn = docs.get( i );

            if ( dn == null || dn.isNull() || !dn.isObject() ) {
                results.add( new DocResult( i, false, List.of(), "Expected a JSON object." ) );
                continue;
            }

            if ( schema == null ) {
                // No schema -> everything conforms
                results.add( new DocResult( i, true, List.of(), null ) );
                continue;
            }

            try {
                // JSON->BSON conversion happens in core (SchemaValidator.validateJson)
                SchemaValidator.ValidationResult res = SchemaValidator.validateJson( schema, dn );

                List<SchemaValidator.Violation> violations = res.violations() == null ? List.of() : res.violations();
                if ( violations.size() > maxViolations ) {
                    violations = violations.subList( 0, maxViolations );
                }

                results.add( new DocResult( i, res.ok(), violations, null ) );
            } catch ( Exception e ) {
                results.add( new DocResult( i, false, List.of(), "Could not parse/validate document: " + e.getMessage() ) );
            }
        }

        boolean allConform = results.stream().allMatch( r -> r.ok );
        boolean allowed = (mode != EnforcementMode.STRICT) || allConform;

        ctx.status( 200 ).json( new ValidateDocumentsResponse(
                allConform, allowed, hasSchema, mode.name(), results, null
        ) );
    }

    // -----------------------------
    // Helpers
    // -----------------------------

    private static int clamp( Integer v, int min, int max, int fallback ) {
        if ( v == null ) {
            return fallback;
        }
        return Math.max( min, Math.min( max, v ) );
    }

    private static String adjustNameForNamespace( String name, LogicalNamespace ns ) {
        if ( name == null ) {
            return null;
        }
        if ( ns != null && !ns.caseSensitive ) {
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
        } catch ( IllegalArgumentException iae ) {
            return EnforcementMode.OFF;
        }
    }

    private static DocumentSchema parseSchemaOrThrow( String json ) {
        try {
            DocumentSchema s = SchemaJson.parse( json );
            s.validateOrThrow();
            return s;
        } catch ( Exception e ) {
            throw new RuntimeException( "Stored collection schema is invalid", e );
        }
    }
}
