package org.polypheny.db.algebra.logical.document;

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

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.bson.BsonDocument;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.document.DocumentSchema;
import org.polypheny.db.schema.document.DocumentSchema.AdditionalProperties;
import org.polypheny.db.schema.document.EnforcementMode;
import org.polypheny.db.schema.document.SchemaJson;
import org.polypheny.db.schema.document.SchemaMeta;
import org.polypheny.db.schema.document.SchemaValidator;
import org.polypheny.db.schema.document.SchemaValidator.ValidationResult;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.polypheny.db.type.PolyType;


public final class DocumentSchemaWriteEnforcer {

    private static final Logger LOG = LoggerFactory.getLogger( DocumentSchemaWriteEnforcer.class );

    private DocumentSchemaWriteEnforcer() {
    }

    /**
     * Schema + enforcement mode bundle.
     */
    private record SchemaContext( DocumentSchema schema, EnforcementMode mode ) {
    }

    /**
     * Entry point called from LogicalDocumentModify.create(...).
     *
     * If there is no schema or enforcement is OFF, this is a no-op.
     * Otherwise it:
     *  - preflights literal INSERTs (if possible)
     *  - performs static checks on update/remove/rename specs
     */
    public static void enforce(
            Entity entity,
            AlgNode input,
            LogicalDocumentModify.Operation operation,
            Map<String, ? extends RexNode> updates,
            List<String> removes,
            Map<String, String> renames ) {

        Optional<SchemaContext> ctxOpt = loadSchemaContext( entity );
        if ( ctxOpt.isEmpty() ) {
            // no schema, or enforcement=OFF, or not a LogicalCollection
            return;
        }

        SchemaContext ctx = ctxOpt.get();
        DocumentSchema schema = ctx.schema();
        EnforcementMode mode = ctx.mode();

        // INSERT: preflight literal values when we can see the documents statically
        if ( operation == LogicalDocumentModify.Operation.INSERT ) {
            preflightLiteralInsert( input, entity, schema, mode );
        }

        if ( operation == LogicalDocumentModify.Operation.UPDATE && updates != null && !updates.isEmpty() ) {
            validateUpdateTypes( entity, schema, mode, updates );
        }

        // UPDATE / generic MODIFY: static spec checks (unknown top-level fields)
        validateUpdateSpec( entity, schema, mode, updates, removes, renames );

        validateRequiredFieldsNotRemoved(entity, schema, mode, removes, renames);
    }

    // -------------------------------------------------------------------------
    // Schema loading (mirrors MqlSchemaEnforcer)
    // -------------------------------------------------------------------------

    /**
     * Load active schema + enforcement for a logical collection.
     *
     * Returns empty if:
     *  - entity is not a LogicalCollection
     *  - there is no SchemaMeta
     *  - schema JSON is null/blank
     *  - enforcement resolves to OFF
     */
    private static Optional<SchemaContext> loadSchemaContext( Entity entity ) {
        if ( entity == null ) {
            return Optional.empty();
        }

        // We must resolve the LOGICAL collection id (SchemaMeta is stored under LogicalCollection.id)
        Long nsId = tryGetNamespaceId( entity );
        if ( nsId == null ) {
            return Optional.empty();
        }

        // Resolve collection by name in the doc snapshot (most reliable)
        String name = entity.getName();
        String adjusted = adjustNameForNamespace( name, nsId );

        var snap = Catalog.getInstance().getSnapshot();
        var collOpt = snap.doc().getCollection( nsId, adjusted );

        if ( collOpt.isPresent() ) {
            // Use the logical collection id (this is what createCollectionWS stored under)
            return loadFromIds( nsId, collOpt.get().id );
        }

        // Fallback: if entity itself is a LogicalCollection, use it directly
        if ( entity instanceof LogicalCollection lc ) {
            return loadFromIds( lc.namespaceId, lc.id );
        }

        // Last resort: try whatever id the entity exposes (may still be wrong for allocations)
        Long rawId = tryGetId( entity );
        if ( rawId != null ) {
            return loadFromIds( nsId, rawId );
        }

        return Optional.empty();
    }

    private static Optional<SchemaContext> loadFromIds( long namespaceId, long collectionId ) {
        Optional<SchemaMeta> metaOpt = SchemaMeta.readCurrent(
                Catalog.getInstance(),
                namespaceId,
                collectionId
        );

        if ( metaOpt.isEmpty() ) {
            return Optional.empty();
        }

        SchemaMeta meta = metaOpt.get();
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

    private static String adjustNameForNamespace( String name, long nsId ) {
        var ns = Catalog.getInstance().getSnapshot().getNamespace( nsId ).orElseThrow();
        return ns.caseSensitive ? name : name.toLowerCase( Locale.ROOT );
    }

// ---- helpers to extract ids from non-logical entity types (best-effort)

    private static Long tryGetNamespaceId( Entity e ) {
        try {
            // common in catalog entities
            var f = e.getClass().getDeclaredField( "namespaceId" );
            f.setAccessible( true );
            Object v = f.get( e );
            return (v instanceof Number n) ? n.longValue() : null;
        } catch ( Exception ignored ) { }

        try {
            var m = e.getClass().getMethod( "getNamespaceId" );
            Object v = m.invoke( e );
            return (v instanceof Number n) ? n.longValue() : null;
        } catch ( Exception ignored ) { }

        // If it’s LogicalCollection we can read it directly
        if ( e instanceof LogicalCollection lc ) {
            return lc.namespaceId;
        }

        return null;
    }

    private static Long tryGetId( Entity e ) {
        try {
            var f = e.getClass().getDeclaredField( "id" );
            f.setAccessible( true );
            Object v = f.get( e );
            return (v instanceof Number n) ? n.longValue() : null;
        } catch ( Exception ignored ) { }

        try {
            var m = e.getClass().getMethod( "getId" );
            Object v = m.invoke( e );
            return (v instanceof Number n) ? n.longValue() : null;
        } catch ( Exception ignored ) { }

        return null;
    }


    /**
     * Reads a long from either a field or a no-arg getter. Tries candidates in order.
     */
    private static Optional<Long> readLong( Object target, String... candidates ) {
        Class<?> c = target.getClass();

        for ( String name : candidates ) {
            // 1) field
            try {
                var f = c.getDeclaredField( name );
                f.setAccessible( true );
                Object v = f.get( target );
                if ( v instanceof Number n ) {
                    return Optional.of( n.longValue() );
                }
            } catch ( Exception ignored ) {
                // ignore
            }

            // 2) method
            try {
                var m = c.getMethod( name );
                Object v = m.invoke( target );
                if ( v instanceof Number n ) {
                    return Optional.of( n.longValue() );
                }
            } catch ( Exception ignored ) {
                // ignore
            }
        }

        return Optional.empty();
    }


    /**
     * Same semantics as MqlSchemaEnforcer.resolveMode:
     *  - null -> OFF
     *  - value is uppercased and parsed
     *  - invalid -> OFF
     */
    private static EnforcementMode resolveMode( SchemaMeta meta ) {
        try {
            return EnforcementMode.valueOf(
                    (meta.enforcement == null ? "OFF" : meta.enforcement).trim().toUpperCase( Locale.ROOT )
            );
        } catch ( IllegalArgumentException iae ) {
            return EnforcementMode.OFF;
        }
    }

    /**
     * Same semantics as MqlSchemaEnforcer.parseSchemaOrThrow.
     */
    private static DocumentSchema parseSchemaOrThrow( String json ) {
        try {
            DocumentSchema s = SchemaJson.parse( json );
            s.validateOrThrow();
            return s;
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Stored collection schema is invalid", e );
        }
    }

    // -------------------------------------------------------------------------
    // INSERT preflight (literal values only), using SchemaValidator
    // -------------------------------------------------------------------------

    /**
     * Plan-time validation for literal INSERTs.
     *
     * Only runs if:
     *  - operation == INSERT (checked by caller)
     *  - the input AlgNode is a LogicalDocumentValues with literal docs
     *
     * For INSERT .. SELECT and other non-literal sources, nothing happens here;
     * those must be enforced at execution time (adapter or constraint enforcer).
     */
    private static void preflightLiteralInsert(
            AlgNode input,
            Entity entity,
            DocumentSchema schema,
            EnforcementMode mode ) {

        if ( mode == EnforcementMode.OFF ) {
            return;
        }

        if ( !(input instanceof LogicalDocumentValues values) ) {
            // Not a VALUES-style literal insert
            return;
        }

        // STATIC documents only; dynamicDocuments are parameters evaluated at runtime.
        List<PolyDocument> docs = values.getDocuments();

        for ( PolyDocument doc : docs ) {
            // Convert PolyDocument -> BSON for validation.
            // PolyDocument has toJson(), so we can safely parse that.
            BsonDocument raw = BsonDocument.parse( doc.toJson() );
            BsonDocument toCheck = stripIdForValidation( raw );

            ValidationResult res = SchemaValidator.validate( schema, toCheck );
            if ( !res.ok() ) {
                String msg = "Inserted document does not conform to the collection schema: "
                        + res.compactSummary( 3 );
                handleViolation( mode, msg, entity.getName(), raw );
            }
        }
    }

    /**
     * Remove internal _id before validation, if present, to mirror other tools.
     */
    private static BsonDocument stripIdForValidation( BsonDocument doc ) {
        if ( doc == null ) {
            return null;
        }
        if ( !doc.containsKey( DocumentType.DOCUMENT_ID ) ) {
            return doc;
        }
        BsonDocument clone = doc.clone();
        clone.remove( DocumentType.DOCUMENT_ID );
        return clone;
    }

    // -------------------------------------------------------------------------
    // UPDATE / MODIFY spec checks (static)
    // -------------------------------------------------------------------------

    /**
     * Static validation of update/remove/rename specs.
     *
     * This does *not* see row values; it only checks that top-level fields being
     * touched exist in the schema when additionalProperties=FORBID.
     *
     * Full post-image validation (types, nested structures) requires runtime
     * enforcement (adapter or ConstraintEnforcer-like node) where the final
     * document is visible.
     */
    private static void validateUpdateSpec(
            Entity entity,
            DocumentSchema schema,
            EnforcementMode mode,
            Map<String, ? extends RexNode> updates,
            List<String> removes,
            Map<String, String> renames ) {

        if ( schema.additionalProperties() == AdditionalProperties.ALLOW ) {
            return;
        }

        var allowedTop = schema.root().properties.keySet();

        // Updates: no unknown top-level paths
        if ( updates != null ) {
            for ( String path : updates.keySet() ) {
                String top = topLevelSegment( path );
                if ( !allowedTop.contains( top ) ) {
                    String msg = "Update touches undeclared field '" + top + "'";
                    handleViolation( mode, msg, entity.getName(), path );
                }
            }
        }

        // Removes: same idea
        if ( removes != null ) {
            for ( String path : removes ) {
                String top = topLevelSegment( path );
                if ( !allowedTop.contains( top ) ) {
                    String msg = "Remove touches undeclared field '" + top + "'";
                    handleViolation( mode, msg, entity.getName(), path );
                }
            }
        }

        // Renames: check both source and target
        if ( renames != null ) {
            for ( Map.Entry<String, String> e : renames.entrySet() ) {
                String fromTop = topLevelSegment( e.getKey() );
                String toTop = topLevelSegment( e.getValue() );

                if ( !allowedTop.contains( fromTop ) || !allowedTop.contains( toTop ) ) {
                    String msg = "Rename between undeclared fields '"
                            + fromTop + "' -> '" + toTop + "'";
                    handleViolation( mode, msg, entity.getName(), e );
                }
            }
        }
    }

    private static void validateRequiredFieldsNotRemoved(
            Entity entity,
            DocumentSchema schema,
            EnforcementMode mode,
            List<String> removes,
            Map<String, String> renames ) {

        if ( mode == EnforcementMode.OFF ) {
            return;
        }

        // Dialect rule: if "required" is omitted, all declared properties are treated as required.
        // Therefore, removing/renaming a declared root property violates requiredness.
        var requiredTop = schema.root().effectiveRequired();

        // $unset / removes
        if ( removes != null ) {
            for ( String path : removes ) {
                String top = topLevelSegment( path );
                if ( requiredTop.contains( top ) ) {
                    String msg = "Update removes required field '" + top + "'";
                    handleViolation( mode, msg, entity.getName(), path );
                }
            }
        }

        // $rename
        if ( renames != null ) {
            for ( Map.Entry<String, String> e : renames.entrySet() ) {
                String fromTop = topLevelSegment( e.getKey() );

                // Renaming a required property means it disappears from its original name
                if ( requiredTop.contains( fromTop ) ) {
                    String msg = "Update renames required field '" + fromTop + "'";
                    handleViolation( mode, msg, entity.getName(), e );
                }
            }
        }
    }


    /**
     * Type-aware validation of UPDATE expressions against the JSON schema.
     *
     * This catches mismatches for:
     *  - $set  (normal assignment)
     *  - $inc  (encoded as PLUS(...) RexCall)
     *  - $mul  (encoded as MULTIPLY/TIMES(...) RexCall)
     *  - $min/$max (encoded as special MQL_UPDATE_MIN/MAX operators)
     */
    private static void validateUpdateTypes(
            Entity entity,
            DocumentSchema schema,
            EnforcementMode mode,
            Map<String, ? extends RexNode> updates ) {

        for ( Map.Entry<String, ? extends RexNode> e : updates.entrySet() ) {
            String path = e.getKey();
            RexNode expr = e.getValue();

            // $unset may appear as null expression in some converters
            if ( expr == null ) {
                continue;
            }

            var nodeOpt = resolveNode( schema, path );
            if ( nodeOpt.isEmpty() ) {
                // unknown path: handled elsewhere for FORBID, or allowed for ALLOW
                continue;
            }

            DocumentSchema.Node node = nodeOpt.get();
            if ( !(node instanceof DocumentSchema.ScalarNode sn) ) {
                // object/array checks need post-image validation; skip here
                continue;
            }

            List<PolyType> expectedTypes = sn.types;
            PolyType expected = sn.type; // first allowed type (for legacy messages)

            // Infer which Mongo update operator this expression represents
            String semantic = "$set";
            if ( expr instanceof org.polypheny.db.rex.RexCall call
                    && call.getOperator() != null
                    && call.getOperator().getName() != null ) {

                String n = call.getOperator().getName().trim().toLowerCase( Locale.ROOT );

                if ( n.contains( "mql_update_min" ) || n.equals( "$min" ) ) {
                    semantic = "$min";
                } else if ( n.contains( "mql_update_max" ) || n.equals( "$max" ) ) {
                    semantic = "$max";
                } else if ( n.equals( "+" ) || n.contains( "plus" ) || n.contains( "add" ) ) {
                    semantic = "$inc";
                } else if ( n.equals( "*" ) || n.contains( "multiply" ) || n.contains( "times" ) ) {
                    semantic = "$mul";
                } else {
                    semantic = "$set";
                }
            }

            // Helpers (inline, no extra methods needed)
            final java.util.function.Predicate<PolyType> isText = t -> {
                if ( t == null ) return false;
                return switch ( t ) {
                    case TEXT, VARCHAR, CHAR -> true;
                    default -> false;
                };
            };

            final java.util.function.Predicate<PolyType> isNumeric = t -> {
                if ( t == null ) return false;
                return switch ( t ) {
                    case TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL, REAL, FLOAT, DOUBLE -> true;
                    default -> false;
                };
            };

            // Extract a better "actual" type for literals that the converter wraps as DOCUMENT
            final java.util.function.Function<RexNode, PolyType> scalarTypeFromRex = n -> {
                if ( n == null ) {
                    return null;
                }

                // 1) Start with the normal AlgDataType
                PolyType t = null;
                try {
                    t = (n.getType() != null) ? n.getType().getPolyType() : null;
                } catch ( Exception ignored ) {
                    // keep null
                }

                // 2) If converter forced DOCUMENT, recover from RexLiteral payload (PolyValue)
                if ( t == PolyType.DOCUMENT && n instanceof org.polypheny.db.rex.RexLiteral lit ) {
                    org.polypheny.db.type.entity.PolyValue v = lit.getValue();

                    if ( v == null || v.isNull() ) {
                        return PolyType.NULL;
                    }

                    // If the payload isn't actually a document, use its real scalar PolyType
                    if ( !v.isDocument() ) {
                        PolyType vt = v.getType();
                        // Normalize string family to TEXT for schema comparisons
                        return switch ( vt ) {
                            case CHAR, VARCHAR, TEXT -> PolyType.TEXT;
                            default -> vt;
                        };
                    }

                    return PolyType.DOCUMENT;
                }

                return t;
            };

            PolyType actual = scalarTypeFromRex.apply( expr );

            // If this is a computed update ($inc/$mul/$min/$max) and the call itself is typed DOCUMENT,
            // try to recover a numeric "actual" from literal operands (e.g. PLUS(field, 1)).
            if ( actual == PolyType.DOCUMENT
                    && (semantic.equals( "$inc" ) || semantic.equals( "$mul" ) || semantic.equals( "$min" ) || semantic.equals( "$max" ))
                    && expr instanceof org.polypheny.db.rex.RexCall call ) {

                for ( RexNode op : call.getOperands() ) {
                    PolyType ot = scalarTypeFromRex.apply( op );
                    if ( ot != null && isNumeric.test( ot ) ) {
                        actual = ot;
                        break;
                    }
                }
            }

            // Operator-specific rules: $inc/$mul/$min/$max require numeric targets + numeric operand
            if ( semantic.equals( "$inc" ) || semantic.equals( "$mul" ) || semantic.equals( "$min" ) || semantic.equals( "$max" ) ) {

                boolean targetNumeric = expectedTypes != null && expectedTypes.stream().anyMatch( isNumeric );
                if ( !targetNumeric ) {
                    String msg = "Update operator " + semantic
                            + " cannot be applied to non-numeric field '" + path
                            + "' (schema expects one of " + expectedTypes + ")";
                    handleViolation( mode, msg, entity.getName(), path );
                    continue;
                }

                // Also ensure the update expression contributes a numeric value (best-effort static check)
                boolean operandNumeric = false;
                if ( expr instanceof org.polypheny.db.rex.RexCall call ) {
                    for ( RexNode op : call.getOperands() ) {
                        PolyType ot = scalarTypeFromRex.apply( op );
                        if ( ot != null && isNumeric.test( ot ) ) {
                            operandNumeric = true;
                            break;
                        }
                    }
                } else {
                    operandNumeric = (actual != null && isNumeric.test( actual ));
                }

                if ( !operandNumeric ) {
                    String msg = "Update operator " + semantic
                            + " for field '" + path + "' requires a numeric value"
                            + ", but expression is typed " + actual;
                    handleViolation( mode, msg, entity.getName(), path );
                    continue;
                }
            }

            // Generic scalar compatibility ($set and also the final type of computed ops)
            if ( actual != null ) {

                boolean ok = true;

                // If the converter was forced to DOCUMENT for scalars, don't over-enforce here.
                if ( actual == PolyType.DOCUMENT ) {
                    ok = true;
                } else if ( expectedTypes != null && !expectedTypes.isEmpty() ) {

                    boolean allowsNull = expectedTypes.contains( PolyType.NULL );
                    boolean allowsText = expectedTypes.stream().anyMatch( isText );
                    boolean allowsNumeric = expectedTypes.stream().anyMatch( isNumeric );
                    boolean allowsBoolean = expectedTypes.contains( PolyType.BOOLEAN );

                    if ( actual == PolyType.NULL ) {
                        ok = allowsNull;
                    } else if ( isText.test( actual ) ) {
                        ok = allowsText;
                    } else if ( isNumeric.test( actual ) ) {
                        ok = allowsNumeric;
                    } else if ( actual == PolyType.BOOLEAN ) {
                        ok = allowsBoolean;
                    } else {
                        ok = expectedTypes.contains( actual );
                    }
                }

                if ( !ok ) {
                    String msg = "Update expression for field '" + path
                            + "' has type " + actual
                            + ", but schema expects one of " + expectedTypes
                            + " (operator: " + semantic + ")";
                    handleViolation( mode, msg, entity.getName(), path );
                }
            }
        }
    }


    /**
     * First segment of a dotted path (e.g., "a.b.c" -> "a").
     */
    private static String topLevelSegment( String path ) {
        if ( path == null ) {
            return "";
        }
        int dot = path.indexOf( '.' );
        return dot < 0 ? path : path.substring( 0, dot );
    }

    // -------------------------------------------------------------------------
    // Violation handling (mirrors MqlSchemaEnforcer.handle)
    // -------------------------------------------------------------------------

    /**
     * Resolve a dotted JSON path (e.g. "a.b[0].c" or "a.0.b") against the schema,
     * using the same semantics as in MqlSchemaEnforcer:
     *
     *  - object: step into named properties
     *  - array: step into "items", numeric segments are treated as indexes
     *  - scalar: cannot have children
     */
    private static Optional<DocumentSchema.Node> resolveNode( DocumentSchema schema, String path ) {
        DocumentSchema.Node cur = schema.root();
        if ( path == null || path.isEmpty() ) {
            return Optional.empty();
        }

        String[] segs = path.split( "\\." );
        for ( String seg : segs ) {

            if ( cur instanceof DocumentSchema.ObjectNode on ) {
                DocumentSchema.Node nxt = on.properties.get( seg );
                if ( nxt == null ) {
                    return Optional.empty();
                }
                cur = nxt;

            } else if ( cur instanceof DocumentSchema.ArrayNode an ) {
                // step into items; allow numeric index segments (e.g. "tags.0")
                cur = an.items;
                if ( seg.matches( "\\d+" ) ) {
                    // consumed an index; continue with next segment
                    continue;
                } else {
                    if ( cur instanceof DocumentSchema.ObjectNode aon ) {
                        DocumentSchema.Node nxt = aon.properties.get( seg );
                        if ( nxt == null ) {
                            return Optional.empty();
                        }
                        cur = nxt;
                    } else {
                        return Optional.empty();
                    }
                }

            } else {
                // scalar cannot have children
                return Optional.empty();
            }
        }

        return Optional.ofNullable( cur );
    }

    private static void handleViolation(
            EnforcementMode mode,
            String msg,
            String entityName,
            Object detail ) {

        switch ( mode ) {
            case STRICT -> throw new GenericRuntimeException( msg );
            case WARN -> {
                if ( LOG.isWarnEnabled() ) {
                    LOG.warn( "{}; allowed due to WARN. Entity='{}' Detail={}", msg, entityName, summarize( detail ) );
                }
            }
            case OFF -> {
                // filtered earlier; nothing to do
            }
        }
    }

    private static String summarize( Object value ) {
        try {
            String s = String.valueOf( value );
            return s.length() > 500 ? s.substring( 0, 500 ) + "…" : s;
        } catch ( Exception e ) {
            return "<unprintable>";
        }
    }

}
