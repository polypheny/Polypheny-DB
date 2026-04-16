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

package org.polypheny.db.schema.document;

import static java.util.Objects.requireNonNull;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.schema.document.DocumentSchema.Node;
import org.polypheny.db.schema.document.SchemaOptionsResolver.AlterMode;
import org.polypheny.db.transaction.Statement;

/**
 * Plans ALTER SCHEMA operations and applies preflight policy.
 */
public final class SchemaAlterEngine {

    public record Plan(DocumentSchema currentSchema, EnforcementMode currentMode, DocumentSchema finalSchema, EnforcementMode finalMode, boolean isPatch, boolean needsPreflight) {

    }


    public record Outcome(boolean applied, SchemaAlterPreflightReport preflight) {

    }


    public SchemaAlterEngine() {
    }


    public Plan plan( SchemaOptionsResolver.Resolved resolved, DocumentSchema currentSchema, EnforcementMode currentMode ) {

        EnforcementMode targetMode = resolved.mode != null ? resolved.mode : currentMode;

        if ( resolved.schema == null ) {
            boolean needsPreflight = currentSchema != null && isTightening( currentMode, targetMode );
            return new Plan( currentSchema, currentMode, null, targetMode, false, needsPreflight );
        }

        if ( resolved.alterMode == AlterMode.PATCH && currentSchema == null ) {
            throw new GenericRuntimeException( "Cannot PATCH schema: collection has no persisted schema to patch." );
        }

        DocumentSchema finalSchema = resolved.alterMode == AlterMode.PATCH ? mergePatch( requireNonNull( currentSchema, "current schema" ), resolved.schema ) : requireNonNull( resolved.schema, "schema" );

        finalSchema.validateOrThrow();

        boolean needsPreflight;
        if ( currentSchema == null ) {
            needsPreflight = targetMode == EnforcementMode.STRICT;
        } else {
            boolean compatible = SchemaCompatibility.isCompatible( currentSchema, finalSchema );
            boolean additionalPropertiesTightened = currentSchema.additionalProperties() == DocumentSchema.AdditionalProperties.ALLOW && finalSchema.additionalProperties() == DocumentSchema.AdditionalProperties.FORBID;

            needsPreflight = !compatible || additionalPropertiesTightened;

            // Moving to STRICT requires a scan even if the schemas look compatible,
            // because existing data may already violate the current schema.
            if ( targetMode == EnforcementMode.STRICT && currentMode != EnforcementMode.STRICT ) {
                needsPreflight = true;
            }
        }

        return new Plan( currentSchema, currentMode, finalSchema, targetMode, resolved.alterMode == AlterMode.PATCH, needsPreflight );
    }


    public SchemaAlterPreflightReport preflightIfRequired( Catalog catalog, LogicalCollection collection, Plan plan, Statement statement ) {

        if ( !plan.needsPreflight() || plan.finalSchema() == null ) {
            return new SchemaAlterPreflightReport( true, 0, 0, List.of() );
        }

        return SchemaAlterPreflight.run( catalog, collection, plan.finalSchema(), statement );
    }


    public SchemaAlterPreflightReport preflightForEnforcementOnlyIfRequired( Catalog catalog, LogicalCollection collection, Plan plan, Statement statement ) {

        EnforcementMode currentMode = plan.currentMode();
        EnforcementMode targetMode = plan.finalMode();

        if ( !isTightening( currentMode, targetMode ) ) {
            return new SchemaAlterPreflightReport( true, 0, 0, List.of() );
        }

        Optional<SchemaMeta> schemaMeta = SchemaMeta.readCurrent( catalog, collection.namespaceId, collection.id );

        if ( schemaMeta.isEmpty() ) {
            return new SchemaAlterPreflightReport( true, 0, 0, List.of() );
        }

        DocumentSchema currentSchema = SchemaJson.parse( schemaMeta.get().schemaJson );
        return SchemaAlterPreflight.run( catalog, collection, currentSchema, statement );
    }


    public Outcome applyPolicyOrThrow( SchemaAlterPreflightReport preflightReport, boolean isSchemaChange, EnforcementMode currentMode, EnforcementMode finalMode ) {

        if ( isSchemaChange ) {
            if ( preflightReport.ok ) {
                return new Outcome( true, preflightReport );
            }

            if ( finalMode != EnforcementMode.STRICT ) {
                return new Outcome( true, preflightReport );
            }

            throw new GenericRuntimeException( String.format( "ALTER SCHEMA would invalidate %d/%d documents; examples: %s", preflightReport.failing, preflightReport.scanned, preflightReport.compactSummary( 5 ) ) );
        }

        if ( isTightening( currentMode, finalMode ) && !preflightReport.ok ) {
            throw new GenericRuntimeException( String.format( "Cannot set validationAction=STRICT: %d/%d documents violate the current schema; examples: %s", preflightReport.failing, preflightReport.scanned, preflightReport.compactSummary( 5 ) ) );
        }

        return new Outcome( true, preflightReport );
    }


    private static boolean isTightening( EnforcementMode currentMode, EnforcementMode finalMode ) {
        if ( finalMode == null ) {
            return false;
        }

        if ( currentMode == EnforcementMode.STRICT ) {
            return false;
        }

        return finalMode == EnforcementMode.STRICT;
    }


    /**
     * Merges a PATCH schema into the current schema.
     */
    public static DocumentSchema mergePatch( DocumentSchema currentSchema, DocumentSchema patchSchema ) {
        DocumentSchema.ObjectNode mergedRoot = mergeObject( currentSchema.root(), patchSchema.root() );

        DocumentSchema.AdditionalProperties additionalProperties = patchSchema.additionalProperties() != null ? patchSchema.additionalProperties() : currentSchema.additionalProperties();

        return new DocumentSchema( mergedRoot, additionalProperties );
    }


    /**
     * PATCH merges nested object nodes recursively. Other node types replace the current value.
     */
    private static DocumentSchema.ObjectNode mergeObject( DocumentSchema.ObjectNode currentObject, DocumentSchema.ObjectNode patchObject ) {

        if ( patchObject == null ) {
            return currentObject;
        }

        Map<String, Node> mergedProperties = new LinkedHashMap<>( currentObject.properties );

        for ( Map.Entry<String, Node> entry : patchObject.properties.entrySet() ) {
            String key = entry.getKey();
            Node patchNode = entry.getValue();
            Node currentNode = currentObject.properties.get( key );

            if ( patchNode instanceof DocumentSchema.ObjectNode patchChildObject && currentNode instanceof DocumentSchema.ObjectNode currentChildObject ) {
                mergedProperties.put( key, mergeObject( currentChildObject, patchChildObject ) );
            } else {
                mergedProperties.put( key, patchNode );
            }
        }

        Set<String> required = patchObject.required != null ? patchObject.required : currentObject.required;

        DocumentSchema.AdditionalProperties additionalProperties = patchObject.additionalProperties != null && patchObject.additionalProperties != DocumentSchema.AdditionalProperties.INHERIT ? patchObject.additionalProperties : currentObject.additionalProperties;

        Integer minProperties = patchObject.minProperties != null ? patchObject.minProperties : currentObject.minProperties;
        Integer maxProperties = patchObject.maxProperties != null ? patchObject.maxProperties : currentObject.maxProperties;

        return new DocumentSchema.ObjectNode( mergedProperties, required, additionalProperties, minProperties, maxProperties );
    }

}