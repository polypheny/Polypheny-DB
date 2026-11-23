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

package org.polypheny.db.catalog.exceptions;

import java.util.List;
import java.util.Map;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.MappingKind;
import org.polypheny.db.type.ScalarConversions;
import org.polypheny.db.type.entity.PolyValue;

/**
 * MappingError is a central factory that creates mapping exceptions through category-specific methods and constructs consistent error messages.
 */
public final class MappingError {

    private MappingError() {
    }


    public static final class Missing {

        public static MappingMissingException collection( long collectionId ) {
            return new MappingMissingException( "Document collection %d is no longer available.".formatted( collectionId ), null, Map.of(), List.of() );
        }


        public static MappingMissingException physicalCollection( long mappingId, long allocationId ) {
            return new MappingMissingException( "Mapping %d requires one physical document collection for allocation %d.".formatted( mappingId, allocationId ), null, Map.of(), List.of() );
        }

        private Missing() {
        }


        public static MappingMissingException partitionColumn( long mappingId, long columnId, long partitionId ) {
            return new MappingMissingException( "Mapping %d references logical column %d unavailable on partition %d.".formatted( mappingId, columnId, partitionId ), null, Map.of(), List.of() );
        }


        public static MappingMissingException logicalColumn( long mappingId, long columnId ) {
            return new MappingMissingException( "Mapping %d references missing logical column %d.".formatted( mappingId, columnId ), null, Map.of(), List.of() );
        }


        public static MappingMissingException adapterAllocation( long mappingId, long allocationId ) {
            return new MappingMissingException( "Mapping %d has no adapter-owned allocation %d.".formatted( mappingId, allocationId ), null, Map.of(), List.of() );
        }


        public static MappingMissingException adapterCatalog( long mappingId, long allocationId ) {
            return new MappingMissingException( "Mapping %d has no adapter catalog for allocation %d.".formatted( mappingId, allocationId ), null, Map.of(), List.of() );
        }


        public static MappingMissingException columnRule( long mappingId, long columnId ) {
            return new MappingMissingException( "Mapping %d has no rule for logical column %d.".formatted( mappingId, columnId ), null, Map.of(), List.of() );
        }


        public static MappingMissingException allocation( long mappingId, long targetEntityId ) {
            return new MappingMissingException( "Mapping %d requires an allocation for logical entity %d.".formatted( mappingId, targetEntityId ), null, Map.of(), List.of() );
        }


        public static MappingMissingException target( long mappingId, long targetEntityId ) {
            return new MappingMissingException( "Mapping %d references missing relational entity %d.".formatted( mappingId, targetEntityId ), null, Map.of(), List.of() );
        }


        public static MappingMissingException targetNamespace() {
            return new MappingMissingException( "Choose an existing target namespace.", null, Map.of( "targetNamespaceId", "This namespace is no longer available." ), List.of() );
        }


        public static MappingMissingException targetColumn( String columnName ) {
            return new MappingMissingException( "The mapping target no longer contains column '%s'.".formatted( columnName ), null, Map.of(), List.of() );
        }


        public static MappingMissingException physicalTable( long mappingId, long allocationId ) {
            return new MappingMissingException( "Mapping %d has no physical table for allocation %d.".formatted( mappingId, allocationId ), null, Map.of(), List.of() );
        }


        public static MappingMissingException transformer( String ruleType ) {
            return new MappingMissingException( "No mapping transformer is registered for rule type %s.".formatted( ruleType ), null, Map.of(), List.of() );
        }


        public static MappingMissingException requestedColumnRule( long mappingId, long allocationId ) {
            return new MappingMissingException( "Mapping %d has no rule for a requested column of allocation %d.".formatted( mappingId, allocationId ), null, Map.of(), List.of() );
        }


        public static MappingMissingException conversionResultType() {
            return new MappingMissingException( "Mapping conversion requires an explicit result type.", null, Map.of(), List.of() );
        }


        public static MappingMissingException allocationMapping( long allocationId, String targetName ) {
            return new MappingMissingException( "No mapping identifies source allocation %s for target '%s'.".formatted( allocationId, targetName ), null, Map.of(), List.of() );
        }


        public static MappingMissingException sourceAllocation( long mappingId, Long allocationId ) {
            return new MappingMissingException( "Mapping %d cannot resolve source allocation %s.".formatted( mappingId, allocationId ), null, Map.of(), List.of() );
        }


        public static MappingMissingException namespace( long mappingId, long namespaceId ) {
            return new MappingMissingException( "Mapping %d cannot resolve target namespace %d.".formatted( mappingId, namespaceId ), null, Map.of(), List.of() );
        }


        public static MappingMissingException previewAllocation( long targetId ) {
            return new MappingMissingException( "Mapped target %d has no allocation available for preview.".formatted( targetId ), null, Map.of(), List.of() );
        }


        public static MappingMissingException sourceTable( String sourceTable, String mappingName, String targetName ) {
            return new MappingMissingException( "Source table '%s' for mapping '%s' is no longer available. Target '%s' was not changed.".formatted( sourceTable, mappingName, targetName ), null, Map.of(), List.of() );
        }


        public static MappingMissingException adapter( long adapterId ) {
            return new MappingMissingException( "Source adapter %d is no longer available.".formatted( adapterId ), null, Map.of(), List.of() );
        }


        public static MappingMissingException adapterCatalog( long adapterId ) {
            return new MappingMissingException( "Accepted physical source metadata for adapter %d is unavailable.".formatted( adapterId ), null, Map.of(), List.of() );
        }


        public static MappingMissingException physicalMetadata( long allocationId ) {
            return new MappingMissingException( "Accepted physical source metadata for allocation %d is unavailable.".formatted( allocationId ), null, Map.of(), List.of() );
        }


        public static MappingMissingException sourceAllocation( long mappingId ) {
            return new MappingMissingException( "Mapping %d has no available source allocation.".formatted( mappingId ), null, Map.of(), List.of() );
        }


        public static MappingMissingException mapping( long mappingId ) {
            return new MappingMissingException( "Unknown mapping %d.".formatted( mappingId ), null, Map.of(), List.of() );
        }


        public static MappingMissingException resource() {
            return new MappingMissingException( "The source adapter or its mapping is no longer available.", null, Map.of(), List.of() );
        }


        public static MappingMissingException sourceColumn( String sourceColumn, String mappingName, String targetTable, String targetColumn ) {
            return new MappingMissingException( "Source column '%s' in mapping '%s' is missing for target column '%s.%s'. Target schema was not changed.".formatted( sourceColumn, mappingName, targetTable, targetColumn ), null, Map.of(), List.of() );
        }


        public static MappingMissingException physicalColumn( long mappingId, long columnId, long allocationId ) {
            return new MappingMissingException( "Mapping %d has no physical column %d for allocation %d.".formatted( mappingId, columnId, allocationId ), null, Map.of(), List.of() );
        }


        public static MappingMissingException sourceField( long mappingId, long columnId, long allocationId ) {
            return new MappingMissingException( "Mapping %d cannot find source column %d on allocation %d.".formatted( mappingId, columnId, allocationId ), null, Map.of(), List.of() );
        }


        public static MappingMissingException fieldId( long mappingId, String fieldName, long allocationId ) {
            return new MappingMissingException( "Mapping %d has no column ID for input field '%s' on allocation %d.".formatted( mappingId, fieldName, allocationId ), null, Map.of(), List.of() );
        }


        public static MappingMissingException explicitSourceAllocation( long mappingId ) {
            return new MappingMissingException( "Mapping %d requires an explicit source allocation when sharing a target.".formatted( mappingId ), null, Map.of(), List.of() );
        }

    }


    public static final class Duplicate {

        private Duplicate() {
        }


        public static MappingDuplicateException physicalPosition( long mappingId, int position, long allocationId ) {
            return new MappingDuplicateException( "Mapping %d has duplicate physical column position %d in allocation %d.".formatted( mappingId, position, allocationId ), null, Map.of(), List.of() );
        }


        public static MappingDuplicateException physicalColumn( long mappingId, long columnId, long allocationId ) {
            return new MappingDuplicateException( "Mapping %d has duplicate physical column %d in allocation %d.".formatted( mappingId, columnId, allocationId ), null, Map.of(), List.of() );
        }


        public static MappingDuplicateException logicalPosition( long mappingId, int position ) {
            return new MappingDuplicateException( "Mapping %d has duplicate logical column position %d.".formatted( mappingId, position ), null, Map.of(), List.of() );
        }


        public static MappingDuplicateException logicalColumn( long mappingId, long columnId ) {
            return new MappingDuplicateException( "Mapping %d has duplicate logical column ID %d.".formatted( mappingId, columnId ), null, Map.of(), List.of() );
        }


        public static MappingDuplicateException mappingId( long mappingId ) {
            return new MappingDuplicateException( "Duplicate mapping ID %d.".formatted( mappingId ), null, Map.of(), List.of() );
        }


        public static MappingDuplicateException mappingName( String mappingName ) {
            return new MappingDuplicateException( "A mapping with this name already exists: %s".formatted( mappingName ), null, Map.of(), List.of() );
        }


        public static MappingDuplicateException sourceTargetPair() {
            return new MappingDuplicateException( "This source already has a mapping to the selected target.", null, Map.of( "sourceAllocationId", "Choose a source that is not already mapped to this target." ), List.of() );
        }


        public static MappingDuplicateException transformer( String ruleType ) {
            return new MappingDuplicateException( "Duplicate mapping transformer for %s.".formatted( ruleType ), null, Map.of(), List.of() );
        }


        public static MappingDuplicateException inputFieldId( long mappingId, long columnId, long allocationId ) {
            return new MappingDuplicateException( "Mapping %d repeats input column ID %d on allocation %d.".formatted( mappingId, columnId, allocationId ), null, Map.of(), List.of() );
        }


        public static MappingDuplicateException sourceAllocation( long mappingId, long allocationId ) {
            return new MappingDuplicateException( "Mapping %d shares source allocation %d with another mapping of the same target.".formatted( mappingId, allocationId ), null, Map.of(), List.of() );
        }

    }


    public static final class Validation {

        public static MappingValidationException documentIdentityRequired( long mappingId ) {
            return new MappingValidationException( "Document mapping %d requires exactly one whole-document identity rule.".formatted( mappingId ), null, Map.of(), List.of() );
        }

        private Validation() {
        }


        public static MappingValidationException logicalSourceNotAllowed() {
            return new MappingValidationException( "Physical-to-logical mappings resolve their source through an allocation; sourceEntityId must be null.", null, Map.of(), List.of() );
        }


        public static MappingValidationException includedColumnRequired( long mappingId ) {
            return new MappingValidationException( "Mapping %d must include at least one logical column.".formatted( mappingId ), null, Map.of(), List.of() );
        }


        public static MappingValidationException arrayFilter( String column, Throwable cause ) {
            return new MappingValidationException( "Invalid array filter for column '%s'.".formatted( column ), cause, Map.of(), List.of() );
        }


        public static MappingValidationException fields( Map<String, String> fieldErrors ) {
            return new MappingValidationException( "Correct the highlighted mapping fields.", null, fieldErrors, List.of() );
        }


        public static MappingValidationException adapterIds() {
            return new MappingValidationException( "Supply valid source adapter IDs.", null, Map.of(), List.of() );
        }


        public static MappingValidationException request() {
            return new MappingValidationException( "A JSON request object is required.", null, Map.of(), List.of() );
        }


        public static MappingValidationException malformedRequest( Throwable cause ) {
            return new MappingValidationException( "The mapping request is malformed, incomplete, or contains unsupported fields.", cause, Map.of(), List.of() );
        }


        public static MappingValidationException mappingId( String mappingId, Throwable cause ) {
            return new MappingValidationException( "Invalid mapping ID '%s' in mapping plan.".formatted( mappingId ), cause, Map.of(), List.of() );
        }


        public static MappingValidationException visibleNamespaceRequired() {
            return new MappingValidationException( "Choose a visible target namespace.", null, Map.of( "targetNamespaceId", "Hidden namespaces cannot be used as mapping targets." ), List.of() );
        }

    }


    public static final class Unsupported {

        public static MappingUnsupportedException models( long mappingId, DataModel sourceModel, DataModel targetModel ) {
            return new MappingUnsupportedException( "Mapping %d from %s to %s is not supported.".formatted( mappingId, sourceModel, targetModel ), null, Map.of(), List.of() );
        }

        public static MappingUnsupportedException documentMappingRequired( long mappingId ) {
            return new MappingUnsupportedException( "Mapping %d requires a physical-to-logical document collection binding.".formatted( mappingId ), null, Map.of(), List.of() );
        }

        public static MappingUnsupportedException relationalSourceRequired( long mappingId, long allocationId ) {
            return new MappingUnsupportedException( "Mapping %d requires a relational physical source for allocation %d.".formatted( mappingId, allocationId ), null, Map.of(), List.of() );
        }

        private Unsupported() {
        }


        public static MappingUnsupportedException relationalAdapterRequired( long mappingId, long allocationId ) {
            return new MappingUnsupportedException( "Mapping %d requires a relational adapter catalog for allocation %d.".formatted( mappingId, allocationId ), null, Map.of(), List.of() );
        }


        public static MappingUnsupportedException kind( long mappingId, MappingKind kind ) {
            return new MappingUnsupportedException( "Mapping %d has unsupported kind %s; only physical-to-logical mappings can currently be published.".formatted( mappingId, kind ), null, Map.of(), List.of() );
        }


        public static MappingUnsupportedException singleTargetPartitionRequired() {
            return new MappingUnsupportedException( "Mappings sharing a target require a single target partition.", null, Map.of(), List.of() );
        }


        public static MappingUnsupportedException refreshRule( String mappingName ) {
            return new MappingUnsupportedException( "Cannot refresh unsupported rule in mapping '%s'.".formatted( mappingName ), null, Map.of(), List.of() );
        }


        public static MappingUnsupportedException columnRule( String ruleType ) {
            return new MappingUnsupportedException( "Unsupported column mapping rule '%s'.".formatted( ruleType ), null, Map.of(), List.of() );
        }


        public static MappingUnsupportedException ruleValidation( long mappingId, String ruleType ) {
            return new MappingUnsupportedException( "Mapping %d has no snapshot validation for rule type %s.".formatted( mappingId, ruleType ), null, Map.of(), List.of() );
        }


        public static MappingUnsupportedException rule( long mappingId, String ruleType ) {
            return new MappingUnsupportedException( "Mapping %d has unsupported rule type %s.".formatted( mappingId, ruleType ), null, Map.of(), List.of() );
        }


        public static MappingUnsupportedException relationalNamespaceRequired() {
            return new MappingUnsupportedException( "Choose a relational target namespace.", null, Map.of( "targetNamespaceId", "Choose a relational target namespace." ), List.of() );
        }


        public static MappingUnsupportedException singlePhysicalSourceRequired() {
            return new MappingUnsupportedException( "Creating mappings currently requires a single physical source table.", null, Map.of(), List.of() );
        }


        public static MappingUnsupportedException existingSourceTargetRequired() {
            return new MappingUnsupportedException( "Existing-target mappings currently require an imported or mapping-backed table.", null, Map.of(), List.of() );
        }


        public static MappingUnsupportedException existingTargetPartitionRequired() {
            return new MappingUnsupportedException( "Existing-target mappings require a single target partition.", null, Map.of(), List.of() );
        }


        public static MappingUnsupportedException catalog() {
            return new MappingUnsupportedException( "Schema mappings are not supported by this catalog.", null, Map.of(), List.of() );
        }


        public static MappingUnsupportedException editor( String restriction ) {
            return new MappingUnsupportedException( "This mapping cannot be edited: %s".formatted( restriction ), null, Map.of(), List.of() );
        }


        public static MappingUnsupportedException relationalMappingRequired( long mappingId ) {
            return new MappingUnsupportedException( "Mapping %d is not a relational physical-to-logical mapping.".formatted( mappingId ), null, Map.of(), List.of() );
        }


        public static MappingUnsupportedException conversion( long mappingId, String column, AlgDataType sourceType, AlgDataType targetType ) {
            return new MappingUnsupportedException( "Mapping %d cannot convert column '%s' from %s to %s.".formatted( mappingId, column, sourceType, targetType ), null, Map.of(), List.of() );
        }


        public static MappingUnsupportedException transform( String transformType ) {
            return new MappingUnsupportedException( "Unsupported mapping transform type %s.".formatted( transformType ), null, Map.of(), List.of() );
        }


        public static MappingUnsupportedException previewExpression( String expression ) {
            return new MappingUnsupportedException( "Unsupported expression in mapped table preview: %s".formatted( expression ), null, Map.of(), List.of() );
        }

    }


    public static final class Incompatible {

        private Incompatible() {
        }


        public static MappingIncompatibleException physicalColumn( long mappingId, long columnId, long allocationId ) {
            return new MappingIncompatibleException( "Mapping %d has incompatible physical column %d in allocation %d.".formatted( mappingId, columnId, allocationId ), null, Map.of(), List.of() );
        }


        public static MappingIncompatibleException sourceModel( long mappingId, DataModel sourceModel, long allocationId, DataModel actualModel ) {
            return new MappingIncompatibleException( "Mapping %d source model %s does not match allocation %d's physical model %s.".formatted( mappingId, sourceModel, allocationId, actualModel ), null, Map.of(), List.of() );
        }


        public static MappingIncompatibleException targetModel( long mappingId, DataModel targetModel, long targetEntityId, DataModel actualModel ) {
            return new MappingIncompatibleException( "Mapping %d target model %s does not match entity %d's model %s.".formatted( mappingId, targetModel, targetEntityId, actualModel ), null, Map.of(), List.of() );
        }


        public static MappingIncompatibleException sharedColumnCoverage() {
            return new MappingIncompatibleException( "Mappings sharing a target must cover the same logical columns.", null, Map.of(), List.of() );
        }


        public static MappingIncompatibleException sourceColumn( String sourceColumn, String mappingName, String targetTable, String targetColumn ) {
            return new MappingIncompatibleException( "Source column '%s' in mapping '%s' is incompatible with target column '%s.%s'. Target schema was not changed.".formatted( sourceColumn, mappingName, targetTable, targetColumn ), null, Map.of(), List.of() );
        }


        public static MappingIncompatibleException directColumn( long mappingId, long columnId, long allocationId ) {
            return new MappingIncompatibleException( "Mapping %d cannot copy source column %d on allocation %d to an incompatible logical column.".formatted( mappingId, columnId, allocationId ), null, Map.of(), List.of() );
        }

    }


    public static final class Revision {

        private Revision() {
        }


        public static MappingRevisionException stale() {
            return new MappingRevisionException( "This mapping changed after it was opened. Reload the saved version before editing it again.", null, Map.of(), List.of() );
        }

    }


    public static final class Conflict {

        private Conflict() {
        }


        public static MappingConflictException logicalOwnerChanged() {
            return new MappingConflictException( "An existing mapping with the same logical owner is required.", null, Map.of(), List.of() );
        }


        public static MappingConflictException dropNamespaceBlocked( String namespaceName, String tableName ) {
            return new MappingConflictException( "Cannot drop namespace '%s' because it would drop source-backed table '%s'. Remove or move the mapping target, or remove the source adapter first.".formatted( namespaceName, tableName ), null, Map.of(), List.of( tableName ) );
        }


        public static MappingConflictException dropTableBlocked( String tableName ) {
            return new MappingConflictException( "Cannot drop source-backed table '%s' with DROP TABLE. Use Drop Mapping for mapping-backed targets or remove the source adapter.".formatted( tableName ), null, Map.of(), List.of( tableName ) );
        }


        public static MappingConflictException removalBlocked( String mappingName, String restriction, List<String> blockingEntities ) {
            return new MappingConflictException( "Cannot remove mapping '%s': %s".formatted( mappingName, restriction ), null, Map.of(), blockingEntities );
        }


        public static MappingConflictException schemaChangeBlocked( boolean changesTypes, List<String> blockingEntities ) {
            return new MappingConflictException( (changesTypes ? "Dependencies prevent changing this logical schema." : "Dependent views or foreign keys prevent changing this logical schema."), null, Map.of(), blockingEntities );
        }


        public static MappingConflictException namespaceMoveBlocked( List<String> blockingEntities ) {
            return new MappingConflictException( "Foreign keys prevent changing the target namespace.", null, Map.of(), blockingEntities );
        }


        public static MappingConflictException foreignKeyColumnRequired( String foreignKey ) {
            return new MappingConflictException( "Mapping excludes a column required by foreign key %s.".formatted( foreignKey ), null, Map.of(), List.of( foreignKey ) );
        }


        public static MappingConflictException viewColumnRequired( String view ) {
            return new MappingConflictException( "Mapping excludes a column required by view %s.".formatted( view ), null, Map.of(), List.of( view ) );
        }

    }


    public static final class Algebra {

        private Algebra() {
        }


        public static MappingAlgebraException physicalColumnMismatch( long mappingId, long columnId, long allocationId ) {
            return new MappingAlgebraException( "Mapping %d has inconsistent physical column %d metadata for allocation %d.".formatted( mappingId, columnId, allocationId ), null, Map.of(), List.of() );
        }


        public static MappingAlgebraException columnPlacementMismatch( long mappingId, long columnId, long allocationId ) {
            return new MappingAlgebraException( "Mapping %d has mismatched column placement %d for allocation %d.".formatted( mappingId, columnId, allocationId ), null, Map.of(), List.of() );
        }


        public static MappingAlgebraException physicalBindingMismatch( long mappingId, long allocationId ) {
            return new MappingAlgebraException( "Mapping %d does not match allocation %d and its physical table.".formatted( mappingId, allocationId ), null, Map.of(), List.of() );
        }


        public static MappingAlgebraException sourceBindingsAmbiguous() {
            return new MappingAlgebraException( "The existing target has ambiguous physical source bindings.", null, Map.of(), List.of() );
        }


        public static MappingAlgebraException allocationMismatch( long mappingId, long allocationId ) {
            return new MappingAlgebraException( "Mapping %d does not match allocation %d.".formatted( mappingId, allocationId ), null, Map.of(), List.of() );
        }


        public static MappingAlgebraException physicalNameMismatch( long mappingId, String actualName, String expectedName, long columnId, long allocationId ) {
            return new MappingAlgebraException( "Mapping %d has physical name %s instead of %s for source column %d on allocation %d.".formatted( mappingId, actualName, expectedName, columnId, allocationId ), null, Map.of(), List.of() );
        }


        public static MappingAlgebraException sourceFieldMismatch( long mappingId, long columnId, long allocationId ) {
            return new MappingAlgebraException( "Mapping %d has inconsistent input metadata for source column %d on allocation %d.".formatted( mappingId, columnId, allocationId ), null, Map.of(), List.of() );
        }


        public static MappingAlgebraException targetFieldMismatch( long mappingId, long columnId, long allocationId ) {
            return new MappingAlgebraException( "Mapping %d cannot resolve logical column %d on allocation %d.".formatted( mappingId, columnId, allocationId ), null, Map.of(), List.of() );
        }


        public static MappingAlgebraException resultTypeMismatch( long mappingId, String fieldName ) {
            return new MappingAlgebraException( "Mapping %d produces an incompatible type for target field %s.".formatted( mappingId, fieldName ), null, Map.of(), List.of() );
        }


        public static MappingAlgebraException physicalSourceMismatch( long mappingId, long physicalEntityId ) {
            return new MappingAlgebraException( "Mapping %d does not use physical source %d.".formatted( mappingId, physicalEntityId ), null, Map.of(), List.of() );
        }


        public static MappingAlgebraException physicalScanRequired( long mappingId ) {
            return new MappingAlgebraException( "Mapping %d requires a physical source scan.".formatted( mappingId ), null, Map.of(), List.of() );
        }


        public static MappingAlgebraException inputCount( int inputCount ) {
            return new MappingAlgebraException( "Mapping plan requires exactly one input; received %d.".formatted( inputCount ), null, Map.of(), List.of() );
        }

    }


    public static final class Execution {

        private Execution() {
        }


        public static MappingExecutionException conversionFailed( String mappingName, String targetTableName, String column, PolyValue value,
                String sourceType, String targetType, Throwable cause ) {
            return new MappingExecutionException( "Mapping '%s', target '%s', column '%s': cannot convert value %s from %s to %s."
                    .formatted( mappingName, targetTableName, column, displayValue( value ), sourceType, targetType ), cause, Map.of(), List.of() );
        }


        public static MappingExecutionException nullNotAllowed( String mappingName, String targetTableName, String column, String sourceType, String targetType ) {
            return new MappingExecutionException( "Mapping '%s', target '%s', column '%s': cannot map value NULL from %s to %s because the target column does not allow NULL."
                    .formatted( mappingName, targetTableName, column, sourceType, targetType ), null, Map.of(), List.of() );
        }


        private static String displayValue( PolyValue value ) {
            if ( ScalarConversions.isNull( value ) ) {
                return "NULL";
            }
            String text = value.isString() ? value.asString().toQuotedJson() : value.toJson();
            return text != null && text.length() > 160 ? text.substring( 0, 160 ) + "…" : text;
        }

    }


    public static final class Operation {

        private Operation() {
        }


        public static MappingOperationException scanFailed( String mappingName, long adapterId, Throwable cause ) {
            return new MappingOperationException( "Could not read source for mapping '%s' on adapter %d.".formatted( mappingName, adapterId ), cause, Map.of(), List.of() );
        }


        public static MappingOperationException discoveryFailed( String mappingName, Throwable cause ) {
            return new MappingOperationException( "Could not discover source columns for mapping '%s'.".formatted( mappingName ), cause, Map.of(), List.of() );
        }


        public static MappingOperationException refreshFailed( String targetName, Throwable cause ) {
            return new MappingOperationException( "Could not refresh sources for mapped target '%s'.".formatted( targetName ), cause, Map.of(), List.of() );
        }


        public static MappingOperationException commitFailed( Throwable cause ) {
            return new MappingOperationException( "Could not commit the mapping change.", cause, Map.of(), List.of() );
        }


        public static MappingOperationException changeFailed( Throwable cause ) {
            return new MappingOperationException( "Could not apply the mapping change.", cause, Map.of(), List.of() );
        }


        public static MappingOperationException requestFailed( Throwable cause ) {
            return new MappingOperationException( "The mapping request could not be completed. Reload the saved version to check its state.", cause, Map.of(), List.of() );
        }

    }

}
