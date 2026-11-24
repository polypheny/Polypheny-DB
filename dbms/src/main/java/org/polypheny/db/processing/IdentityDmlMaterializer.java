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

package org.polypheny.db.processing;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.index.Index;
import org.polypheny.db.adapter.index.IndexManager;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.exceptions.ConstraintViolationException;
import org.polypheny.db.algebra.logical.common.LogicalConditionalExecute;
import org.polypheny.db.algebra.logical.relational.LogicalModifyCollect;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelUnion;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalDefaultValue;
import org.polypheny.db.catalog.entity.logical.IdentityColumn;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Statement.GeneratedKeySelection;
import org.polypheny.db.transaction.Statement.InsertReturning;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;


/** Resolves identity defaults once, before constraint checking and write routing. */
final class IdentityDmlMaterializer {

    private IdentityDmlMaterializer() {
    }


    static AlgRoot materialize( AlgRoot root, Statement statement, AlgDataType parameterRowType, boolean validateBeforeRouting ) {
        if ( root.alg instanceof LogicalRelModify modify && modify.getEntity() instanceof LogicalTable table ) {
            if ( modify.isInsert() || modify.isUpdate() ) {
                validateIdentityLayout( modify, table, statement );
            }
            if ( modify.isInsert() ) {
                return materializeInsert( root, modify, table, statement, parameterRowType );
            }
            if ( usesIdentityUpdate( root ) ) {
                return materializeUpdate( root, modify, table, statement, parameterRowType, validateBeforeRouting );
            }
        }
        return root;
    }


    private static void validateIdentityLayout( LogicalRelModify modify, LogicalTable table, Statement statement ) {
        Snapshot planned = modify.getCluster().getSnapshot();
        Snapshot locked = statement.getTransaction().getSnapshot();
        if ( planned == locked ) {
            return;
        }
        List<LogicalColumn> plannedColumns = planned.rel().getColumns( table.id );
        List<LogicalColumn> lockedColumns = locked.rel().getColumns( table.id );
        if ( !statement.isInsertRowCaptureRequested()
                && plannedColumns.stream().noneMatch( column -> column.getIdentity() != null )
                && lockedColumns.stream().noneMatch( column -> column.getIdentity() != null ) ) {
            return;
        }

        // RESTART and sequence options may replace the UUID. They are read from the
        // locked snapshot. The row layout, literal defaults and mode belong to the plan.
        AlgDataTypeFactory typeFactory = modify.getCluster().getTypeFactory();
        if ( !columnLayout( plannedColumns, typeFactory ).equals( columnLayout( lockedColumns, typeFactory ) )
                || !planned.rel().getTable( table.id ).map( logicalTable -> logicalTable.primaryKey )
                        .equals( locked.rel().getTable( table.id ).map( logicalTable -> logicalTable.primaryKey ) ) ) {
            throw new GenericRuntimeException( "Identity or column layout of '%s' changed during planning. Retry the statement.", table.name );
        }
    }


    private static List<ColumnLayout> columnLayout( List<LogicalColumn> columns, AlgDataTypeFactory typeFactory ) {
        return columns.stream().sorted( Comparator.comparingInt( column -> column.position ) )
                .map( column -> new ColumnLayout( column.id, column.name, column.getAlgDataType( typeFactory ), column.nullable,
                        column.getLiteralDefaultValue(), column.getIdentity() == null ? null : column.getIdentity().mode() ) )
                .toList();
    }


    static boolean usesIdentityUpdate( AlgRoot root ) {
        return root.alg instanceof LogicalRelModify modify && modify.isUpdate()
                && modify.getSourceExpressions().stream().anyMatch( expression -> expression.getKind() == Kind.DEFAULT );
    }


    private static AlgRoot materializeInsert( AlgRoot root, LogicalRelModify modify, LogicalTable table, Statement statement, AlgDataType parameterRowType ) {
        AlgNode source = modify.getInput();
        AlgDataType rowType = table.getTupleType();
        List<AlgDataTypeField> targetFields = rowType.getFields();
        List<AlgDataTypeField> sourceFields = source.getTupleType().getFields();
        List<IdentityField> identities = new ArrayList<>();
        int flagIndex = targetFields.size();
        for ( AlgDataTypeField field : targetFields ) {
            if ( flagIndex == sourceFields.size() ) {
                break;
            }
            if ( sourceFields.get( flagIndex ).getName().equals( LogicalRelModify.identityDefaultFieldName( field.getId() ) ) ) {
                LogicalColumn column = statement.getTransaction().getSnapshot().rel().getColumn( field.getId() ).orElseThrow();
                if ( column.getIdentity() == null ) {
                    throw new GenericRuntimeException( "Identity definition changed for column '%s'. Retry the insert.", column.name );
                }
                identities.add( new IdentityField( field.getIndex(), flagIndex++, column.getIdentity(), field.getType().getPolyType() ) );
            }
        }
        if ( identities.isEmpty() && !statement.isInsertRowCaptureRequested() ) {
            return root;
        }
        if ( flagIndex != sourceFields.size() ) {
            throw new GenericRuntimeException( "Invalid insert row layout for table '%s'.", table.name );
        }

        List<List<PolyValue>> sourceRows = readRows( source, statement, parameterRowType );
        for ( IdentityField identity : identities ) {
            int count = (int) sourceRows.stream().filter( row -> row.get( identity.flagIndex() ).asBoolean().value ).count();
            if ( count == 0 ) {
                continue;
            }
            List<Long> values = Catalog.getInstance().getIdentitySequences().reserve( identity.definition(), count );
            int index = 0;
            for ( List<PolyValue> row : sourceRows ) {
                if ( row.get( identity.flagIndex() ).asBoolean().value ) {
                    row.set( identity.columnIndex(), PolyValue.fromType( values.get( index++ ), identity.type() ) );
                }
            }
        }

        ImmutableList.Builder<ImmutableList<RexLiteral>> tuples = ImmutableList.builder();
        List<List<PolyValue>> capturedRows = new ArrayList<>();
        GeneratedKeySelection keySelection = statement.getGeneratedKeySelection();
        boolean generatedOnly = keySelection != null && keySelection.names().isEmpty() && keySelection.indexes().isEmpty();
        for ( List<PolyValue> row : sourceRows ) {
            ImmutableList.Builder<RexLiteral> tuple = ImmutableList.builder();
            for ( AlgDataTypeField field : targetFields ) {
                tuple.add( toLiteral( row.get( field.getIndex() ), field ) );
            }
            ImmutableList<RexLiteral> inserted = tuple.build();
            tuples.add( inserted );
            if ( statement.isInsertRowCaptureRequested() && (!generatedOnly
                    || identities.stream().anyMatch( identity -> row.get( identity.flagIndex() ).asBoolean().value )) ) {
                List<PolyValue> captured = new ArrayList<>( row.subList( 0, targetFields.size() ) );
                for ( AlgDataTypeField field : targetFields ) {
                    if ( field.getType().getPolyType() == PolyType.DECIMAL ) {
                        int index = field.getIndex();
                        captured.set( index, inserted.get( index ).getValue() );
                    }
                }
                capturedRows.add( captured );
            }
        }
        if ( statement.isInsertRowCaptureRequested() ) {
            captureStoredRows( modify, table, statement, parameterRowType, targetFields, capturedRows );
        }

        // Parameter sets have already been expanded into concrete rows, once each.
        statement.getDataContext().resetParameterValues();
        statement.setMaterializedDmlRowCount( sourceRows.size() );
        AlgNode values = LogicalRelValues.create( modify.getCluster(), rowType, tuples.build() );
        return root.withAlg( modify.copy( modify.getTraitSet(), List.of( values ) ) );
    }


    private static void captureStoredRows( LogicalRelModify modify, LogicalTable table, Statement statement,
            AlgDataType parameterRowType, List<AlgDataTypeField> fields, List<List<PolyValue>> rows ) {
        InsertReturning projection = statement.getInsertReturning() != null ? statement.getInsertReturning() : statement.getGeneratedKeysProjection();
        List<AlgDataTypeField> decimals = projection.ordinals().stream().distinct().map( fields::get )
                .filter( field -> rows.stream().anyMatch( row -> needsDecimalReadback( field, row.get( field.getIndex() ) ) ) )
                .toList();
        if ( decimals.isEmpty() ) {
            statement.setInsertedRows( rows );
            return;
        }
        List<StoredDecimalBatch> batches = prepareDecimalReadback( modify, table, statement, parameterRowType, fields, decimals, rows );
        statement.setInsertedRowsReader( () -> readStoredDecimals( table, statement, parameterRowType, decimals, rows, batches ) );
    }


    private static boolean needsDecimalReadback( AlgDataTypeField field, PolyValue value ) {
        AlgDataType type = field.getType();
        return type.getPolyType() == PolyType.DECIMAL && PolyTypeUtil.hasScale( type )
                && value != null && !value.isNull() && value.asNumber().bigDecimalValue().scale() != type.getScale();
    }


    // Storage adapters can round decimal assignments differently. Read the values in the same transaction
    // after INSERT, projecting the input ordinal so the output follows the original statement's row order.
    private static List<StoredDecimalBatch> prepareDecimalReadback( LogicalRelModify modify, LogicalTable table, Statement statement,
            AlgDataType parameterRowType, List<AlgDataTypeField> fields, List<AlgDataTypeField> decimals, List<List<PolyValue>> rows ) {
        List<Long> primaryKey = statement.getTransaction().getSnapshot().rel().getPrimaryKey( table.primaryKey ).orElseThrow().fieldIds;
        List<AlgDataTypeField> keyFields = fields.stream().filter( field -> primaryKey.contains( field.getId() ) ).toList();
        RexBuilder rex = modify.getCluster().getRexBuilder();
        List<StoredDecimalBatch> batches = new ArrayList<>();
        int batchSize = 128;
        for ( int offset = 0; offset < rows.size(); offset += batchSize ) {
            List<List<PolyValue>> batch = rows.subList( offset, Math.min( offset + batchSize, rows.size() ) );
            LogicalRelScan scan = LogicalRelScan.create( modify.getCluster(), table );
            List<RexNode> predicates = new ArrayList<>();
            List<RexNode> matches = new ArrayList<>();
            for ( int index = 0; index < batch.size(); index++ ) {
                List<PolyValue> row = batch.get( index );
                List<RexNode> keyMatch = keyFields.stream().map( field -> rex.makeCall( OperatorRegistry.get( OperatorName.EQUALS ),
                        rex.makeInputRef( scan, field.getIndex() ), storedKeyLiteral( rex, field, row.get( field.getIndex() ) ) ) ).toList();
                RexNode predicate = RexUtil.composeConjunction( rex, keyMatch, false );
                predicates.add( predicate );
                matches.add( predicate );
                matches.add( rex.makeExactLiteral( BigDecimal.valueOf( index ) ) );
            }
            matches.add( rex.makeExactLiteral( BigDecimal.valueOf( -1 ) ) );
            List<RexNode> expressions = new ArrayList<>();
            expressions.add( rex.makeCall( OperatorRegistry.get( OperatorName.CASE ), matches ) );
            decimals.forEach( field -> expressions.add( rex.makeInputRef( scan, field.getIndex() ) ) );
            List<String> aliases = new ArrayList<>();
            aliases.add( "insert_ordinal" );
            decimals.forEach( field -> aliases.add( field.getName() ) );
            AlgNode filtered = LogicalRelFilter.create( scan, RexUtil.composeDisjunction( rex, predicates, false ) );
            AlgNode projected = LogicalRelProject.create( filtered, expressions, aliases );
            boolean castsKey = batch.stream().anyMatch( row -> keyFields.stream()
                    .anyMatch( field -> needsDecimalKeyCoercion( field, row.get( field.getIndex() ) ) ) );
            // A dialect or optimizer must not turn a lookup cast into a different, already-existing key.
            // Detect preexisting matches and refuse to expose them as rows written by this INSERT.
            Set<Integer> existing = new HashSet<>();
            if ( castsKey ) {
                readDecimalBatch( projected, statement, parameterRowType )
                        .forEach( stored -> existing.add( stored.get( 0 ).asNumber().intValue() ) );
            }
            batches.add( new StoredDecimalBatch( batch, projected, existing ) );
        }
        return batches;
    }


    private static List<List<PolyValue>> readStoredDecimals( LogicalTable table, Statement statement, AlgDataType parameterRowType,
            List<AlgDataTypeField> decimals, List<List<PolyValue>> rows, List<StoredDecimalBatch> batches ) {
        for ( StoredDecimalBatch batch : batches ) {
            Set<Integer> matched = new HashSet<>();
            for ( List<PolyValue> stored : readDecimalBatch( batch.query(), statement, parameterRowType ) ) {
                int index = stored.get( 0 ).asNumber().intValue();
                if ( index < 0 || index >= batch.rows().size() || batch.existing().contains( index ) || !matched.add( index ) ) {
                    throw new GenericRuntimeException( "Could not match an inserted row in '%s' to its stored values.", table.name );
                }
                for ( int column = 0; column < decimals.size(); column++ ) {
                    batch.rows().get( index ).set( decimals.get( column ).getIndex(), stored.get( column + 1 ) );
                }
            }
            if ( matched.size() != batch.rows().size() ) {
                throw new GenericRuntimeException( "Could not read back all inserted rows in '%s'.", table.name );
            }
        }
        return rows;
    }


    private static List<List<PolyValue>> readDecimalBatch( AlgNode projected, Statement statement, AlgDataType parameterRowType ) {
        DataContext context = statement.getDataContext();
        List<Map<Long, PolyValue>> originalValues = context.getParameterValues();
        Map<Long, AlgDataType> originalTypes = Map.copyOf( context.getParameterTypes() );
        context.resetParameterValues();
        try {
            List<List<PolyValue>> stored = new ArrayList<>();
            readRows( projected, statement, parameterRowType, Map.of(), stored );
            return stored;
        } finally {
            context.setParameterTypes( originalTypes );
            context.setParameterValues( originalValues );
        }
    }


    private static boolean needsDecimalKeyCoercion( AlgDataTypeField field, PolyValue value ) {
        return needsDecimalReadback( field, value )
                && value.asNumber().bigDecimalValue().stripTrailingZeros().scale() > field.getType().getScale();
    }


    private static RexNode storedKeyLiteral( RexBuilder rex, AlgDataTypeField field, PolyValue value ) {
        if ( needsDecimalKeyCoercion( field, value ) ) {
            // Let the store apply its own precision and scale conversion to a decimal primary key.
            return rex.makeAbstractCast( field.getType(), rex.makeExactLiteral( value.asNumber().bigDecimalValue() ) );
        }
        return toLiteral( value, field );
    }


    private record StoredDecimalBatch( List<List<PolyValue>> rows, AlgNode query, Set<Integer> existing ) {
    }


    private static AlgRoot materializeUpdate( AlgRoot root, LogicalRelModify modify, LogicalTable table,
            Statement statement, AlgDataType parameterRowType, boolean validateBeforeRouting ) {
        Snapshot snapshot = statement.getTransaction().getSnapshot();
        AlgDataType rowType = table.getTupleType();
        List<AlgDataTypeField> targetFields = rowType.getFields();
        List<AlgDataTypeField> updateFields = modify.getUpdateColumns().stream().map( name -> rowType.getField( name, true, false ) ).toList();
        List<List<PolyValue>> sourceRows = readRows( modify.getInput(), statement, parameterRowType );
        statement.getDataContext().resetParameterValues();

        List<List<Long>> generated = new ArrayList<>();
        for ( int i = 0; i < updateFields.size(); i++ ) {
            if ( modify.getSourceExpressions().get( i ).getKind() != Kind.DEFAULT ) {
                generated.add( List.of() );
                continue;
            }
            LogicalColumn column = snapshot.rel().getColumn( updateFields.get( i ).getId() ).orElseThrow();
            if ( column.getIdentity() == null ) {
                throw new GenericRuntimeException( "Identity definition changed for column '%s'. Retry the update.", column.name );
            }
            generated.add( sourceRows.isEmpty() ? List.of() : Catalog.getInstance().getIdentitySequences().reserve( column.getIdentity(), sourceRows.size() ) );
        }

        List<Long> primaryKey = snapshot.rel().getPrimaryKey( table.primaryKey ).orElseThrow().fieldIds;
        Set<Long> predicates = new LinkedHashSet<>( primaryKey );
        PartitionProperty partition = snapshot.alloc().getPartitionProperty( table.id ).orElseThrow();
        if ( partition.isPartitioned && updateFields.stream().anyMatch( field -> field.getId() == partition.partitionColumnId ) ) {
            predicates.add( partition.partitionColumnId );
        }
        List<AlgDataTypeField> predicateFields = targetFields.stream().filter( field -> predicates.contains( field.getId() ) ).toList();

        List<AlgNode> updates = new ArrayList<>();
        List<List<PolyValue>> replacementRows = new ArrayList<>();
        for ( int i = 0; i < sourceRows.size(); i++ ) {
            List<PolyValue> source = sourceRows.get( i );
            List<PolyValue> replacement = new ArrayList<>( source.subList( 0, targetFields.size() ) );
            List<RexNode> assignments = new ArrayList<>();
            for ( int j = 0; j < updateFields.size(); j++ ) {
                AlgDataTypeField field = updateFields.get( j );
                PolyValue value = generated.get( j ).isEmpty()
                        ? source.get( targetFields.size() + j )
                        : PolyValue.fromType( generated.get( j ).get( i ), field.getType().getPolyType() );
                RexLiteral literal = toLiteral( value, field );
                assignments.add( literal );
                replacement.set( field.getIndex(), literal.getValue() );
            }
            updates.add( updateOneRow( modify, table, targetFields, predicateFields, source, assignments ) );
            replacementRows.add( replacement );
        }

        if ( updates.isEmpty() ) {
            RexBuilder rex = modify.getCluster().getRexBuilder();
            List<RexNode> noOpAssignments = new ArrayList<>();
            for ( AlgDataTypeField field : updateFields ) {
                noOpAssignments.add( rex.makeNullLiteral( field.getType() ) );
            }
            updates.add( updateOneRow( modify, table, targetFields, predicateFields, List.of(), noOpAssignments ) );
        } else {
            List<AlgDataTypeField> primaryFields = targetFields.stream().filter( field -> primaryKey.contains( field.getId() ) ).toList();
            validateUpdateKeys( modify, table, statement, parameterRowType, primaryFields, sourceRows, replacementRows );
            if ( validateBeforeRouting ) {
                validateBeforeRouting( root, modify, statement, parameterRowType, targetFields, updateFields, sourceRows, replacementRows );
            }
            if ( RuntimeConfig.POLYSTORE_INDEXES_ENABLED.getBoolean() ) {
                updateIndexes( table, statement, targetFields, sourceRows, replacementRows );
            }
        }
        statement.setMaterializedDmlRowCount( sourceRows.size() );
        return root.withAlg( updates.size() == 1 ? updates.get( 0 ) : LogicalModifyCollect.create( updates, true ) );
    }


    /** Runs the strategy's prechecks for the whole input while the old values are still visible. */
    private static void validateBeforeRouting(
            AlgRoot root, LogicalRelModify modify, Statement statement, AlgDataType parameterRowType,
            List<AlgDataTypeField> targetFields, List<AlgDataTypeField> updateFields,
            List<List<PolyValue>> sourceRows, List<List<PolyValue>> replacementRows ) {
        AlgDataTypeFactory.Builder inputType = modify.getCluster().getTypeFactory().builder();
        targetFields.forEach( inputType::add );
        List<String> sourceNames = modify.getInput().getTupleType().getFieldNames();
        for ( int i = 0; i < updateFields.size(); i++ ) {
            inputType.add( null, sourceNames.get( targetFields.size() + i ), null, updateFields.get( i ).getType() );
        }
        ImmutableList.Builder<ImmutableList<RexLiteral>> tuples = ImmutableList.builder();
        for ( int i = 0; i < sourceRows.size(); i++ ) {
            ImmutableList.Builder<RexLiteral> tuple = ImmutableList.builder();
            List<PolyValue> previous = sourceRows.get( i );
            List<PolyValue> replacement = replacementRows.get( i );
            targetFields.forEach( field -> tuple.add( toLiteral( previous.get( field.getIndex() ), field ) ) );
            updateFields.forEach( field -> tuple.add( toLiteral( replacement.get( field.getIndex() ), field ) ) );
            tuples.add( tuple.build() );
        }
        AlgNode input = LogicalRelValues.create( modify.getCluster(), inputType.build(), tuples.build() );
        RexBuilder rex = modify.getCluster().getRexBuilder();
        List<RexNode> assignments = new ArrayList<>();
        for ( int i = 0; i < updateFields.size(); i++ ) {
            assignments.add( rex.makeInputRef( input, targetFields.size() + i ) );
        }
        LogicalRelModify validation = LogicalRelModify.create( modify.getEntity(), input, Operation.UPDATE,
                modify.getUpdateColumns(), assignments, modify.isFlattened() );
        AlgNode checks = ConstraintEnforceAttacher.enforceConstraintBeforeQuery( root.withAlg( validation ), statement ).alg;
        while ( checks instanceof LogicalConditionalExecute check ) {
            if ( !readRows( check.getLeft(), statement, parameterRowType ).isEmpty() ) {
                throw new ConstraintViolationException( check.getExceptionMessage(), ConstraintViolationException.sqlStateFor( check.getExceptionClass() ) );
            }
            checks = check.getRight();
        }
    }


    private static void validateUpdateKeys(
            LogicalRelModify modify, LogicalTable table, Statement statement, AlgDataType parameterRowType,
            List<AlgDataTypeField> keyFields, List<List<PolyValue>> oldRows, List<List<PolyValue>> newRows ) {
        List<List<PolyValue>> oldKeys = oldRows.stream().map( row -> primaryKey( row, keyFields ) ).toList();
        Set<List<PolyValue>> oldOwners = new HashSet<>( oldKeys );
        if ( oldOwners.size() != oldKeys.size() ) {
            throw new GenericRuntimeException( "Identity update on '%s' requires a unique primary key for every matching row.", table.name );
        }

        Set<List<PolyValue>> newKeys = new HashSet<>();
        for ( int i = 0; i < newRows.size(); i++ ) {
            List<PolyValue> newKey = primaryKey( newRows.get( i ), keyFields );
            if ( !newKeys.add( newKey ) || (!newKey.equals( oldKeys.get( i ) ) && oldOwners.contains( newKey )) ) {
                throw new GenericRuntimeException( "Identity update on '%s' would reuse another row's primary key.", table.name );
            }
        }

        // A declared primary key is not necessarily enforced by every Polypheny store.
        // Read the relevant keys under the same lock to prove each write will target one row.
        Set<List<PolyValue>> candidates = new LinkedHashSet<>( oldKeys );
        candidates.addAll( newKeys );
        RexBuilder rex = modify.getCluster().getRexBuilder();
        AlgNode scan = LogicalRelScan.create( modify.getCluster(), table );
        List<RexNode> keyRefs = keyFields.stream().<RexNode>map( field -> rex.makeInputRef( scan, field.getIndex() ) ).toList();
        List<String> keyNames = keyFields.stream().map( AlgDataTypeField::getName ).toList();
        List<List<PolyValue>> checks = new ArrayList<>( candidates );
        Set<List<PolyValue>> seen = new HashSet<>();
        int batchSize = 128;
        for ( int offset = 0; offset < checks.size(); offset += batchSize ) {
            List<RexNode> alternatives = new ArrayList<>();
            for ( List<PolyValue> key : checks.subList( offset, Math.min( offset + batchSize, checks.size() ) ) ) {
                List<RexNode> parts = new ArrayList<>();
                for ( int i = 0; i < keyFields.size(); i++ ) {
                    parts.add( rex.makeCall( OperatorRegistry.get( OperatorName.EQUALS ), keyRefs.get( i ), toLiteral( key.get( i ), keyFields.get( i ) ) ) );
                }
                alternatives.add( RexUtil.composeConjunction( rex, parts, false ) );
            }
            AlgNode filtered = LogicalRelFilter.create( scan, RexUtil.composeDisjunction( rex, alternatives, false ) );
            AlgNode projected = LogicalRelProject.create( filtered, keyRefs, keyNames );
            for ( List<PolyValue> row : readRows( projected, statement, parameterRowType ) ) {
                List<PolyValue> key = new ArrayList<>();
                for ( int i = 0; i < keyFields.size(); i++ ) {
                    key.add( toLiteral( row.get( i ), keyFields.get( i ) ).getValue() );
                }
                if ( !oldOwners.contains( key ) || !seen.add( key ) ) {
                    throw new GenericRuntimeException( "Identity update on '%s' would reuse another row's primary key.", table.name );
                }
            }
        }
        if ( !seen.equals( oldOwners ) ) {
            throw new GenericRuntimeException( "Identity update on '%s' could not identify each original row by its primary key.", table.name );
        }
    }


    private static List<PolyValue> primaryKey( List<PolyValue> row, List<AlgDataTypeField> keyFields ) {
        return keyFields.stream().map( field -> toLiteral( row.get( field.getIndex() ), field ).getValue() ).toList();
    }


    private static LogicalRelModify updateOneRow(
            LogicalRelModify modify, LogicalTable table, List<AlgDataTypeField> fields, List<AlgDataTypeField> keys,
            List<PolyValue> oldValues, List<? extends RexNode> assignments ) {
        RexBuilder rex = modify.getCluster().getRexBuilder();
        AlgNode scan = LogicalRelScan.create( modify.getCluster(), table );
        List<RexNode> conditions = new ArrayList<>();
        for ( AlgDataTypeField field : keys ) {
            if ( oldValues.isEmpty() ) {
                break;
            }
            RexNode ref = rex.makeInputRef( scan, field.getIndex() );
            RexLiteral prior = toLiteral( oldValues.get( field.getIndex() ), field );
            conditions.add( prior.getValue() == null
                    ? rex.makeCall( OperatorRegistry.get( OperatorName.IS_NULL ), ref )
                    : rex.makeCall( OperatorRegistry.get( OperatorName.EQUALS ), ref, prior ) );
        }
        RexNode condition = oldValues.isEmpty() ? rex.makeLiteral( false ) : RexUtil.composeConjunction( rex, conditions, false );
        AlgNode filtered = LogicalRelFilter.create( scan, condition );
        List<RexNode> projections = new ArrayList<>();
        for ( AlgDataTypeField field : fields ) {
            projections.add( rex.makeInputRef( filtered, field.getIndex() ) );
        }
        projections.addAll( assignments );
        AlgNode input = LogicalRelProject.create( filtered, projections, modify.getInput().getTupleType().getFieldNames() );
        return LogicalRelModify.create( table, input, Operation.UPDATE, modify.getUpdateColumns(), assignments, modify.isFlattened() );
    }


    private static void updateIndexes(
            LogicalTable table, Statement statement, List<AlgDataTypeField> fields, List<List<PolyValue>> oldRows, List<List<PolyValue>> newRows ) {
        var namespace = statement.getTransaction().getSnapshot().getNamespace( table.namespaceId ).orElseThrow();
        for ( Index index : IndexManager.getInstance().getIndices( namespace, table ) ) {
            List<Integer> indexed = index.getColumns().stream().map( name -> fieldIndex( fields, name ) ).toList();
            List<Integer> targets = index.getTargetColumns().stream().map( name -> fieldIndex( fields, name ) ).toList();
            List<Pair<List<PolyValue>, List<PolyValue>>> deleted = new ArrayList<>();
            List<Pair<List<PolyValue>, List<PolyValue>>> inserted = new ArrayList<>();
            for ( int i = 0; i < oldRows.size(); i++ ) {
                List<PolyValue> oldRow = oldRows.get( i );
                List<PolyValue> newRow = newRows.get( i );
                Pair<List<PolyValue>, List<PolyValue>> oldEntry = new Pair<>( indexed.stream().map( oldRow::get ).toList(), targets.stream().map( oldRow::get ).toList() );
                Pair<List<PolyValue>, List<PolyValue>> newEntry = new Pair<>( indexed.stream().map( newRow::get ).toList(), targets.stream().map( newRow::get ).toList() );
                if ( !oldEntry.equals( newEntry ) ) {
                    deleted.add( oldEntry );
                    inserted.add( newEntry );
                }
            }
            index.deleteAllPrimary( statement.getTransaction().getXid(), deleted );
            index.insertAll( statement.getTransaction().getXid(), inserted );
        }
    }


    private static int fieldIndex( List<AlgDataTypeField> fields, String name ) {
        return fields.stream().filter( field -> field.getName().equals( name ) ).findFirst().orElseThrow().getIndex();
    }


    private static RexLiteral toLiteral( PolyValue value, AlgDataTypeField field ) {
        AlgDataType type = field.getType();
        if ( value == null || value.isNull() ) {
            if ( !type.isNullable() ) {
                throw new GenericRuntimeException( "Column '%s' does not allow NULL.", field.getName() );
            }
            return new RexLiteral( null, type, type.getPolyType(), true );
        }
        Pair<PolyValue, PolyType> converted = RexLiteral.convertType( value, type );
        return new RexLiteral( converted.left, type, converted.right, true );
    }


    private static List<List<PolyValue>> readRows( AlgNode source, Statement statement, AlgDataType parameterRowType ) {
        DataContext dataContext = statement.getDataContext();
        List<Map<Long, PolyValue>> parameters = dataContext.getParameterValues();
        List<List<PolyValue>> rows = new ArrayList<>();
        if ( parameters.isEmpty() ) {
            readRows( source, statement, parameterRowType, Map.of(), rows );
            return rows;
        }
        for ( Map<Long, PolyValue> binding : parameters ) {
            readRows( source, statement, parameterRowType, binding, rows );
        }
        return rows;
    }


    private static void readRows(
            AlgNode source, Statement statement, AlgDataType parameterRowType, Map<Long, PolyValue> binding, List<List<PolyValue>> rows ) {
        if ( source instanceof LogicalRelUnion union && union.all ) {
            for ( AlgNode input : union.getInputs() ) {
                readRows( input, statement, parameterRowType, binding, rows );
            }
            return;
        }
        if ( source instanceof LogicalRelValues values ) {
            for ( ImmutableList<RexLiteral> tuple : values.getTuples() ) {
                rows.add( new ArrayList<>( tuple.stream().map( RexLiteral::getValue ).toList() ) );
            }
            return;
        }
        if ( source instanceof LogicalRelProject project && project.getInput() instanceof LogicalRelValues values
                && values.getTuples().size() == 1 && project.getProjects().stream().allMatch( expression ->
                expression instanceof RexLiteral || expression instanceof RexDynamicParam param && binding.containsKey( param.getIndex() ) ) ) {
            List<PolyValue> row = new ArrayList<>();
            for ( RexNode expression : project.getProjects() ) {
                row.add( expression instanceof RexLiteral literal ? literal.getValue() : binding.get( ((RexDynamicParam) expression).getIndex() ) );
            }
            rows.add( row );
            return;
        }

        // A source SELECT is a query, whereas JDBC batch bindings are individual executions.
        // Execute it for each binding instead of submitting a JDBC batch of SELECT statements.
        DataContext context = statement.getDataContext();
        List<Map<Long, PolyValue>> originalValues = context.getParameterValues();
        Map<Long, AlgDataType> originalTypes = Map.copyOf( context.getParameterTypes() );
        try {
            context.setParameterValues( binding.isEmpty() ? List.of() : List.of( binding ) );
            AlgRoot query = AlgRoot.of( source, Kind.SELECT );
            PolyImplementation implementation = statement.getQueryProcessor().prepareQuery( query, parameterRowType, false, true, false );
            rows.addAll( implementation.execute( statement, -1 ).getAllRowsAndClose() );
        } finally {
            context.setParameterTypes( originalTypes );
            context.setParameterValues( originalValues );
        }
    }


    private record ColumnLayout( long id, String name, AlgDataType type, boolean nullable, LogicalDefaultValue defaultValue, IdentityColumn.Mode mode ) {
    }


    private record IdentityField( int columnIndex, int flagIndex, IdentityColumn definition, PolyType type ) {
    }

}
