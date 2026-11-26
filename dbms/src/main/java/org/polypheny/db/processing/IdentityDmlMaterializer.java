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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DataStore.NumericAssignment;
import org.polypheny.db.adapter.DataStore.NumericExpression;
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
import org.polypheny.db.catalog.entity.allocation.AllocationPlacement;
import org.polypheny.db.catalog.entity.logical.IdentityColumn;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.partition.PartitionManager;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.processing.shuttles.QueryParameterizer;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Statement.GeneratedKeySelection;
import org.polypheny.db.transaction.Statement.InsertReturning;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.numerical.PolyLong;
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

        List<List<AssignmentInput>> assignmentInputs = new ArrayList<>();
        Optional<List<List<PolyValue>>> nativeRows = Optional.empty();
        if ( identities.isEmpty() && !isScalarInsertSource( source )
                && statement.getQueryProcessor() instanceof AbstractQueryProcessor processor ) {
            nativeRows = processor.readNativeInsertSource( modify, table, parameterRowType );
        }
        List<List<PolyValue>> sourceRows = nativeRows.orElseGet( () -> readRows( source, statement, parameterRowType, assignmentInputs ) );
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

        List<AlgDataTypeField> temporalKey = statement.isInsertRowCaptureRequested()
                ? temporalPrimaryKey( table, statement, targetFields ) : List.of();
        convertInsertAssignments( modify, table, statement, targetFields, sourceRows, assignmentInputs, nativeRows.isPresent(), temporalKey );
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
                    int index = field.getIndex();
                    PolyType type = field.getType().getPolyType();
                    if ( PolyType.NUMERIC_TYPES.contains( type ) || isTimeType( type ) ) {
                        captured.set( index, inserted.get( index ).getValue() );
                    }
                }
                capturedRows.add( captured );
            }
        }
        ImmutableList<ImmutableList<RexLiteral>> inserted = tuples.build();
        if ( statement.isInsertRowCaptureRequested() ) {
            validateTemporalInsertKeys( modify, table, statement, parameterRowType, temporalKey, inserted );
            captureStoredRows( modify, table, statement, parameterRowType, targetFields, capturedRows );
        }

        // Parameter sets have already been expanded into concrete rows, once each.
        statement.getDataContext().resetParameterValues();
        statement.setMaterializedDmlRowCount( sourceRows.size() );
        AlgNode values = LogicalRelValues.create( modify.getCluster(), rowType, inserted );
        return root.withAlg( modify.copy( modify.getTraitSet(), List.of( values ) ) );
    }


    private static boolean isScalarInsertSource( AlgNode source ) {
        return source instanceof LogicalRelValues
                || source instanceof LogicalRelProject project && project.getInput() instanceof LogicalRelValues values
                && values.getTuples().size() == 1;
    }


    private static void convertInsertAssignments( LogicalRelModify modify, LogicalTable table, Statement statement,
            List<AlgDataTypeField> fields, List<List<PolyValue>> rows, List<List<AssignmentInput>> assignmentInputs,
            boolean nativeNumeric, List<AlgDataTypeField> temporalKey ) {
        Map<AlgDataTypeField, Map<Integer, NumericAssignment>> numerics = new LinkedHashMap<>();
        Map<AlgDataTypeField, Map<Integer, PolyValue>> times = new LinkedHashMap<>();
        for ( AlgDataTypeField field : fields ) {
            PolyType target = field.getType().getPolyType();
            if ( isTimeType( target ) ) {
                Map<Integer, PolyValue> assignments = temporalAssignments( field, rows );
                if ( !assignments.isEmpty() ) {
                    times.put( field, assignments );
                }
                continue;
            }
            boolean decimalKey = target == PolyType.DECIMAL && temporalKey.contains( field );
            if ( !PolyType.NUMERIC_TYPES.contains( target ) || (nativeNumeric && !decimalKey) ) {
                continue;
            }
            Map<Integer, NumericAssignment> assignments = new LinkedHashMap<>();
            int index = field.getIndex();
            for ( int i = 0; i < rows.size(); i++ ) {
                List<PolyValue> row = rows.get( i );
                PolyValue value = row.get( index );
                AssignmentInput input = nativeNumeric ? null : assignmentInputs.get( i ).get( index );
                boolean hasNumber = value != null && !value.isNull() && value.isNumber();
                if ( input != null && requiresNativeAssignment( input, field.getType(), hasNumber ) ) {
                    assignments.put( i, new NumericAssignment( value, input.type(), input.expression() ) );
                    continue;
                }
                if ( decimalKey && needsDecimalReadback( field, value ) ) {
                    assignments.put( i, new NumericAssignment( value, field.getType(), null ) );
                    continue;
                }
                if ( !hasNumber || target != PolyType.BIGINT || value.isInteger() || value.isLong() ) {
                    continue;
                }
                try {
                    BigDecimal number = value.asNumber().bigDecimalValue();
                    if ( number.stripTrailingZeros().scale() <= 0 ) {
                        row.set( index, PolyLong.of( number.longValueExact() ) );
                    } else {
                        assignments.put( i, new NumericAssignment( value, field.getType(), null ) );
                    }
                } catch ( ArithmeticException | NumberFormatException e ) {
                    throw numericAssignmentOutOfRange( field, value, e );
                }
            }
            if ( !assignments.isEmpty() ) {
                numerics.put( field, assignments );
            }
        }
        if ( numerics.isEmpty() && times.isEmpty() ) {
            return;
        }
        PartitionProperty partitions = statement.getTransaction().getSnapshot().alloc().getPartitionProperty( table.id ).orElseThrow();
        AlgDataTypeField partitionField = partitions.isPartitioned
                ? fields.stream().filter( field -> field.getId() == partitions.partitionColumnId ).findFirst().orElseThrow() : null;
        List<AlgDataTypeField> conversionOrder = fields.stream()
                .filter( field -> times.containsKey( field ) || numerics.containsKey( field ) )
                .sorted( Comparator.comparing( field -> field != partitionField ) ).toList();
        for ( AlgDataTypeField field : conversionOrder ) {
            if ( times.containsKey( field ) ) {
                convertOnPlacement( modify, table, statement, field, partitionField, partitions, rows, times.get( field ),
                        (store, values) -> store.castTemporalAssignments( statement.getTransaction(), modify.getCluster(), field.getType(), values ) );
            } else {
                convertOnPlacement( modify, table, statement, field, partitionField, partitions, rows, numerics.get( field ),
                        (store, values) -> store.castNumericAssignments( statement.getTransaction(), modify.getCluster(), field.getType(), values ) );
            }
        }
    }


    private static boolean isTimeType( PolyType type ) {
        return type == PolyType.TIME || type == PolyType.TIMESTAMP;
    }


    private static Map<Integer, PolyValue> temporalAssignments( AlgDataTypeField field, List<List<PolyValue>> rows ) {
        int millisPerUnit = switch ( field.getType().getPrecision() ) {
            case 0 -> 1000;
            case 1 -> 100;
            case 2 -> 10;
            default -> 1;
        };
        if ( millisPerUnit == 1 ) {
            return Map.of();
        }
        Map<Integer, PolyValue> assignments = new LinkedHashMap<>();
        for ( int i = 0; i < rows.size(); i++ ) {
            PolyValue value = rows.get( i ).get( field.getIndex() );
            if ( value != null && !value.isNull() ) {
                PolyValue temporal = toLiteral( value, field ).getValue();
                if ( temporal.asTemporal().getMillisSinceEpoch() % millisPerUnit != 0 ) {
                    assignments.put( i, temporal );
                }
            }
        }
        return assignments;
    }


    private static boolean requiresNativeAssignment( AssignmentInput input, AlgDataType target, boolean hasNumber ) {
        RexNode expression = input.expression() == null ? null : input.expression().node();
        if ( expression instanceof RexCall ) {
            return true;
        }
        if ( !hasNumber ) {
            return false;
        }
        if ( target.getPolyType() == PolyType.DECIMAL ) {
            // The destination still performs the final assignment. Only explicitly typed
            // source parameters need their own DECIMAL constraint applied first.
            return expression instanceof RexDynamicParam
                    && (input.type().getPolyType() != PolyType.DECIMAL || input.type().getPrecision() != target.getPrecision()
                    || input.type().getScale() != target.getScale());
        }
        return input.type().getPolyType() != target.getPolyType();
    }


    private static <T> void convertOnPlacement( LogicalRelModify modify, LogicalTable table, Statement statement,
            AlgDataTypeField field, AlgDataTypeField partitionField, PartitionProperty partitions,
            List<List<PolyValue>> rows, Map<Integer, T> assignments, BiFunction<DataStore<?>, List<T>, List<PolyValue>> convert ) {
        String assignmentKind = isTimeType( field.getType().getPolyType() ) ? "temporal" : "numeric";
        Snapshot snapshot = statement.getTransaction().getSnapshot();
        PartitionManager partitionManager = partitions.isPartitioned
                ? PartitionManagerFactory.getInstance().getPartitionManager( partitions.partitionType ) : null;
        boolean boundPartitionKey = partitionField == field && modify.getInput() instanceof LogicalRelProject project
                && project.getProjects().get( field.getIndex() ) instanceof RexDynamicParam;
        List<AllocationPlacement> tablePlacements = snapshot.alloc().getPlacementsFromLogical( table.id );
        Map<Long, List<AllocationPlacement>> placements = new LinkedHashMap<>();
        for ( AllocationPlacement placement : tablePlacements ) {
            if ( snapshot.alloc().getColumn( placement.id, field.getId() ).isPresent() ) {
                placements.computeIfAbsent( placement.adapterId, id -> new ArrayList<>() ).add( placement );
            }
        }
        Map<Integer, PolyValue> converted = new HashMap<>();
        for ( Map.Entry<Long, List<AllocationPlacement>> store : placements.entrySet() ) {
            List<Integer> candidates = assignments.keySet().stream().filter( row -> {
                if ( partitionField == null ) {
                    return true;
                }
                PolyValue input = rows.get( row ).get( partitionField.getIndex() );
                // Cast a partition key on the original placement before re-routing on the stored key.
                PolyValue key = partitionField == field ? input : toLiteral( input, partitionField ).getValue();
                long partition = targetPartition( table, partitions, partitionManager, key, boundPartitionKey );
                return hasPartition( snapshot, store.getValue(), partition );
            } ).toList();
            if ( candidates.isEmpty() ) {
                continue;
            }
            List<PolyValue> assigned;
            try {
                assigned = convert.apply( AdapterManager.getInstance().getStore( store.getKey() ).orElseThrow(),
                        candidates.stream().map( assignments::get ).toList() );
            } catch ( ArithmeticException e ) {
                throw new PolyphenyDbException( "A " + assignmentKind + " assignment is out of range for column '" + field.getName() + "'.", e, "22003" );
            }
            for ( int i = 0; i < candidates.size(); i++ ) {
                PolyValue value = assigned.get( i );
                PolyValue previous = converted.putIfAbsent( candidates.get( i ), value );
                if ( previous != null && !previous.equals( value ) ) {
                    throw new PolyphenyDbException( "Store placements disagree on the " + assignmentKind + " assignment to column '" + field.getName() + "'.", null, "22000" );
                }
            }
        }
        if ( converted.size() != assignments.size() ) {
            throw new PolyphenyDbException( "Could not identify a store for the " + assignmentKind + " assignment to column '" + field.getName() + "'.", null, "22000" );
        }
        if ( partitionField == field ) {
            for ( Map.Entry<Integer, PolyValue> assignment : converted.entrySet() ) {
                PolyValue input = rows.get( assignment.getKey() ).get( field.getIndex() );
                long original = targetPartition( table, partitions, partitionManager, input, boundPartitionKey );
                long canonical = targetPartition( table, partitions, partitionManager, assignment.getValue(), false );
                if ( original != canonical && !partitionAdapters( snapshot, tablePlacements, original )
                        .equals( partitionAdapters( snapshot, tablePlacements, canonical ) ) ) {
                    throw new PolyphenyDbException( "The " + assignmentKind + " assignment to partition column '" + field.getName()
                            + "' would move the row to a different set of stores.", null, "22000" );
                }
            }
        }
        converted.forEach( ( row, value ) -> rows.get( row ).set( field.getIndex(), value ) );
    }


    private static long targetPartition( LogicalTable table, PartitionProperty partitions, PartitionManager manager, PolyValue key, boolean isBinding ) {
        String value = key == null ? PartitionManager.NULL_STRING : isBinding ? key.toString() : key.toJson().replace( "'", "" );
        return manager.getTargetPartitionId( table, partitions, value );
    }


    private static Set<Long> partitionAdapters( Snapshot snapshot, List<AllocationPlacement> placements, long partition ) {
        Set<Long> adapters = new HashSet<>();
        for ( AllocationPlacement placement : placements ) {
            if ( snapshot.alloc().getAlloc( placement.id, partition ).isPresent() ) {
                adapters.add( placement.adapterId );
            }
        }
        return adapters;
    }


    private static boolean hasPartition( Snapshot snapshot, List<AllocationPlacement> placements, long partition ) {
        return placements.stream().anyMatch( placement -> snapshot.alloc().getAlloc( placement.id, partition ).isPresent() );
    }


    private static List<AlgDataTypeField> insertPrimaryKey( LogicalTable table, Statement statement, List<AlgDataTypeField> fields ) {
        List<Long> key = statement.getTransaction().getSnapshot().rel().getPrimaryKey( table.primaryKey ).orElseThrow().fieldIds;
        return fields.stream().filter( field -> key.contains( field.getId() ) ).toList();
    }


    private static List<AlgDataTypeField> temporalPrimaryKey( LogicalTable table, Statement statement, List<AlgDataTypeField> fields ) {
        if ( fields.stream().noneMatch( field -> isTimeType( field.getType().getPolyType() ) ) ) {
            return List.of();
        }
        List<AlgDataTypeField> key = insertPrimaryKey( table, statement, fields );
        return key.stream().anyMatch( field -> isTimeType( field.getType().getPolyType() ) ) ? key : List.of();
    }


    private static void validateTemporalInsertKeys( LogicalRelModify modify, LogicalTable table, Statement statement, AlgDataType parameterRowType,
            List<AlgDataTypeField> key, List<ImmutableList<RexLiteral>> inserted ) {
        if ( key.isEmpty() || inserted.isEmpty() ) {
            return;
        }
        List<List<PolyValue>> rows = inserted.stream().map( tuple -> tuple.stream().map( RexLiteral::getValue ).toList() ).toList();
        Set<List<PolyValue>> keys = new HashSet<>();
        for ( List<PolyValue> row : rows ) {
            if ( !keys.add( primaryKey( row, key ) ) ) {
                throw new PolyphenyDbException( "Multiple rows in the INSERT assign the same primary key in '%s'.".formatted( table.name ), null, "23505" );
            }
        }
        prepareStoredValueReadback( modify, table, statement, parameterRowType, key, List.of(), rows, true );
    }


    private static void captureStoredRows( LogicalRelModify modify, LogicalTable table, Statement statement,
            AlgDataType parameterRowType, List<AlgDataTypeField> fields, List<List<PolyValue>> rows ) {
        InsertReturning projection = statement.getInsertReturning() != null ? statement.getInsertReturning() : statement.getGeneratedKeysProjection();
        List<AlgDataTypeField> projectedFields = projection.ordinals().stream().distinct().map( fields::get )
                .filter( field -> rows.stream().anyMatch( row -> needsDecimalReadback( field, row.get( field.getIndex() ) ) ) )
                .toList();
        if ( projectedFields.isEmpty() ) {
            statement.setInsertedRows( rows );
            return;
        }
        List<List<PolyValue>> affectedRows = rows.stream().filter( row -> projectedFields.stream()
                .anyMatch( field -> needsDecimalReadback( field, row.get( field.getIndex() ) ) ) ).toList();
        List<StoredValueBatch> batches = prepareStoredValueReadback( modify, table, statement, parameterRowType,
                insertPrimaryKey( table, statement, fields ), projectedFields, affectedRows, false );
        statement.setInsertedRowsReader( () -> readStoredValues( table, statement, parameterRowType, projectedFields, rows, batches ) );
    }


    private static boolean needsDecimalReadback( AlgDataTypeField field, PolyValue value ) {
        if ( value == null || value.isNull() ) {
            return false;
        }
        AlgDataType type = field.getType();
        return type.getPolyType() == PolyType.DECIMAL && PolyTypeUtil.hasScale( type ) && value.asNumber().bigDecimalValue().scale() != type.getScale();
    }


    // Read coerced values in the same transaction after INSERT, projecting the input ordinal
    // so the output follows the original statement's row order.
    private static List<StoredValueBatch> prepareStoredValueReadback( LogicalRelModify modify, LogicalTable table, Statement statement,
            AlgDataType parameterRowType, List<AlgDataTypeField> keyFields, List<AlgDataTypeField> projectedFields, List<List<PolyValue>> rows,
            boolean requireNewKeys ) {
        RexBuilder rex = modify.getCluster().getRexBuilder();
        List<StoredValueBatch> batches = new ArrayList<>();
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
            projectedFields.forEach( field -> expressions.add( rex.makeInputRef( scan, field.getIndex() ) ) );
            List<String> aliases = new ArrayList<>();
            aliases.add( "insert_ordinal" );
            projectedFields.forEach( field -> aliases.add( field.getName() ) );
            AlgNode filtered = LogicalRelFilter.create( scan, RexUtil.composeDisjunction( rex, predicates, false ) );
            AlgNode projected = LogicalRelProject.create( filtered, expressions, aliases );
            boolean castsDecimalKey = batch.stream().anyMatch( row -> keyFields.stream()
                    .anyMatch( field -> needsDecimalKeyCoercion( field, row.get( field.getIndex() ) ) ) );
            // A dialect or optimizer must not turn a lookup cast into a different, already-existing key.
            // Detect preexisting matches and refuse to expose them as rows written by this INSERT.
            Set<Integer> existing = new HashSet<>();
            if ( requireNewKeys || castsDecimalKey ) {
                readStoredValueBatch( projected, statement, parameterRowType )
                        .forEach( stored -> existing.add( stored.get( 0 ).asNumber().intValue() ) );
            }
            if ( requireNewKeys && !existing.isEmpty() ) {
                throw new PolyphenyDbException( "Cannot project stored values for an INSERT whose primary key already exists in '%s'."
                        .formatted( table.name ), null, "23505" );
            }
            batches.add( new StoredValueBatch( batch, projected, existing ) );
        }
        return batches;
    }


    private static List<List<PolyValue>> readStoredValues( LogicalTable table, Statement statement, AlgDataType parameterRowType,
            List<AlgDataTypeField> projectedFields, List<List<PolyValue>> rows, List<StoredValueBatch> batches ) {
        for ( StoredValueBatch batch : batches ) {
            Set<Integer> matched = new HashSet<>();
            for ( List<PolyValue> stored : readStoredValueBatch( batch.query(), statement, parameterRowType ) ) {
                int index = stored.get( 0 ).asNumber().intValue();
                if ( index < 0 || index >= batch.rows().size() || batch.existing().contains( index ) || !matched.add( index ) ) {
                    throw new GenericRuntimeException( "Could not match an inserted row in '%s' to its stored values.", table.name );
                }
                List<PolyValue> row = batch.rows().get( index );
                for ( int column = 0; column < projectedFields.size(); column++ ) {
                    AlgDataTypeField field = projectedFields.get( column );
                    int fieldIndex = field.getIndex();
                    if ( needsDecimalReadback( field, row.get( fieldIndex ) ) ) {
                        row.set( fieldIndex, stored.get( column + 1 ) );
                    }
                }
            }
            if ( matched.size() != batch.rows().size() ) {
                throw new GenericRuntimeException( "Could not read back all inserted rows in '%s'.", table.name );
            }
        }
        return rows;
    }


    private static List<List<PolyValue>> readStoredValueBatch( AlgNode projected, Statement statement, AlgDataType parameterRowType ) {
        DataContext context = statement.getDataContext();
        List<Map<Long, PolyValue>> originalValues = context.getParameterValues();
        Map<Long, AlgDataType> originalTypes = Map.copyOf( context.getParameterTypes() );
        context.resetParameterValues();
        try {
            List<List<PolyValue>> stored = new ArrayList<>();
            readRows( projected, statement, parameterRowType, Map.of(), stored, null );
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


    private record StoredValueBatch( List<List<PolyValue>> rows, AlgNode query, Set<Integer> existing ) {
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
        if ( type.getPolyType() == PolyType.DECIMAL ) {
            return new RexLiteral( PolyBigDecimal.convert( value ), type, PolyType.DECIMAL, true );
        }
        if ( value.isNumber() && (type.getPolyType() == PolyType.REAL || type.getPolyType() == PolyType.FLOAT) ) {
            return new RexLiteral( PolyFloat.of( value.asNumber().floatValue() ), type, type.getPolyType(), true );
        }
        if ( value.isNumber() && type.getPolyType() == PolyType.DOUBLE ) {
            return new RexLiteral( PolyDouble.of( value.asNumber().doubleValue() ), type, PolyType.DOUBLE, true );
        }
        if ( value.isNumber() && (type.getPolyType() == PolyType.TINYINT || type.getPolyType() == PolyType.SMALLINT || type.getPolyType() == PolyType.INTEGER) ) {
            try {
                return new RexLiteral( PolyInteger.of( value.asNumber().intValue() ), type, type.getPolyType(), true );
            } catch ( ArithmeticException overflow ) {
                throw numericAssignmentOutOfRange( field, value, overflow );
            }
        }
        Pair<PolyValue, PolyType> converted = RexLiteral.convertType( value, type );
        return new RexLiteral( converted.left, type, converted.right, true );
    }


    private static PolyphenyDbException numericAssignmentOutOfRange( AlgDataTypeField field, PolyValue value, RuntimeException cause ) {
        return new PolyphenyDbException( "Value " + value + " is out of range for " + field.getType().getPolyType() + " column '" + field.getName() + "'.", cause, "22003" );
    }


    private static List<List<PolyValue>> readRows( AlgNode source, Statement statement, AlgDataType parameterRowType ) {
        return readRows( source, statement, parameterRowType, null );
    }


    private static List<List<PolyValue>> readRows( AlgNode source, Statement statement, AlgDataType parameterRowType,
            List<List<AssignmentInput>> assignmentInputs ) {
        DataContext dataContext = statement.getDataContext();
        List<Map<Long, PolyValue>> parameters = dataContext.getParameterValues();
        List<List<PolyValue>> rows = new ArrayList<>();
        if ( parameters.isEmpty() ) {
            readRows( source, statement, parameterRowType, Map.of(), rows, assignmentInputs );
            return rows;
        }
        for ( Map<Long, PolyValue> binding : parameters ) {
            readRows( source, statement, parameterRowType, binding, rows, assignmentInputs );
        }
        return rows;
    }


    private static void readRows(
            AlgNode source, Statement statement, AlgDataType parameterRowType, Map<Long, PolyValue> binding,
            List<List<PolyValue>> rows, List<List<AssignmentInput>> assignmentInputs ) {
        boolean parameterizeLiterals = assignmentInputs != null && RuntimeConfig.PARAMETERIZE_DML.getBoolean()
                && statement.getDataContext().getParameterValues().isEmpty();
        if ( source instanceof LogicalRelUnion union && union.all ) {
            for ( AlgNode input : union.getInputs() ) {
                readRows( input, statement, parameterRowType, binding, rows, assignmentInputs );
            }
            return;
        }
        if ( source instanceof LogicalRelValues values ) {
            for ( ImmutableList<RexLiteral> tuple : values.getTuples() ) {
                addSourceRow( rows, assignmentInputs, source, tuple, new ArrayList<>( tuple.stream().map( RexLiteral::getValue ).toList() ), binding, parameterizeLiterals );
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
            addSourceRow( rows, assignmentInputs, source, project.getProjects(), row, binding, parameterizeLiterals );
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
            List<? extends RexNode> expressions = source instanceof LogicalRelProject project ? project.getProjects() : List.of();
            for ( List<PolyValue> row : implementation.execute( statement, -1 ).getAllRowsAndClose() ) {
                addSourceRow( rows, assignmentInputs, source, expressions, row, binding, parameterizeLiterals );
            }
        } finally {
            context.setParameterTypes( originalTypes );
            context.setParameterValues( originalValues );
        }
    }


    private static void addSourceRow( List<List<PolyValue>> rows, List<List<AssignmentInput>> assignmentInputs,
            AlgNode source, List<? extends RexNode> expressions, List<PolyValue> row, Map<Long, PolyValue> binding, boolean parameterizeLiterals ) {
        rows.add( row );
        if ( assignmentInputs == null ) {
            return;
        }
        AlgDataTypeFactory typeFactory = source.getCluster().getTypeFactory();
        boolean parameterizedValues = source instanceof LogicalRelValues && parameterizeLiterals;
        boolean scalarProject = source instanceof LogicalRelProject project && project.getInput() instanceof LogicalRelValues values
                && values.getTuples().size() == 1;
        List<AssignmentInput> inputs = new ArrayList<>( row.size() );
        for ( int i = 0; i < row.size(); i++ ) {
            RexNode expression = i < expressions.size() ? expressions.get( i ) : null;
            PolyValue value = row.get( i );
            if ( scalarProject && expression instanceof RexCall && PolyType.NUMERIC_TYPES.contains( expression.getType().getPolyType() )
                    && RexUtil.isConstant( expression ) ) {
                inputs.add( new AssignmentInput( expression.getType(), numericExpression( expression, binding, parameterizeLiterals ) ) );
                continue;
            }
            if ( value == null || value.isNull() || !value.isNumber() ) {
                inputs.add( null );
                continue;
            }
            if ( parameterizedValues ) {
                AlgDataType type = source.getTupleType().getFields().get( i ).getType();
                inputs.add( PolyType.NUMERIC_TYPES.contains( type.getPolyType() )
                        ? new AssignmentInput( type, new NumericExpression( new RexDynamicParam( type, 0 ), Map.of( 0L, value ) ) ) : null );
                continue;
            }
            if ( parameterizeLiterals && scalarProject && expression instanceof RexLiteral literal
                    && PolyType.NUMERIC_TYPES.contains( literal.getType().getPolyType() ) ) {
                inputs.add( new AssignmentInput( literal.getType(), numericExpression( literal, binding, true ) ) );
                continue;
            }
            if ( expression instanceof RexDynamicParam parameter ) {
                inputs.add( PolyType.NUMERIC_TYPES.contains( parameter.getType().getPolyType() )
                        ? new AssignmentInput( parameter.getType(), new NumericExpression( parameter, binding ) ) : null );
                continue;
            }
            PolyType sourceType = expression != null && PolyType.APPROX_TYPES.contains( expression.getType().getPolyType() )
                    ? expression.getType().getPolyType() : value.getType();
            AlgDataType inputType;
            if ( sourceType == PolyType.DECIMAL ) {
                BigDecimal decimal = value.asNumber().bigDecimalValue();
                int scale = Math.max( decimal.scale(), 0 );
                int precision = Math.max( decimal.precision() - Math.min( decimal.scale(), 0 ), scale );
                inputType = typeFactory.createPolyType( PolyType.DECIMAL, precision, scale );
            } else {
                inputType = typeFactory.createPolyType( sourceType );
            }
            // The normal SQL plan folds a CAST of a literal before it renders an inline approximate value.
            RexLiteral literal = null;
            if ( !parameterizeLiterals && expression != null && PolyType.APPROX_TYPES.contains( sourceType )
                    && RexUtil.isLiteral( expression, true ) ) {
                literal = expression instanceof RexLiteral folded ? folded : new RexLiteral( value, expression.getType(), sourceType, true );
            }
            inputs.add( new AssignmentInput( inputType, literal == null ? null : new NumericExpression( literal, Map.of() ) ) );
        }
        assignmentInputs.add( inputs );
    }


    private static NumericExpression numericExpression( RexNode expression, Map<Long, PolyValue> binding, boolean parameterizeLiterals ) {
        if ( !parameterizeLiterals ) {
            return new NumericExpression( expression, binding );
        }
        // Rewrite literals exactly as for ordinary INSERT, before rendering on the store.
        QueryParameterizer parameterizer = new QueryParameterizer( 0, List.of(), false );
        RexNode parameterized = expression.accept( parameterizer );
        Map<Long, PolyValue> values = new HashMap<>();
        parameterizer.getValues().values().forEach( parameters -> {
            DataContext.ParameterValue parameter = parameters.get( 0 );
            values.put( parameter.index(), parameter.value() );
        } );
        return new NumericExpression( parameterized, values );
    }


    private record AssignmentInput( AlgDataType type, NumericExpression expression ) {
    }


    private record ColumnLayout( long id, String name, AlgDataType type, boolean nullable, LogicalDefaultValue defaultValue, IdentityColumn.Mode mode ) {
    }


    private record IdentityField( int columnIndex, int flagIndex, IdentityColumn definition, PolyType type ) {
    }

}
