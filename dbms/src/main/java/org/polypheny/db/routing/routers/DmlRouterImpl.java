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

package org.polypheny.db.routing.routers;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.core.common.BatchIterator;
import org.polypheny.db.algebra.core.common.ConditionalExecute;
import org.polypheny.db.algebra.core.common.ConstraintEnforcer;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.core.document.DocumentScan;
import org.polypheny.db.algebra.core.lpg.LpgScan;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.algebra.logical.common.LogicalBatchIterator;
import org.polypheny.db.algebra.logical.common.LogicalConditionalExecute;
import org.polypheny.db.algebra.logical.common.LogicalConstraintEnforcer;
import org.polypheny.db.algebra.logical.common.LogicalContextSwitcher;
import org.polypheny.db.algebra.logical.document.LogicalDocumentModify;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.logical.document.LogicalDocumentValues;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgScan;
import org.polypheny.db.algebra.logical.relational.LogicalModifyCollect;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationPartition;
import org.polypheny.db.catalog.entity.allocation.AllocationPlacement;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.partition.PartitionManager;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.processing.WhereClauseVisitor;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.routing.DmlRouter;
import org.polypheny.db.routing.RoutingContext;
import org.polypheny.db.routing.RoutingManager;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.RoutedAlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Triple;

@Slf4j
public class DmlRouterImpl extends BaseRouter implements DmlRouter {


    @Override
    public AlgNode routeDml( LogicalRelModify modify, Statement statement ) {
        AlgCluster cluster = modify.getCluster();

        if ( modify.entity == null ) {
            throw new GenericRuntimeException( "Unexpected operator!" );
        }
        Optional<LogicalTable> oTable = modify.entity.unwrap( LogicalTable.class );

        if ( oTable.isEmpty() ) {
            throw new GenericRuntimeException( "Unexpected table. Only logical tables expected here!" );
        }
        LogicalTable table = oTable.get();

        List<LogicalColumn> columns = catalog.getSnapshot().rel().getColumns( table.id );
        List<Long> columnIds = columns.stream().map( c -> c.id ).toList();

        // Make sure that this table can be modified
        if ( !table.modifiable ) {
            if ( table.entityType == EntityType.ENTITY ) {
                throw new GenericRuntimeException( "Unable to modify a table marked as read-only!" );
            } else if ( table.entityType == EntityType.SOURCE ) {
                throw new GenericRuntimeException( "The table '%s' is provided by a data source which does not support data modification.", table.name );
            } else if ( table.entityType == EntityType.VIEW ) {
                throw new GenericRuntimeException( "Polypheny-DB does not support modifying views." );
            }
            throw new GenericRuntimeException( "Unknown table type: %s", table.entityType.name() );
        }

        long pkid = table.primaryKey;
        List<Long> pkColumnIds = catalog.getSnapshot().rel().getPrimaryKey( pkid ).orElseThrow().fieldIds;
        LogicalColumn pkColumn = catalog.getSnapshot().rel().getColumn( pkColumnIds.get( 0 ) ).orElseThrow();

        // Essentially gets a list of all stores where this table resides
        List<AllocationPlacement> pkPlacements = catalog.getSnapshot().alloc().getPlacementsOfColumn( pkColumn.id );
        PartitionProperty property = catalog.getSnapshot().alloc().getPartitionProperty( table.id ).orElseThrow();

        if ( property.isPartitioned && log.isDebugEnabled() ) {
            log.debug( "\nListing all relevant stores for table: '{}' and all partitions: {}", table.name, property.partitionGroupIds );
            for ( AllocationPlacement placement : pkPlacements ) {
                log.debug(
                        "\t\t -> '{}' {}\t{}",
                        catalog.getSnapshot().getAdapter( placement.adapterId ),
                        null,//catalog.getPartitionGroupsOnDataPlacement( dataPlacement.adapterId, dataPlacement.tableId ),
                        null );//catalog.getPartitionGroupsIndexOnDataPlacement( dataPlacement.adapterId, dataPlacement.tableId ) );
            }
        }

        // Execute on all primary key placements
        List<AlgNode> modifies = new ArrayList<>();

        // Needed for partitioned updates when source partition and target partition are not equal
        // SET Value is the new partition, where clause is the source
        boolean operationWasRewritten = false;

        List<Map<Long, PolyValue>> allValues = statement.getDataContext().getParameterValues();

        for ( AllocationPlacement pkPlacement : pkPlacements ) {
            // Get placements on storeId
            List<AllocationColumn> placementsOnAdapter = catalog.getSnapshot().alloc().getColumns( pkPlacement.id );

            // If this is an update, check whether we need to execute on this storeId at all
            List<String> updateColumns = modify.getUpdateColumns();
            List<? extends RexNode> sourceExpressions = modify.getSourceExpressions();
            if ( placementsOnAdapter.size() != oTable.get().getColumnIds().size() ) {

                if ( modify.getOperation() == Operation.UPDATE ) {
                    updateColumns = new ArrayList<>( modify.getUpdateColumns() );
                    sourceExpressions = new ArrayList<>( modify.getSourceExpressions() );
                    Iterator<String> updateColumnListIterator = updateColumns.iterator();
                    Iterator<? extends RexNode> sourceExpressionListIterator = sourceExpressions.iterator();
                    while ( updateColumnListIterator.hasNext() ) {
                        String columnName = updateColumnListIterator.next();
                        sourceExpressionListIterator.next();

                        Optional<LogicalColumn> optionalColumn = catalog.getSnapshot().rel().getColumn( table.id, columnName );
                        if ( optionalColumn.isEmpty() ) {
                            throw new GenericRuntimeException( "Could not find the column with the name %s", columnName );
                        }
                        if ( catalog.getSnapshot().alloc().getColumn( pkPlacement.id, optionalColumn.get().id ).isEmpty() ) {
                            updateColumnListIterator.remove();
                            sourceExpressionListIterator.remove();
                        }
                    }
                    if ( updateColumns.isEmpty() ) {
                        continue;
                    }
                }
            }

            long identPart = -1;
            long identifiedPartitionForSetValue = -1;
            Set<Long> accessedPartitions = new HashSet<>();
            // Identify where clause of UPDATE
            if ( property.isPartitioned ) {
                boolean worstCaseRouting = false;
                Set<Long> identifiedPartitionsInFilter = new HashSet<>();

                PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
                PartitionManager partitionManager = partitionManagerFactory.getPartitionManager( property.partitionType );

                WhereClauseVisitor whereClauseVisitor = new WhereClauseVisitor( statement, columnIds.indexOf( property.partitionColumnId ) );
                modify.accept( new AlgShuttleImpl() {
                    @Override
                    public AlgNode visit( LogicalRelFilter filter ) {
                        super.visit( filter );
                        filter.accept( whereClauseVisitor );
                        return filter;
                    }
                } );

                List<String> whereClauseValues = null;
                if ( !whereClauseVisitor.getValues().isEmpty() ) {

                    whereClauseValues = whereClauseVisitor.getValues().stream()
                            .map( PolyValue::toJson )
                            .toList();
                    if ( log.isDebugEnabled() ) {
                        log.debug( "Found Where Clause Values: {}", whereClauseValues );
                    }
                    worstCaseRouting = true;
                }

                if ( whereClauseValues != null ) {
                    for ( String value : whereClauseValues ) {
                        worstCaseRouting = false;
                        identPart = (int) partitionManager.getTargetPartitionId( table, property, value );
                        accessedPartitions.add( identPart );
                        identifiedPartitionsInFilter.add( identPart );
                    }
                }

                String partitionValue = "";
                // Set true if partitionColumn is part of UPDATE Statement, else assume worst case routing

                if ( modify.getOperation() == Operation.UPDATE ) {
                    Pair<Set<Long>, String> accessedPartitionsPartVal = handleDmlUpdate(
                            updateColumns,
                            table,
                            property,
                            sourceExpressions,
                            partitionValue,
                            partitionManager,
                            identifiedPartitionsInFilter,
                            identifiedPartitionForSetValue,
                            identPart );
                    accessedPartitions.clear();
                    accessedPartitions.addAll( accessedPartitionsPartVal.left );
                    partitionValue = accessedPartitionsPartVal.right;
                } else if ( modify.getOperation() == Operation.INSERT ) {
                    Triple<Long, String, Boolean> identPartValueRewritten = handleDmlInsert(
                            updateColumns,
                            modify,
                            columnIds,
                            columns,
                            property,
                            table,
                            accessedPartitions,
                            partitionManager,
                            pkPlacement,
                            statement,
                            placementsOnAdapter,
                            cluster,
                            modifies,
                            sourceExpressions,
                            allValues );
                    partitionValue = identPartValueRewritten.middle;
                    identPart = identPartValueRewritten.left;
                    operationWasRewritten = identPartValueRewritten.right;
                } else if ( modify.getOperation() == Operation.DELETE ) {
                    if ( whereClauseValues == null ) {
                        worstCaseRouting = true;
                    } else {
                        worstCaseRouting = whereClauseValues.size() >= 4;
                    }

                }

                if ( worstCaseRouting ) {
                    log.debug( "PartitionColumnID was not an explicit part of statement, partition routing will therefore assume worst-case: Routing to ALL PARTITIONS" );
                    accessedPartitions = new HashSet<>( property.partitionIds );
                }
            } else {
                // un-partitioned tables only have one partition anyway
                identPart = property.partitionIds.get( 0 );
                accessedPartitions.add( identPart );
            }

            if ( statement.getMonitoringEvent() != null ) {
                statement.getMonitoringEvent()
                        .updateAccessedPartitions(
                                Collections.singletonMap( oTable.get().id, accessedPartitions ) );
            }

            if ( !operationWasRewritten ) {
                for ( long partitionId : accessedPartitions ) {

                    if ( catalog.getSnapshot().alloc().getAlloc( pkPlacement.id, partitionId ).isEmpty() ) {
                        continue;
                    }

                    // Build DML
                    LogicalRelModify adjustedModify;
                    AllocationEntity allocation = catalog.getSnapshot().alloc().getAlloc( pkPlacement.id, partitionId ).orElseThrow();
                    AlgNode input = buildDml(
                            super.recursiveCopy( modify.getInput( 0 ) ),
                            RoutedAlgBuilder.create( statement, cluster ),
                            oTable.get(),
                            placementsOnAdapter,
                            allocation,
                            statement,
                            cluster,
                            false,
                            statement.getDataContext().getParameterValues() ).build();

                    adjustedModify = LogicalRelModify.create(
                            allocation,
                            input,
                            modify.getOperation(),
                            updateColumns,
                            sourceExpressions,
                            modify.isFlattened()
                    );

                    modifies.add( adjustedModify );
                }
            }
        }

        if ( modifies.size() == 1 ) {
            return modifies.get( 0 );
        } else {
            return new LogicalModifyCollect( modify.getCluster(), modify.getTraitSet(), modifies, true );
        }


    }


    private Triple<Long, String, Boolean> handleDmlInsert( List<String> updateColumns, LogicalRelModify modify, List<Long> columnIds, List<LogicalColumn> columns, PartitionProperty property, LogicalTable table, Set<Long> accessedPartitionList, PartitionManager partitionManager, AllocationPlacement pkPlacement, Statement statement, List<AllocationColumn> placementsOnAdapter, AlgCluster cluster, List<AlgNode> modifies, List<? extends RexNode> sourceExpressions, List<Map<Long, PolyValue>> allValues ) {
        String partitionValue = null;
        long identPart = -1;
        boolean worstCaseRouting;
        boolean operationWasRewritten = false;
        int i;
        if ( modify.getInput() instanceof LogicalRelValues ) {
            // Get fieldList and map columns to index since they could be in arbitrary order
            int partitionColumnIndex = -1;
            for ( int j = 0; j < (modify.getInput()).getTupleType().getFields().size(); j++ ) {
                String columnFieldName = (modify.getInput()).getTupleType().getFields().get( j ).getName();

                // Retrieve columnId of fieldName and map it to its fieldList location of INSERT Stmt
                int columnIndex = columns.stream().map( c -> c.name ).toList().indexOf( columnFieldName );

                // Determine location of partitionColumn in fieldList
                if ( columnIds.get( columnIndex ) == property.partitionColumnId ) {
                    partitionColumnIndex = columnIndex;
                    if ( log.isDebugEnabled() ) {
                        log.debug( "INSERT: Found PartitionColumnID: '{}' at column index: {}", property.partitionColumnId, j );

                    }
                }
            }

            // Will executed all required tuples that belong on the same partition jointly
            Map<Long, List<ImmutableList<RexLiteral>>> tuplesOnPartition = new HashMap<>();
            for ( ImmutableList<RexLiteral> currentTuple : ((LogicalRelValues) modify.getInput()).tuples ) {

                if ( partitionColumnIndex == -1 || currentTuple.get( partitionColumnIndex ).getValue() == null ) {
                    partitionValue = PartitionManager.NULL_STRING;
                } else {
                    partitionValue = currentTuple.get( partitionColumnIndex ).value.toJson().replace( "'", "" );
                }
                identPart = (int) partitionManager.getTargetPartitionId( table, property, partitionValue );
                accessedPartitionList.add( identPart );

                if ( !tuplesOnPartition.containsKey( identPart ) ) {
                    tuplesOnPartition.put( identPart, new ArrayList<>() );
                }
                tuplesOnPartition.get( identPart ).add( currentTuple );

            }

            for ( Map.Entry<Long, List<ImmutableList<RexLiteral>>> partitionMapping : tuplesOnPartition.entrySet() ) {
                Long currentPartitionId = partitionMapping.getKey();

                if ( catalog.getSnapshot().alloc().getPartitionsFromLogical( table.id ).stream().noneMatch( p -> p.id == currentPartitionId ) ) {
                    continue;
                }

                for ( ImmutableList<RexLiteral> row : partitionMapping.getValue() ) {
                    LogicalRelValues newLogicalRelValues = new LogicalRelValues(
                            modify.getCluster(),
                            modify.getCluster().traitSet(),
                            (modify.getInput()).getTupleType(),
                            ImmutableList.copyOf( ImmutableList.of( row ) ) );

                    AllocationEntity allocation = catalog.getSnapshot().alloc().getAlloc( pkPlacement.id, currentPartitionId ).orElseThrow();

                    AlgNode input = buildDml(
                            newLogicalRelValues,
                            RoutedAlgBuilder.create( statement, cluster ),
                            table,
                            placementsOnAdapter,
                            allocation,
                            statement,
                            cluster,
                            true,
                            statement.getDataContext().getParameterValues() ).build();

                    // Build DML
                    RelModify<?> adjustedModify = LogicalRelModify.create(
                            allocation,
                            input,
                            modify.getOperation(),
                            updateColumns,
                            sourceExpressions,
                            modify.isFlattened() );

                    modifies.add( adjustedModify );

                }
            }
            operationWasRewritten = true;

        } else if ( modify.getInput() instanceof LogicalRelProject
                && ((LogicalRelProject) modify.getInput()).getInput() instanceof LogicalRelValues ) {

            String partitionColumnName = catalog.getSnapshot().rel().getColumn( property.partitionColumnId ).orElseThrow().name;
            List<String> fieldNames = modify.getInput().getTupleType().getFieldNames();

            LogicalRelModify ltm = modify;
            LogicalRelProject lproject = (LogicalRelProject) ltm.getInput();

            List<RexNode> fieldValues = lproject.getProjects();

            for ( i = 0; i < fieldNames.size(); i++ ) {
                String columnName = fieldNames.get( i );

                if ( partitionColumnName.equals( columnName ) ) {

                    if ( ((LogicalRelProject) modify.getInput()).getProjects().get( i ).getKind().equals( Kind.DYNAMIC_PARAM ) ) {

                        // Needed to identify the column which contains the partition value
                        long partitionValueIndex = ((RexDynamicParam) fieldValues.get( i )).getIndex();

                        long tempPartitionId = 0;
                        // Get partitionValue per row/tuple to be inserted
                        // Create as many independent TableModifies as there are entries in getParameterValues

                        Map<Long, List<Map<Long, PolyValue>>> tempValues = new HashMap<>();
                        statement.getDataContext().resetContext();
                        for ( Map<Long, PolyValue> currentRow : allValues ) {
                            // first we sort the values to insert according to the partitionManager and their partitionId

                            tempPartitionId = partitionManager.getTargetPartitionId( table, property, currentRow.get( partitionValueIndex ).toString() );

                            if ( catalog.getSnapshot().alloc().getAlloc( pkPlacement.id, tempPartitionId ).isEmpty() ) {
                                continue;
                            }

                            statement.getDataContext().setParameterTypes( statement.getDataContext().getParameterTypes() );

                            if ( !tempValues.containsKey( tempPartitionId ) ) {
                                tempValues.put( tempPartitionId, new ArrayList<>() );
                            }
                            tempValues.get( tempPartitionId ).add( currentRow );
                        }

                        for ( Entry<Long, List<Map<Long, PolyValue>>> entry : tempValues.entrySet() ) {
                            // then we add a modification for each partition
                            statement.getDataContext().setParameterValues( entry.getValue() );

                            AllocationEntity allocation = catalog.getSnapshot().alloc().getAlloc( pkPlacement.id, entry.getKey() ).orElseThrow();

                            AlgNode input = buildDml(
                                    super.recursiveCopy( modify.getInput( 0 ) ),
                                    RoutedAlgBuilder.create( statement, cluster ),
                                    table,
                                    placementsOnAdapter,
                                    allocation,
                                    statement,
                                    cluster,
                                    false,
                                    entry.getValue() ).build();

                            // Build DML
                            LogicalRelModify adjustedModify = LogicalRelModify.create(
                                    allocation,
                                    input,
                                    modify.getOperation(),
                                    updateColumns,
                                    sourceExpressions,
                                    modify.isFlattened() );

                            statement.getDataContext().addContext();
                            modifies.add( new LogicalContextSwitcher( adjustedModify ) );
                        }

                        operationWasRewritten = true;
                    } else {
                        partitionValue = ((LogicalRelProject) modify.getInput()).getProjects().get( i ).toString().replace( "'", "" );
                        identPart = (int) partitionManager.getTargetPartitionId( table, property, partitionValue );
                        accessedPartitionList.add( identPart );
                    }
                    worstCaseRouting = false;
                    break;
                } else {
                    // When loop is finished
                    if ( i == fieldNames.size() - 1 ) {

                        worstCaseRouting = true;
                        // Because partitionColumn has not been specified in insert
                    }
                }
            }
        } else {
            worstCaseRouting = true;
        }

        if ( log.isDebugEnabled() ) {
            String partitionColumnName = catalog.getSnapshot().rel().getColumn( property.partitionColumnId ).orElseThrow().name;
            String partitionName = null; //.getPartitionGroup( identPart ).partitionGroupName;
            log.debug( "INSERT: partitionColumn-value: '{}' should be put on partition: {} ({}), which is partitioned with column {}",
                    partitionValue, identPart, partitionName, partitionColumnName );
        }

        return Triple.of( identPart, partitionValue, operationWasRewritten );
    }


    private Pair<Set<Long>, String> handleDmlUpdate( List<String> updateColumns, LogicalTable table, PartitionProperty property, List<? extends RexNode> sourceExpressionList, String partitionValue, PartitionManager partitionManager, Set<Long> identifiedPartitionsInFilter, long identifiedPartitionForSetValue, long identPart ) {
        Set<Long> accessedPartitions = new HashSet<>();
        // In case of update always use worst case routing for now.
        // Since you have to identify the current partition to delete the entry and then create a new entry on the correct partitions
        int index = 0;

        for ( String cn : updateColumns ) {

            if ( catalog.getSnapshot().rel().getColumn( table.id, cn ).orElseThrow().id == property.partitionColumnId ) {
                if ( log.isDebugEnabled() ) {
                    log.debug( " UPDATE: Found PartitionColumnID Match: '{}' at index: {}", property.partitionColumnId, index );
                }
                // Routing/Locking can now be executed on certain partitions
                partitionValue = ((RexLiteral) sourceExpressionList.get( index )).value.toJson().replace( "'", "" );
                if ( log.isDebugEnabled() ) {
                    log.debug(
                            "UPDATE: partitionColumn-value: '{}' should be put on partition: {}",
                            partitionValue,
                            partitionManager.getTargetPartitionId( table, property, partitionValue ) );
                }
                identPart = (int) partitionManager.getTargetPartitionId( table, property, partitionValue );
                // Needed to verify if UPDATE shall be executed on two partitions or not
                identifiedPartitionForSetValue = identPart;
                accessedPartitions.add( identPart );
                break;
            }
            index++;
        }

        // If WHERE clause has any value for partition column
        if ( !identifiedPartitionsInFilter.isEmpty() ) {

            // Partition has been identified in SET
            if ( identifiedPartitionForSetValue != -1 ) {

                // SET value and single WHERE clause point to same partition.
                // Inplace update possible
                if ( identifiedPartitionsInFilter.size() == 1 && identifiedPartitionsInFilter.contains( identifiedPartitionForSetValue ) ) {
                    if ( log.isDebugEnabled() ) {
                        log.debug( "oldValue and new value reside on same partition: {}", identifiedPartitionForSetValue );
                    }
                } else {
                    throw new GenericRuntimeException( "Updating partition key is not allowed" );
                }
            }// WHERE clause only
            else {
                throw new GenericRuntimeException( "Updating partition key is not allowed" );

                //Simply execute the UPDATE on all identified partitions
                //Nothing to do
                //worstCaseRouting = false;
            }
        }// If only SET is specified
        // Changes the value of partition column of complete table to only reside on one partition
        //Partition functionality cannot be used --> worstCase --> send query to every partition
        else {
            accessedPartitions = new HashSet<>( property.partitionIds );
        }

        return Pair.of( accessedPartitions, partitionValue );

    }


    @Override
    public AlgNode handleConditionalExecute( AlgNode node, RoutingContext context ) {
        LogicalConditionalExecute lce = (LogicalConditionalExecute) node;
        RoutedAlgBuilder builder = context.getRoutedAlgBuilder();
        builder = RoutingManager.getInstance().getFallbackRouter().routeFirst( lce.getLeft(), builder, context );
        AlgNode action;
        if ( lce.getRight() instanceof LogicalConditionalExecute ) {
            action = handleConditionalExecute( lce.getRight(), context );
        } else if ( lce.getRight() instanceof LogicalRelModify ) {
            action = routeDml( (LogicalRelModify) lce.getRight(), context.getStatement() );
        } else {
            throw new IllegalArgumentException();
        }

        return LogicalConditionalExecute.create( builder.build(), action, lce );
    }


    @Override
    public AlgNode handleConstraintEnforcer( AlgNode alg, RoutingContext context ) {
        LogicalConstraintEnforcer constraint = (LogicalConstraintEnforcer) alg;
        RoutedAlgBuilder builder = context.getRoutedAlgBuilder();
        builder = RoutingManager.getInstance().getFallbackRouter().routeFirst( constraint.getRight(), builder, context );

        if ( constraint.getLeft() instanceof RelModify ) {
            return LogicalConstraintEnforcer.create(
                    routeDml( (LogicalRelModify) constraint.getLeft(), context.getStatement() ),
                    builder.build(),
                    constraint.getExceptionClasses(),
                    constraint.getExceptionMessages() );
        } else if ( constraint.getLeft() instanceof BatchIterator ) {
            return LogicalConstraintEnforcer.create(
                    handleBatchIterator( constraint.getLeft(), context ),
                    builder.build(),
                    constraint.getExceptionClasses(),
                    constraint.getExceptionMessages() );
        } else {
            throw new GenericRuntimeException( "The provided modify query for the ConstraintEnforcer was not recognized!" );
        }
    }


    @Override
    public AlgNode handleBatchIterator( AlgNode alg, RoutingContext context ) {
        LogicalBatchIterator iterator = (LogicalBatchIterator) alg;
        AlgNode input;
        if ( iterator.getInput() instanceof RelModify ) {
            input = routeDml( (LogicalRelModify) iterator.getInput(), context.getStatement() );
        } else if ( iterator.getInput() instanceof ConditionalExecute ) {
            input = handleConditionalExecute( iterator.getInput(), context );
        } else if ( iterator.getInput() instanceof ConstraintEnforcer ) {
            input = handleConstraintEnforcer( iterator.getInput(), context );
        } else {
            throw new GenericRuntimeException( "BachIterator had an unknown child!" );
        }

        return LogicalBatchIterator.create( input, context.getStatement() );
    }


    @Override
    public AlgNode routeDocumentDml( LogicalDocumentModify alg, Statement statement, @Nullable AllocationEntity target, @Nullable List<Long> excludedPlacements ) {
        Snapshot snapshot = statement.getTransaction().getSnapshot();

        LogicalCollection collection = alg.entity.unwrap( LogicalCollection.class ).orElseThrow();

        List<AlgNode> modifies = new ArrayList<>();

        List<AllocationEntity> allocs = snapshot.alloc().getFromLogical( collection.id );

        for ( AllocationEntity allocation : allocs ) {
            RoutedAlgBuilder algBuilder = RoutedAlgBuilder.create( statement, alg.getCluster() );
            modifies.add( alg.toBuilder().entity( allocation ).input( buildDml( alg.getInput(), statement, algBuilder, target, excludedPlacements ).build() ).build() );
        }

        if ( modifies.size() == 1 ) {
            return modifies.get( 0 );
        }

        return new LogicalModifyCollect( alg.getCluster(), alg.getTraitSet(), modifies, true );
    }


    @Override
    public AlgNode routeGraphDml( LogicalLpgModify alg, Statement statement, @Nullable AllocationEntity target, @Nullable List<Long> excludedPlacements ) {
        Snapshot snapshot = statement.getTransaction().getSnapshot();
        List<AlgNode> modifies = new ArrayList<>();

        if ( alg.entity.unwrap( AllocationGraph.class ).isPresent() ) {
            // we already selected an allocation entity to execute on
            return alg;
        }

        LogicalGraph graph = alg.entity.unwrap( LogicalGraph.class ).orElseThrow();

        if ( target != null ) {
            return new LogicalLpgModify( alg.getCluster(),
                    alg.getTraitSet(),
                    target,
                    buildGraphDml( alg.getInput(), statement, target, excludedPlacements ),
                    alg.operation,
                    alg.ids,
                    alg.operations );
        }

        List<AllocationPlacement> targetPlacements = catalog.getSnapshot().alloc().getPlacementsFromLogical( graph.id );
        if ( excludedPlacements != null ) {
            targetPlacements = targetPlacements.stream().filter( p -> !excludedPlacements.contains( p.id ) ).toList();
        }

        for ( AllocationPlacement placement : targetPlacements ) {
            List<AllocationPartition> partitions = snapshot.alloc().getPartitionsFromLogical( graph.id );
            if ( partitions.size() > 1 ) {
                throw new GenericRuntimeException( "Vertical partitioned graphs are not supported yet." );
            }

            AllocationEntity alloc = snapshot.alloc().getAlloc( placement.id, partitions.get( 0 ).id ).orElseThrow();

            modifies.add( new LogicalLpgModify( alg.getCluster(),
                    alg.getTraitSet(),
                    alloc,
                    buildGraphDml( alg.getInput(), statement, null, excludedPlacements ),
                    alg.operation,
                    alg.ids,
                    alg.operations ) );

        }

        if ( modifies.size() == 1 ) {
            return modifies.get( 0 );
        }

        return new LogicalModifyCollect( modifies.get( 0 ).getCluster(), modifies.get( 0 ).getTraitSet(), modifies, true );
    }


    private AlgNode buildGraphDml( AlgNode node, Statement statement, @Nullable AllocationEntity target, @Nullable List<Long> excludedPlacements ) {
        if ( node instanceof LpgScan ) {
            return super.handleGraphScan( (LogicalLpgScan) node, statement, target, excludedPlacements );
        }
        int i = 0;
        List<AlgNode> inputs = new ArrayList<>();
        for ( AlgNode input : node.getInputs() ) {
            inputs.add( i, buildGraphDml( input, statement, target, excludedPlacements ) );
            i++;
        }
        return node.copy( node.getTraitSet(), inputs );
    }


    private AlgBuilder buildDml( AlgNode node, Statement statement, RoutedAlgBuilder builder, @Nullable AllocationEntity target, @Nullable List<Long> excludedPlacements ) {

        for ( int i = 0; i < node.getInputs().size(); i++ ) {
            buildDml( node.getInput( i ), statement, builder, target, excludedPlacements );
        }

        if ( node instanceof LogicalDocumentScan ) {
            return builder.push( super.handleDocScan(
                    (DocumentScan<?>) node,
                    statement,
                    null ) );
        } else if ( node instanceof LogicalRelScan ) {
            return super.handleRelScan(
                    builder,
                    statement,
                    node.getEntity() );

        } else if ( node instanceof LogicalLpgScan ) {
            return AlgBuilder.create( statement ).push( super.handleGraphScan(
                    (LogicalLpgScan) node,
                    statement,
                    target,
                    excludedPlacements ) );
        } else if ( node instanceof LogicalRelValues values ) {

            return super.handleValues( values, builder );
        } else if ( node instanceof LogicalDocumentValues ) {
            return builder.push( node );
        }

        return super.handleGeneric( node, builder );
    }


    private AlgBuilder buildDml(
            AlgNode node,
            RoutedAlgBuilder builder,
            LogicalTable table,
            List<AllocationColumn> placements,
            AllocationEntity allocEntity,
            Statement statement,
            AlgCluster cluster,
            boolean remapParameterValues,
            List<Map<Long, PolyValue>> parameterValues ) {
        for ( int i = 0; i < node.getInputs().size(); i++ ) {
            buildDml( node.getInput( i ), builder, table, placements, allocEntity, statement, cluster, remapParameterValues, parameterValues );
        }

        if ( log.isDebugEnabled() ) {
            log.debug( "List of Store specific ColumnPlacements: " );
            for ( AllocationColumn ccp : placements ) {
                log.debug( "{}.{}", ccp.adapterId, ccp.getLogicalColumnName() );
            }
        }

        if ( node instanceof LogicalDocumentScan ) {
            return handleLogicalDocumentScan( builder, statement );
        } else if ( node instanceof LogicalRelScan && node.getEntity() != null ) {
            return handleRelScan( builder, statement, getParentOrCurrent( allocEntity, ((LogicalRelScan) node).entity ) );
        } else if ( node instanceof LogicalDocumentValues ) {
            return handleDocuments( (LogicalDocumentValues) node, builder );
        } else if ( node instanceof Values ) {
            return handleValues( node, builder, table, placements );
        } else if ( node instanceof LogicalRelProject ) {
            return handleLogicalProject( node, builder, table, placements, statement, remapParameterValues, parameterValues );
        } else if ( node instanceof LogicalRelFilter ) {
            return handleLogicalFilter( node, builder, table, placements, statement );
        } else {
            return super.handleGeneric( node, builder );
        }
    }


    private Entity getParentOrCurrent( AllocationEntity allocEntity, Entity entity ) {
        if ( allocEntity == null || allocEntity.logicalId != entity.id ) {
            return entity;
        }
        return allocEntity;
    }


    private RoutedAlgBuilder handleLogicalFilter( AlgNode node, RoutedAlgBuilder builder, LogicalTable table, List<AllocationColumn> placements, Statement statement ) {
        List<LogicalColumn> columns = statement.getTransaction().getSnapshot().rel().getColumns( table.id );
        if ( columns.size() != placements.size() ) { // partitioned, check if there is a illegal condition
            RexCall call = ((RexCall) ((LogicalRelFilter) node).getCondition());

            for ( RexNode operand : call.operands ) {
                dmlConditionCheck( (LogicalRelFilter) node, table, placements, operand );
            }
        }
        return super.handleGeneric( node, builder );
    }


    @NotNull
    private RoutedAlgBuilder handleLogicalDocumentScan( RoutedAlgBuilder builder, Statement statement ) {
        builder = super.handleRelScan(
                builder,
                statement,
                null );
        LogicalRelScan scan = (LogicalRelScan) builder.build();
        builder.push( scan.copy( scan.getTraitSet().replace( ModelTrait.DOCUMENT ), scan.getInputs() ) );
        return builder;
    }


    private AlgBuilder handleLogicalProject( AlgNode node, RoutedAlgBuilder builder, LogicalTable table, List<AllocationColumn> placements, Statement statement, boolean remapParameterValues, List<Map<Long, PolyValue>> parameterValues ) {
        List<LogicalColumn> columns = statement.getTransaction().getSnapshot().rel().getColumns( table.id );
        PartitionProperty property = statement.getTransaction().getSnapshot().alloc().getPartitionProperty( table.id ).orElseThrow();
        if ( columns.size() == placements.size() ) { // full placement, generic handling is sufficient
            if ( property.isPartitioned && remapParameterValues ) {
                return remapParameterizedDml( node, builder, statement, parameterValues );
            } else {
                return super.handleGeneric( node, builder );
            }
        } else { // vertically partitioned, adjust project
            if ( ((LogicalRelProject) node).getInput().getTupleType().toString().equals( "RecordType(INTEGER ZERO)" ) ) {
                if ( property.isPartitioned && remapParameterValues ) {
                    builder = remapParameterizedDml( node, builder, statement, parameterValues );
                }
                builder.push( node.copy( node.getTraitSet(), ImmutableList.of( builder.peek( 0 ) ) ) );
                List<RexNode> rexNodes = new ArrayList<>();
                for ( AllocationColumn ccp : placements ) {
                    rexNodes.add( builder.field( ccp.getLogicalColumnName() ) );
                }
                return builder.project( rexNodes );
            } else {
                List<RexNode> rexNodes = new ArrayList<>();
                for ( AllocationColumn ccp : placements ) {
                    rexNodes.add( builder.field( ccp.getLogicalColumnName() ) );
                }
                for ( RexNode rexNode : ((LogicalRelProject) node).getProjects() ) {
                    if ( !(rexNode instanceof RexIndexRef) ) {
                        rexNodes.add( rexNode );
                    }
                }
                return builder.project( rexNodes );
            }
        }
    }


    private AlgBuilder handleValues( AlgNode node, RoutedAlgBuilder builder, LogicalTable table, List<AllocationColumn> placements ) {
        LogicalRelValues values = (LogicalRelValues) node;

        builder = super.handleValues( values, builder );

        List<LogicalColumn> columns = Catalog.snapshot().rel().getColumns( table.id );
        if ( columns.size() == placements.size() ) { // full placement, no additional checks required
            return builder;
        } else if ( node.getTupleType().toString().equals( "RecordType(INTEGER ZERO)" ) ) {
            // This is a prepared statement. Actual values are in the project. Do nothing
            return builder;
        } else { // partitioned, add additional project
            ArrayList<RexNode> rexNodes = new ArrayList<>();
            for ( AllocationColumn ccp : placements ) {
                rexNodes.add( builder.field( ccp.getLogicalColumnName() ) );
            }
            return builder.project( rexNodes );
        }
    }


    private AlgBuilder handleSelectFromOtherTable( RoutedAlgBuilder builder, LogicalTable catalogTable, Statement statement ) {
        Snapshot snapshot = statement.getTransaction().getSnapshot();
        // Select from other table
        snapshot = statement.getDataContext().getSnapshot();
        if ( snapshot.alloc().getFromLogical( catalogTable.id ).size() > 1 ) {
            throw new UnsupportedOperationException( "DMLs from other partitioned tables is not supported" );
        }

        long pkid = catalogTable.primaryKey;
        List<Long> pkColumnIds = snapshot.rel().getPrimaryKey( pkid ).orElseThrow().fieldIds;
        LogicalColumn pkColumn = snapshot.rel().getColumn( pkColumnIds.get( 0 ) ).orElseThrow();
        List<AllocationColumn> pkPlacements = snapshot.alloc().getColumnFromLogical( pkColumn.id ).orElseThrow();

        List<AlgNode> nodes = new ArrayList<>();
        for ( AllocationColumn pkPlacement : pkPlacements ) {

            nodes.add( super.handleRelScan(
                    builder,
                    statement,
                    catalogTable ).build() );
        }

        if ( nodes.size() == 1 ) {
            return builder.push( nodes.get( 0 ) );
        }

        return builder.pushAll( nodes ).union( true );
    }


    private void dmlConditionCheck( LogicalRelFilter node, LogicalTable catalogTable, List<AllocationColumn> placements, RexNode operand ) {
        if ( operand instanceof RexIndexRef ) {
            int index = ((RexIndexRef) operand).getIndex();
            AlgDataTypeField field = node.getInput().getTupleType().getFields().get( index );
            LogicalColumn column;
            String columnName;
            String[] columnNames = field.getName().split( "\\." );
            if ( columnNames.length == 1 ) { // columnName
                columnName = columnNames[0];
            } else if ( columnNames.length == 2 ) { // tableName.columnName
                if ( !catalogTable.name.equalsIgnoreCase( columnNames[0] ) ) {
                    throw new GenericRuntimeException( "Table name does not match expected table name: " + field.getName() );
                }
                columnName = columnNames[1];
            } else if ( columnNames.length == 3 ) { // schemaName.tableName.columnName
                if ( !Catalog.snapshot().getNamespace( catalogTable.id ).orElseThrow().name.equalsIgnoreCase( columnNames[0] ) ) {
                    throw new GenericRuntimeException( "Schema name does not match expected schema name: " + field.getName() );
                }
                if ( !catalogTable.name.equalsIgnoreCase( columnNames[1] ) ) {
                    throw new GenericRuntimeException( "Table name does not match expected table name: " + field.getName() );
                }
                columnName = columnNames[2];
            } else {
                throw new GenericRuntimeException( "Invalid column name: " + field.getName() );
            }
            column = Catalog.snapshot().rel().getColumn( catalogTable.id, columnName ).orElseThrow();
            if ( Catalog.snapshot().alloc().getColumn( placements.get( 0 ).placementId, column.id ).isEmpty() ) {
                throw new GenericRuntimeException( "Current implementation of vertical partitioning does not allow conditions on partitioned columns. " );
                // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                // TODO: Use indexes
            }
        } else if ( operand instanceof RexCall ) {
            for ( RexNode o : ((RexCall) operand).operands ) {
                dmlConditionCheck( node, catalogTable, placements, o );
            }
        }
    }


    private RoutedAlgBuilder remapParameterizedDml( AlgNode node, RoutedAlgBuilder builder, Statement statement, List<Map<Long, PolyValue>> parameterValues ) {
        if ( parameterValues.size() <= 1 ) {
            // changed for now, this should not be a problem
            throw new GenericRuntimeException( "The parameter values is expected to have a size of one in this case!" );
        }

        List<RexNode> projects = new ArrayList<>();
        for ( RexNode project : ((LogicalRelProject) node).getProjects() ) {
            if ( project instanceof RexDynamicParam ) {
                long newIndex = parameterValues.get( 0 ).size();
                long oldIndex = ((RexDynamicParam) project).getIndex();
                AlgDataType type = statement.getDataContext().getParameterType( oldIndex );
                if ( type == null ) {
                    type = project.getType();
                }
                PolyValue value = parameterValues.get( 0 ).get( oldIndex );
                projects.add( new RexDynamicParam( type, newIndex ) );
                parameterValues.get( 0 ).put( newIndex, value );
            }
        }

        LogicalRelValues logicalRelValues = LogicalRelValues.createOneRow( node.getCluster() );
        LogicalRelProject newProject = new LogicalRelProject(
                node.getCluster(),
                node.getTraitSet(),
                logicalRelValues,
                projects,
                node.getTupleType()
        );
        return super.handleGeneric( newProject, builder );
    }

}
