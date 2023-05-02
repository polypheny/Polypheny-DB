/*
 * Copyright 2019-2023 The Polypheny Project
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.core.common.BatchIterator;
import org.polypheny.db.algebra.core.common.ConditionalExecute;
import org.polypheny.db.algebra.core.common.ConstraintEnforcer;
import org.polypheny.db.algebra.core.common.Modify;
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
import org.polypheny.db.algebra.logical.relational.LogicalFilter;
import org.polypheny.db.algebra.logical.relational.LogicalModifyCollect;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.partition.PartitionManager;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.processing.WhereClauseVisitor;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.routing.DmlRouter;
import org.polypheny.db.routing.LogicalQueryInformation;
import org.polypheny.db.routing.RoutingManager;
import org.polypheny.db.schema.Entity;
import org.polypheny.db.schema.graph.ModifiableGraph;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.schema.types.ModifiableEntity;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.RoutedAlgBuilder;
import org.polypheny.db.transaction.Statement;

@Slf4j
public class DmlRouterImpl extends BaseRouter implements DmlRouter {


    @Override
    public AlgNode routeDml( LogicalRelModify modify, Statement statement ) {
        AlgOptCluster cluster = modify.getCluster();

        if ( modify.getEntity() == null ) {
            throw new RuntimeException( "Unexpected operator!" );
        }

        LogicalTable catalogTable = modify.getEntity().unwrap( LogicalTable.class );
        Snapshot snapshot = statement.getTransaction().getSnapshot();

        // Get placements of this table

        // Make sure that this table can be modified
        if ( !catalogTable.modifiable ) {
            if ( catalogTable.entityType == EntityType.ENTITY ) {
                throw new RuntimeException( "Unable to modify a table marked as read-only!" );
            } else if ( catalogTable.entityType == EntityType.SOURCE ) {
                throw new RuntimeException( "The table '" + catalogTable.name + "' is provided by a data source which does not support data modification." );
            } else if ( catalogTable.entityType == EntityType.VIEW ) {
                throw new RuntimeException( "Polypheny-DB does not support modifying views." );
            }
            throw new RuntimeException( "Unknown table type: " + catalogTable.entityType.name() );
        }

        long pkid = catalogTable.primaryKey;
        List<Long> pkColumnIds = snapshot.rel().getPrimaryKey( pkid ).columnIds;
        LogicalColumn pkColumn = snapshot.rel().getColumn( pkColumnIds.get( 0 ) );

        List<AllocationEntity> allocs = snapshot.alloc().getFromLogical( catalogTable.id );

        AllocationEntity allocation = allocs.get( 0 );
        ModifiableEntity modifiableTable = allocation.unwrap( ModifiableEntity.class );

        AlgNode input = buildDmlNew(
                super.recursiveCopy( modify.getInput( 0 ) ),
                statement
        ).build();

        // Build DML

        //List<String> updateColumnList = modify.getUpdateColumnList();
        //List<? extends RexNode> sourceExpressionList = modify.getSourceExpressionList();

        /*return modifiableTable.toModificationAlg(
                cluster,
                cluster.traitSet(),
                physical,
                input,
                modify.getOperation(),
                updateColumnList,
                sourceExpressionList );*/
        Convention convention = AdapterManager.getInstance().getAdapter( allocation.adapterId ).getCurrentSchema().getConvention();
        if ( convention != null ) {
            convention.register( modify.getCluster().getPlanner() );
        }
        return LogicalRelModify.create( allocation, input, modify.getOperation(), modify.getUpdateColumnList(), modify.getSourceExpressionList(), modify.isFlattened() );

    }


    /**
     * Default implementation: Execute DML on all placements
     */
    public AlgNode routeDmlOld( LogicalRelModify modify, Statement statement ) {
        AlgOptCluster cluster = modify.getCluster();

        if ( modify.getEntity() == null ) {
            throw new RuntimeException( "Unexpected operator!" );
        }

        LogicalTable catalogTable = modify.getEntity().unwrap( LogicalTable.class );
        Snapshot snapshot = statement.getTransaction().getSnapshot();

        // Get placements of this table

        // Make sure that this table can be modified
        if ( !catalogTable.modifiable ) {
            if ( catalogTable.entityType == EntityType.ENTITY ) {
                throw new RuntimeException( "Unable to modify a table marked as read-only!" );
            } else if ( catalogTable.entityType == EntityType.SOURCE ) {
                throw new RuntimeException( "The table '" + catalogTable.name + "' is provided by a data source which does not support data modification." );
            } else if ( catalogTable.entityType == EntityType.VIEW ) {
                throw new RuntimeException( "Polypheny-DB does not support modifying views." );
            }
            throw new RuntimeException( "Unknown table type: " + catalogTable.entityType.name() );
        }

        long pkid = catalogTable.primaryKey;
        List<Long> pkColumnIds = snapshot.rel().getPrimaryKey( pkid ).columnIds;
        LogicalColumn pkColumn = snapshot.rel().getColumn( pkColumnIds.get( 0 ) );

        // Essentially gets a list of all stores where this table resides
        List<AllocationColumn> pkPlacements = snapshot.alloc().getColumnFromLogical( pkColumn.id );
        List<AllocationEntity> allocs = snapshot.alloc().getFromLogical( catalogTable.id );//.getPartitionProperty( catalogTable.id );
        if ( !allocs.isEmpty() && log.isDebugEnabled() ) {
            log.debug( "\nListing all relevant stores for table: '{}' and all partitions: {}", catalogTable.name, -1 );//property.partitionGroupIds );
            for ( AllocationColumn dataPlacement : pkPlacements ) {
                log.debug(
                        "\t\t -> '{}' {}\t{}",
                        dataPlacement.adapterId,
                        snapshot.alloc().getPartitionGroupsOnDataPlacement( dataPlacement.adapterId, dataPlacement.tableId ),
                        snapshot.alloc().getPartitionGroupsIndexOnDataPlacement( dataPlacement.adapterId, dataPlacement.tableId ) );
            }
        }

        // Execute on all primary key placements
        List<AlgNode> modifies = new ArrayList<>();

        // Needed for partitioned updates when source partition and target partition are not equal
        // SET Value is the new partition, where clause is the source
        boolean operationWasRewritten = false;
        List<Map<Long, Object>> tempParamValues = null;

        Map<Long, AlgDataType> types = statement.getDataContext().getParameterTypes();
        List<Map<Long, Object>> allValues = statement.getDataContext().getParameterValues();

        Map<Long, Object> newParameterValues = new HashMap<>();
        for ( AllocationColumn pkPlacement : pkPlacements ) {

            // Get placements on store
            List<AllocationColumn> placementsOnAdapter = snapshot.alloc().getColumnPlacementsOnAdapterPerTable( pkPlacement.adapterId, catalogTable.id );

            // If this is an update, check whether we need to execute on this store at all
            List<String> updateColumnList = modify.getUpdateColumnList();
            List<? extends RexNode> sourceExpressionList = modify.getSourceExpressionList();
            List<LogicalColumn> columns = snapshot.rel().getColumns( catalogTable.id );
            if ( placementsOnAdapter.size() != columns.size() ) {

                if ( modify.getOperation() == Modify.Operation.UPDATE ) {
                    updateColumnList = new LinkedList<>( modify.getUpdateColumnList() );
                    sourceExpressionList = new LinkedList<>( modify.getSourceExpressionList() );
                    Iterator<String> updateColumnListIterator = updateColumnList.iterator();
                    Iterator<? extends RexNode> sourceExpressionListIterator = sourceExpressionList.iterator();
                    while ( updateColumnListIterator.hasNext() ) {
                        String columnName = updateColumnListIterator.next();
                        sourceExpressionListIterator.next();
                        LogicalColumn logicalColumn = snapshot.rel().getColumn( catalogTable.id, columnName );
                        if ( snapshot.alloc().getAllocation( pkPlacement.adapterId, logicalColumn.id ) == null ) {
                            updateColumnListIterator.remove();
                            sourceExpressionListIterator.remove();
                        }
                    }
                    if ( updateColumnList.size() == 0 ) {
                        continue;
                    }
                }
            }

            long identPart = -1;
            long identifiedPartitionForSetValue = -1;
            Set<Long> accessedPartitionList = new HashSet<>();
            // Identify where clause of UPDATE
            if ( allocs.size() > 1 ) {
                boolean worstCaseRouting = false;
                Set<Long> identifiedPartitionsInFilter = new HashSet<>();

                PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
                PartitionManager partitionManager = partitionManagerFactory.getPartitionManager( allocs.get( 0 ).getPartitionType() );

                WhereClauseVisitor whereClauseVisitor = new WhereClauseVisitor( statement, columns.indexOf( -1 ) );//property.partitionColumnId ) );
                modify.accept( new AlgShuttleImpl() {
                    @Override
                    public AlgNode visit( LogicalFilter filter ) {
                        super.visit( filter );
                        filter.accept( whereClauseVisitor );
                        return filter;
                    }
                } );

                List<String> whereClauseValues = null;
                if ( !whereClauseVisitor.getValues().isEmpty() ) {

                    whereClauseValues = whereClauseVisitor.getValues().stream()
                            .map( Object::toString )
                            .collect( Collectors.toList() );
                    if ( log.isDebugEnabled() ) {
                        log.debug( "Found Where Clause Values: {}", whereClauseValues );
                    }
                    worstCaseRouting = true;
                }

                if ( whereClauseValues != null ) {
                    for ( String value : whereClauseValues ) {
                        worstCaseRouting = false;
                        identPart = (int) partitionManager.getTargetPartitionId( catalogTable, value );
                        accessedPartitionList.add( identPart );
                        identifiedPartitionsInFilter.add( identPart );
                    }
                }

                String partitionValue = "";
                // Set true if partitionColumn is part of UPDATE Statement, else assume worst case routing

                if ( modify.getOperation() == Modify.Operation.UPDATE ) {
                    // In case of update always use worst case routing for now.
                    // Since you have to identify the current partition to delete the entry and then create a new entry on the correct partitions
                    int index = 0;

                    for ( String cn : updateColumnList ) {
                        if ( snapshot.rel().getColumn( catalogTable.id, cn ).id == -1 ) {//property.partitionColumnId ) {
                            if ( log.isDebugEnabled() ) {
                                log.debug( " UPDATE: Found PartitionColumnID Match: '{}' at index: {}", -1, index );//property.partitionColumnId, index );
                            }
                            // Routing/Locking can now be executed on certain partitions
                            partitionValue = sourceExpressionList.get( index ).toString().replace( "'", "" );
                            if ( log.isDebugEnabled() ) {
                                log.debug(
                                        "UPDATE: partitionColumn-value: '{}' should be put on partition: {}",
                                        partitionValue,
                                        partitionManager.getTargetPartitionId( catalogTable, partitionValue ) );
                            }
                            identPart = (int) partitionManager.getTargetPartitionId( catalogTable, partitionValue );
                            // Needed to verify if UPDATE shall be executed on two partitions or not
                            identifiedPartitionForSetValue = identPart;
                            accessedPartitionList.add( identPart );
                            break;
                        }
                        index++;
                    }

                    // If WHERE clause has any value for partition column
                    if ( identifiedPartitionsInFilter.size() > 0 ) {

                        // Partition has been identified in SET
                        if ( identifiedPartitionForSetValue != -1 ) {

                            // SET value and single WHERE clause point to same partition.
                            // Inplace update possible
                            if ( identifiedPartitionsInFilter.size() == 1 && identifiedPartitionsInFilter.contains( identifiedPartitionForSetValue ) ) {
                                if ( log.isDebugEnabled() ) {
                                    log.debug( "oldValue and new value reside on same partition: {}", identifiedPartitionForSetValue );
                                }
                                worstCaseRouting = false;
                            } else {
                                throw new RuntimeException( "Updating partition key is not allowed" );
                            }
                        }// WHERE clause only
                        else {
                            throw new RuntimeException( "Updating partition key is not allowed" );

                            //Simply execute the UPDATE on all identified partitions
                            //Nothing to do
                            //worstCaseRouting = false;
                        }
                    }// If only SET is specified
                    // Changes the value of partition column of complete table to only reside on one partition
                    else if ( identifiedPartitionForSetValue != -1 ) {

                        // Data Migrate copy of all other partitions beside the identified on towards the identified one
                        // Then inject a DELETE statement for all those partitions

                        // Do the update only on the identified partition

                    }// If nothing has been specified
                    //Partition functionality cannot be used --> worstCase --> send query to every partition
                    else {
                        worstCaseRouting = true;
                        accessedPartitionList = allocs.stream().map( a -> a.id ).collect( Collectors.toSet() );
                    }

                } else if ( modify.getOperation() == Modify.Operation.INSERT ) {
                    int i;

                    if ( modify.getInput() instanceof LogicalValues ) {

                        // Get fieldList and map columns to index since they could be in arbitrary order
                        int partitionColumnIndex = -1;
                        Map<Long, Integer> resultColMapping = new HashMap<>();
                        for ( int j = 0; j < (modify.getInput()).getRowType().getFieldList().size(); j++ ) {
                            String columnFieldName = (modify.getInput()).getRowType().getFieldList().get( j ).getKey();

                            // Retrieve columnId of fieldName and map it to its fieldList location of INSERT Stmt
                            int columnIndex = columns.stream().map( c -> c.name ).collect( Collectors.toList() ).indexOf( columnFieldName );
                            resultColMapping.put( columns.stream().map( c -> c.id ).collect( Collectors.toList() ).get( columnIndex ), j );

                            // Determine location of partitionColumn in fieldList
                            if ( columns.stream().map( c -> c.id ).collect( Collectors.toList() ).get( columnIndex ) == -1 ) {//property.partitionColumnId ) {
                                partitionColumnIndex = columnIndex;
                                if ( log.isDebugEnabled() ) {
                                    log.debug( "INSERT: Found PartitionColumnID: '{}' at column index: {}", -1, j );//property.partitionColumnId, j );
                                    worstCaseRouting = false;

                                }
                            }
                        }

                        // Will executed all required tuples that belong on the same partition jointly
                        Map<Long, List<ImmutableList<RexLiteral>>> tuplesOnPartition = new HashMap<>();
                        for ( ImmutableList<RexLiteral> currentTuple : ((LogicalValues) modify.getInput()).tuples ) {

                            worstCaseRouting = false;
                            if ( partitionColumnIndex == -1 || currentTuple.get( partitionColumnIndex ).getValue() == null ) {
                                partitionValue = partitionManager.getUnifiedNullValue();
                            } else {
                                partitionValue = currentTuple.get( partitionColumnIndex ).toString().replace( "'", "" );
                            }
                            identPart = (int) partitionManager.getTargetPartitionId( catalogTable, partitionValue );
                            accessedPartitionList.add( identPart );

                            if ( !tuplesOnPartition.containsKey( identPart ) ) {
                                tuplesOnPartition.put( identPart, new ArrayList<>() );
                            }
                            tuplesOnPartition.get( identPart ).add( currentTuple );

                        }

                        for ( Map.Entry<Long, List<ImmutableList<RexLiteral>>> partitionMapping : tuplesOnPartition.entrySet() ) {
                            Long currentPartitionId = partitionMapping.getKey();

                            if ( !snapshot.alloc().getPartitionsOnDataPlacement( pkPlacement.adapterId, catalogTable.id ).contains( currentPartitionId ) ) {
                                continue;
                            }

                            for ( ImmutableList<RexLiteral> row : partitionMapping.getValue() ) {
                                LogicalValues newLogicalValues = new LogicalValues(
                                        cluster,
                                        cluster.traitSet(),
                                        (modify.getInput()).getRowType(),
                                        ImmutableList.copyOf( ImmutableList.of( row ) ) );

                                AlgNode input = buildDml(
                                        newLogicalValues,
                                        RoutedAlgBuilder.create( statement, cluster ),
                                        catalogTable,
                                        placementsOnAdapter,
                                        snapshot.alloc().getAllocation( pkPlacement.adapterId, currentPartitionId ),
                                        statement,
                                        cluster,
                                        true,
                                        statement.getDataContext().getParameterValues() ).build();

                                AllocationEntity allocation = snapshot.alloc().getAllocation( currentPartitionId );
                                AlgNode node = AdapterManager.getInstance().getAdapter( allocation.adapterId ).getScan( allocation.id, AlgBuilder.create( statement ) );
                                ModifiableEntity modifiableTable = node.getEntity().unwrap( ModifiableEntity.class );

                                // Build DML
                                Modify<?> adjustedModify = modifiableTable.toModificationAlg(
                                        cluster,
                                        cluster.traitSet(),
                                        allocation,
                                        input,
                                        modify.getOperation(),
                                        updateColumnList,
                                        sourceExpressionList );

                                modifies.add( adjustedModify );

                            }
                        }
                        operationWasRewritten = true;

                    } else if ( modify.getInput() instanceof LogicalProject
                            && ((LogicalProject) modify.getInput()).getInput() instanceof LogicalValues ) {

                        String partitionColumnName = "empty";//snapshot.rel().getColumn( property.partitionColumnId ).name;
                        List<String> fieldNames = modify.getInput().getRowType().getFieldNames();

                        LogicalRelModify ltm = modify;
                        LogicalProject lproject = (LogicalProject) ltm.getInput();

                        List<RexNode> fieldValues = lproject.getProjects();

                        for ( i = 0; i < fieldNames.size(); i++ ) {
                            String columnName = fieldNames.get( i );

                            if ( partitionColumnName.equals( columnName ) ) {

                                if ( ((LogicalProject) modify.getInput()).getProjects().get( i ).getKind().equals( Kind.DYNAMIC_PARAM ) ) {

                                    // Needed to identify the column which contains the partition value
                                    long partitionValueIndex = ((RexDynamicParam) fieldValues.get( i )).getIndex();

                                    long tempPartitionId = 0;
                                    // Get partitionValue per row/tuple to be inserted
                                    // Create as many independent TableModifies as there are entries in getParameterValues

                                    Map<Long, List<Map<Long, Object>>> tempValues = new HashMap<>();
                                    statement.getDataContext().resetContext();
                                    for ( Map<Long, Object> currentRow : allValues ) {
                                        // first we sort the values to insert according to the partitionManager and their partitionId

                                        tempPartitionId = partitionManager.getTargetPartitionId( catalogTable, currentRow.get( partitionValueIndex ).toString() );

                                        if ( !snapshot.alloc().getPartitionsOnDataPlacement( pkPlacement.adapterId, catalogTable.id ).contains( tempPartitionId ) ) {
                                            continue;
                                        }

                                        statement.getDataContext().setParameterTypes( statement.getDataContext().getParameterTypes() );
                                        statement.getDataContext().setParameterValues( tempValues.get( tempPartitionId ) );

                                        List<Map<Long, Object>> parameterValues = new ArrayList<>();
                                        parameterValues.add( new HashMap<>( newParameterValues ) );
                                        parameterValues.get( 0 ).putAll( currentRow );

                                        if ( !tempValues.containsKey( tempPartitionId ) ) {
                                            tempValues.put( tempPartitionId, new ArrayList<>() );
                                        }
                                        tempValues.get( tempPartitionId ).add( currentRow );
                                    }

                                    for ( Entry<Long, List<Map<Long, Object>>> entry : tempValues.entrySet() ) {
                                        // then we add a modification for each partition
                                        statement.getDataContext().setParameterValues( entry.getValue() );

                                        AlgNode input = buildDml(
                                                super.recursiveCopy( modify.getInput( 0 ) ),
                                                RoutedAlgBuilder.create( statement, cluster ),
                                                catalogTable,
                                                placementsOnAdapter,
                                                snapshot.alloc().getAllocation( pkPlacement.adapterId, entry.getKey() ),
                                                statement,
                                                cluster,
                                                false,
                                                entry.getValue() ).build();

                                        PhysicalTable physical = input.getEntity().unwrap( PhysicalTable.class );
                                        ModifiableEntity modifiableTable = physical.unwrap( ModifiableEntity.class );

                                        // Build DML
                                        Modify<?> adjustedModify = modifiableTable.toModificationAlg(
                                                cluster,
                                                modify.getTraitSet(),
                                                physical,
                                                input,
                                                modify.getOperation(),
                                                updateColumnList,
                                                sourceExpressionList );

                                        statement.getDataContext().addContext();
                                        modifies.add( new LogicalContextSwitcher( adjustedModify ) );
                                    }

                                    operationWasRewritten = true;
                                    worstCaseRouting = false;
                                } else {
                                    partitionValue = ((LogicalProject) modify.getInput()).getProjects().get( i ).toString().replace( "'", "" );
                                    identPart = (int) partitionManager.getTargetPartitionId( catalogTable, partitionValue );
                                    accessedPartitionList.add( identPart );
                                    worstCaseRouting = false;
                                }
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
                        String partitionColumnName = "empty";//snapshot.rel().getColumn( property.partitionColumnId ).name;
                        // String partitionName = snapshot.alloc().getPartitionGroup( identPart ).partitionGroupName;
                        log.debug( "INSERT: partitionColumn-value: '{}' should be put on partition: {}, which is partitioned with column {}",
                                partitionValue, identPart, partitionColumnName );
                    }


                } else if ( modify.getOperation() == Modify.Operation.DELETE ) {
                    if ( whereClauseValues == null ) {
                        worstCaseRouting = true;
                    } else {
                        worstCaseRouting = whereClauseValues.size() >= 4;
                    }

                }

                if ( worstCaseRouting ) {
                    log.debug( "PartitionColumnID was not an explicit part of statement, partition routing will therefore assume worst-case: Routing to ALL PARTITIONS" );
                    accessedPartitionList = allocs.stream().map( a -> a.id ).collect( Collectors.toSet() );//property.partitionIds );
                }
            } else {
                // un-partitioned tables only have one partition anyway
                identPart = allocs.get( 0 ).id;//property.partitionIds.get( 0 );
                accessedPartitionList.add( identPart );
            }

            if ( statement.getMonitoringEvent() != null ) {
                statement.getMonitoringEvent()
                        .updateAccessedPartitions(
                                Collections.singletonMap( catalogTable.id, accessedPartitionList )
                        );
            }

            if ( !operationWasRewritten ) {

                for ( long partitionId : accessedPartitionList ) {

                    if ( !snapshot.alloc().getPartitionsOnDataPlacement( pkPlacement.adapterId, catalogTable.id ).contains( partitionId ) ) {
                        continue;
                    }

                    AllocationEntity alloc = snapshot.alloc().getAllocation( partitionId );

                    // Build DML
                    Modify<?> adjustedModify;
                    AlgNode input = buildDml(
                            super.recursiveCopy( modify.getInput( 0 ) ),
                            RoutedAlgBuilder.create( statement, cluster ),
                            catalogTable,
                            placementsOnAdapter,
                            alloc,
                            statement,
                            cluster,
                            false,
                            statement.getDataContext().getParameterValues() ).build();

                    ModifiableEntity modifiableTable = input.getEntity().unwrap( ModifiableEntity.class );

                    if ( modifiableTable != null && modifiableTable == input.getEntity().unwrap( Entity.class ) ) {
                        adjustedModify = modifiableTable.toModificationAlg(
                                cluster,
                                input.getTraitSet(),
                                alloc,
                                input,
                                modify.getOperation(),
                                updateColumnList,
                                sourceExpressionList
                        );
                    } else {
                        adjustedModify = LogicalRelModify.create(
                                alloc,
                                input,
                                modify.getOperation(),
                                updateColumnList,
                                sourceExpressionList,
                                modify.isFlattened()
                        );
                    }
                    modifies.add( adjustedModify );
                }
            }
        }

        // Update parameter values (horizontal partitioning)
        if ( !newParameterValues.isEmpty() ) {
            statement.getDataContext().resetParameterValues();
            int idx = 0;
            for ( Map.Entry<Long, Object> entry : newParameterValues.entrySet() ) {
                statement.getDataContext().addParameterValues(
                        entry.getKey(),
                        statement.getDataContext().getParameterType( idx++ ),
                        ImmutableList.of( entry.getValue() ) );
            }
        }

        if ( modifies.size() == 1 ) {
            return modifies.get( 0 );
        } else {
            return new LogicalModifyCollect( modify.getCluster(), modify.getTraitSet(), modifies, true );
        }
    }


    @Override
    public AlgNode handleConditionalExecute( AlgNode node, Statement statement, LogicalQueryInformation queryInformation ) {
        LogicalConditionalExecute lce = (LogicalConditionalExecute) node;
        RoutedAlgBuilder builder = RoutedAlgBuilder.create( statement, node.getCluster() );
        builder = RoutingManager.getInstance().getFallbackRouter().routeFirst( lce.getLeft(), builder, statement, node.getCluster(), queryInformation );
        AlgNode action;
        if ( lce.getRight() instanceof LogicalConditionalExecute ) {
            action = handleConditionalExecute( lce.getRight(), statement, queryInformation );
        } else if ( lce.getRight() instanceof LogicalRelModify ) {
            action = routeDml( (LogicalRelModify) lce.getRight(), statement );
        } else {
            throw new IllegalArgumentException();
        }

        return LogicalConditionalExecute.create( builder.build(), action, lce );
    }


    @Override
    public AlgNode handleConstraintEnforcer( AlgNode alg, Statement statement, LogicalQueryInformation queryInformation ) {
        LogicalConstraintEnforcer constraint = (LogicalConstraintEnforcer) alg;
        RoutedAlgBuilder builder = RoutedAlgBuilder.create( statement, alg.getCluster() );
        builder = RoutingManager.getInstance().getFallbackRouter().routeFirst( constraint.getRight(), builder, statement, alg.getCluster(), queryInformation );

        if ( constraint.getLeft() instanceof RelModify ) {
            return LogicalConstraintEnforcer.create(
                    routeDml( (LogicalRelModify) constraint.getLeft(), statement ),
                    builder.build(),
                    constraint.getExceptionClasses(),
                    constraint.getExceptionMessages() );
        } else if ( constraint.getLeft() instanceof BatchIterator ) {
            return LogicalConstraintEnforcer.create(
                    handleBatchIterator( constraint.getLeft(), statement, queryInformation ),
                    builder.build(),
                    constraint.getExceptionClasses(),
                    constraint.getExceptionMessages() );
        } else {
            throw new RuntimeException( "The provided modify query for the ConstraintEnforcer was not recognized!" );
        }
    }


    @Override
    public AlgNode handleBatchIterator( AlgNode alg, Statement statement, LogicalQueryInformation queryInformation ) {
        LogicalBatchIterator iterator = (LogicalBatchIterator) alg;
        AlgNode input;
        if ( iterator.getInput() instanceof RelModify ) {
            input = routeDml( (LogicalRelModify) iterator.getInput(), statement );
        } else if ( iterator.getInput() instanceof ConditionalExecute ) {
            input = handleConditionalExecute( iterator.getInput(), statement, queryInformation );
        } else if ( iterator.getInput() instanceof ConstraintEnforcer ) {
            input = handleConstraintEnforcer( iterator.getInput(), statement, queryInformation );
        } else {
            throw new RuntimeException( "BachIterator had an unknown child!" );
        }

        return LogicalBatchIterator.create( input, statement );
    }


    @Override
    public AlgNode routeDocumentDml( LogicalDocumentModify alg, Statement statement, LogicalQueryInformation queryInformation, Long adapterId ) {
        Snapshot snapshot = statement.getTransaction().getSnapshot();

        LogicalCollection collection = alg.entity.unwrap( LogicalCollection.class );

        List<AlgNode> modifies = new ArrayList<>();

        List<AllocationEntity> allocs = snapshot.alloc().getFromLogical( collection.id );

        for ( AllocationEntity allocation : allocs ) {
            modifies.add( alg.toBuilder().entity( allocation ).input( buildDocumentDml( alg.getInput(), statement, queryInformation ) ).build() );
        }

        if ( modifies.size() == 1 ) {
            return modifies.get( 0 );
        }

        return new LogicalModifyCollect( alg.getCluster(), alg.getTraitSet(), modifies, true );
    }


    @Override
    public AlgNode routeGraphDml( LogicalLpgModify alg, Statement statement ) {
        LogicalGraph catalogGraph = alg.entity.unwrap( LogicalGraph.class );
        List<Long> placements = statement
                .getTransaction()
                .getSnapshot()
                .alloc()
                .getFromLogical( catalogGraph.id ).stream().map( c -> c.adapterId )
                .collect( Collectors.toList() );
        return routeGraphDml( alg, statement, catalogGraph, placements );
    }


    @Override
    public AlgNode routeGraphDml( LogicalLpgModify alg, Statement statement, LogicalGraph catalogGraph, List<Long> placements ) {

        Snapshot snapshot = statement.getTransaction().getSnapshot();

        List<AlgNode> modifies = new ArrayList<>();

        for ( long adapterId : placements ) {
            AllocationEntity alloc = snapshot.alloc().getAllocation( catalogGraph.id, adapterId );

            if ( !(alloc instanceof ModifiableGraph) ) {
                throw new RuntimeException( "Graph is not modifiable." );
            }

            modifies.add( ((ModifiableEntity) alloc).toModificationAlg(
                    alg.getCluster(),
                    alg.getTraitSet(),
                    alloc,
                    buildGraphDml( alg.getInput(), statement, adapterId ),
                    alg.operation,
                    alg.ids,
                    alg.operations ) );

        }

        if ( modifies.size() == 1 ) {
            return modifies.get( 0 );
        }

        return new LogicalModifyCollect( modifies.get( 0 ).getCluster(), modifies.get( 0 ).getTraitSet(), modifies, true );
    }

    private AlgNode buildDocumentDml( AlgNode node, Statement statement, LogicalQueryInformation queryInformation ) {
        if ( node instanceof DocumentScan ) {
            return super.handleDocumentScan( (DocumentScan<?>) node, statement, RoutedAlgBuilder.create( statement, node.getCluster() ), null ).build();
        }
        int i = 0;
        List<AlgNode> inputs = new ArrayList<>();
        for ( AlgNode input : node.getInputs() ) {
            inputs.add( i, buildDocumentDml( input, statement, queryInformation ) );
            i++;
        }
        return node.copy( node.getTraitSet(), inputs );
    }


    private AlgNode buildGraphDml( AlgNode node, Statement statement, long adapterId ) {
        if ( node instanceof LpgScan ) {
            return super.handleGraphScan( (LogicalLpgScan) node, statement, adapterId );
        }
        int i = 0;
        List<AlgNode> inputs = new ArrayList<>();
        for ( AlgNode input : node.getInputs() ) {
            inputs.add( i, buildGraphDml( input, statement, adapterId ) );
            i++;
        }
        return node.copy( node.getTraitSet(), inputs );
    }




    private AlgBuilder buildDmlNew( AlgNode algNode, Statement statement ) {

        return RoutedAlgBuilder.create( statement ).push( algNode );
    }


    private AlgBuilder buildDml(
            AlgNode node,
            RoutedAlgBuilder builder,
            LogicalTable catalogTable,
            List<AllocationColumn> placements,
            AllocationEntity allocationTable,
            Statement statement,
            AlgOptCluster cluster,
            boolean remapParameterValues,
            List<Map<Long, Object>> parameterValues ) {
        for ( int i = 0; i < node.getInputs().size(); i++ ) {
            buildDml( node.getInput( i ), builder, catalogTable, placements, allocationTable, statement, cluster, remapParameterValues, parameterValues );
        }

        if ( log.isDebugEnabled() ) {
            log.debug( "List of Store specific ColumnPlacements: " );
            for ( AllocationColumn ccp : placements ) {
                log.debug( "{}.{}", ccp.adapterId, ccp.getLogicalColumnName() );
            }
        }

        if ( node instanceof LogicalDocumentScan ) {
            builder = super.handleScan(
                    builder,
                    statement,
                    allocationTable
            );
            LogicalRelScan scan = (LogicalRelScan) builder.build();
            builder.push( scan.copy( scan.getTraitSet().replace( ModelTrait.DOCUMENT ), scan.getInputs() ) );
            return builder;
        } else if ( node instanceof LogicalRelScan && node.getEntity() != null ) {

            // Special handling for INSERT INTO foo SELECT * FROM foo2
            if ( false ) {
                return handleSelectFromOtherTable( builder, catalogTable, statement );
            }

            builder = super.handleScan(
                    builder,
                    statement,
                    allocationTable
            );

            return builder;


        } else if ( node instanceof Values ) {
            if ( node.getModel() == NamespaceType.DOCUMENT ) {
                return handleDocuments( (LogicalDocumentValues) node, builder );
            }

            LogicalValues values = (LogicalValues) node;

            builder = super.handleValues( values, builder );

            List<LogicalColumn> columns = Catalog.snapshot().rel().getColumns( catalogTable.id );
            if ( columns.size() == placements.size() ) { // full placement, no additional checks required
                return builder;
            } else if ( node.getRowType().toString().equals( "RecordType(INTEGER ZERO)" ) ) {
                // This is a prepared statement. Actual values are in the project. Do nothing
                return builder;
            } else { // partitioned, add additional project
                ArrayList<RexNode> rexNodes = new ArrayList<>();
                for ( AllocationColumn ccp : placements ) {
                    rexNodes.add( builder.field( ccp.getLogicalColumnName() ) );
                }
                return builder.project( rexNodes );
            }
        } else if ( node instanceof LogicalProject ) {
            List<LogicalColumn> columns = statement.getTransaction().getSnapshot().rel().getColumns( catalogTable.id );
            PartitionProperty property = statement.getTransaction().getSnapshot().alloc().getPartitionProperty( catalogTable.id );
            if ( columns.size() == placements.size() ) { // full placement, generic handling is sufficient
                if ( property.isPartitioned && remapParameterValues ) {  //  && ((LogicalProject) node).getInput().getRowType().toString().equals( "RecordType(INTEGER ZERO)" )
                    return remapParameterizedDml( node, builder, statement, parameterValues );
                } else {
                    return super.handleGeneric( node, builder );
                }
            } else { // vertically partitioned, adjust project
                if ( ((LogicalProject) node).getInput().getRowType().toString().equals( "RecordType(INTEGER ZERO)" ) ) {
                    if ( property.isPartitioned && remapParameterValues ) {
                        builder = remapParameterizedDml( node, builder, statement, parameterValues );
                    }
                    builder.push( node.copy( node.getTraitSet(), ImmutableList.of( builder.peek( 0 ) ) ) );
                    ArrayList<RexNode> rexNodes = new ArrayList<>();
                    for ( AllocationColumn ccp : placements ) {
                        rexNodes.add( builder.field( ccp.getLogicalColumnName() ) );
                    }
                    return builder.project( rexNodes );
                } else {
                    ArrayList<RexNode> rexNodes = new ArrayList<>();
                    for ( AllocationColumn ccp : placements ) {
                        rexNodes.add( builder.field( ccp.getLogicalColumnName() ) );
                    }
                    for ( RexNode rexNode : ((LogicalProject) node).getProjects() ) {
                        if ( !(rexNode instanceof RexInputRef) ) {
                            rexNodes.add( rexNode );
                        }
                    }
                    return builder.project( rexNodes );
                }
            }
        } else if ( node instanceof LogicalFilter ) {
            List<LogicalColumn> columns = statement.getTransaction().getSnapshot().rel().getColumns( catalogTable.id );
            if ( columns.size() != placements.size() ) { // partitioned, check if there is a illegal condition
                RexCall call = ((RexCall) ((LogicalFilter) node).getCondition());

                for ( RexNode operand : call.operands ) {
                    dmlConditionCheck( (LogicalFilter) node, catalogTable, placements, operand );
                }
            }
            return super.handleGeneric( node, builder );
        } else {
            return super.handleGeneric( node, builder );
        }
    }


    private AlgBuilder handleSelectFromOtherTable( RoutedAlgBuilder builder, LogicalTable catalogTable, Statement statement ) {
        LogicalTable fromTable = catalogTable;
        Snapshot snapshot = statement.getTransaction().getSnapshot();
        // Select from other table
        snapshot = statement.getDataContext().getSnapshot();
        if ( snapshot.alloc().getFromLogical( fromTable.id ).size() > 1 ) {
            throw new UnsupportedOperationException( "DMLs from other partitioned tables is not supported" );
        }

        long pkid = fromTable.primaryKey;
        List<Long> pkColumnIds = snapshot.rel().getPrimaryKey( pkid ).columnIds;
        LogicalColumn pkColumn = snapshot.rel().getColumn( pkColumnIds.get( 0 ) );
        List<AllocationColumn> pkPlacements = snapshot.alloc().getColumnFromLogical( pkColumn.id );

        List<AlgNode> nodes = new ArrayList<>();
        for ( AllocationColumn pkPlacement : pkPlacements ) {

            snapshot.alloc().getColumnPlacementsOnAdapterPerTable( pkPlacement.adapterId, fromTable.id );

            PartitionProperty property = snapshot.alloc().getPartitionProperty( fromTable.id );

            AllocationEntity alloc = snapshot.alloc().getAllocation( pkPlacement.adapterId, property.partitionIds.get( 0 ) );

            nodes.add( super.handleScan(
                    builder,
                    statement,
                    alloc
            ).build() );

        }

        if ( nodes.size() == 1 ) {
            return builder.push( nodes.get( 0 ) );
        }

        return builder.pushAll( nodes ).union( true );
    }


    private void dmlConditionCheck( LogicalFilter node, LogicalTable catalogTable, List<AllocationColumn> placements, RexNode operand ) {
        if ( operand instanceof RexInputRef ) {
            int index = ((RexInputRef) operand).getIndex();
            AlgDataTypeField field = node.getInput().getRowType().getFieldList().get( index );
            LogicalColumn column;
            String columnName;
            String[] columnNames = field.getName().split( "\\." );
            if ( columnNames.length == 1 ) { // columnName
                columnName = columnNames[0];
            } else if ( columnNames.length == 2 ) { // tableName.columnName
                if ( !catalogTable.name.equalsIgnoreCase( columnNames[0] ) ) {
                    throw new RuntimeException( "Table name does not match expected table name: " + field.getName() );
                }
                columnName = columnNames[1];
            } else if ( columnNames.length == 3 ) { // schemaName.tableName.columnName
                if ( !Catalog.snapshot().getNamespace( catalogTable.id ).name.equalsIgnoreCase( columnNames[0] ) ) {
                    throw new RuntimeException( "Schema name does not match expected schema name: " + field.getName() );
                }
                if ( !catalogTable.name.equalsIgnoreCase( columnNames[1] ) ) {
                    throw new RuntimeException( "Table name does not match expected table name: " + field.getName() );
                }
                columnName = columnNames[2];
            } else {
                throw new RuntimeException( "Invalid column name: " + field.getName() );
            }
            column = Catalog.snapshot().rel().getColumn( catalogTable.id, columnName );
            if ( Catalog.snapshot().alloc().getAllocation( placements.get( 0 ).adapterId, column.id ) == null ) {
                throw new RuntimeException( "Current implementation of vertical partitioning does not allow conditions on partitioned columns. " );
                // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                // TODO: Use indexes
            }
        } else if ( operand instanceof RexCall ) {
            for ( RexNode o : ((RexCall) operand).operands ) {
                dmlConditionCheck( node, catalogTable, placements, o );
            }
        }
    }


    private RoutedAlgBuilder remapParameterizedDml( AlgNode node, RoutedAlgBuilder builder, Statement statement, List<Map<Long, Object>> parameterValues ) {
        if ( parameterValues.size() <= 1 ) {
            // changed for now, this should not be a problem
            throw new RuntimeException( "The parameter values is expected to have a size of one in this case!" );
        }

        List<RexNode> projects = new ArrayList<>();
        for ( RexNode project : ((LogicalProject) node).getProjects() ) {
            if ( project instanceof RexDynamicParam ) {
                long newIndex = parameterValues.get( 0 ).size();
                long oldIndex = ((RexDynamicParam) project).getIndex();
                AlgDataType type = statement.getDataContext().getParameterType( oldIndex );
                if ( type == null ) {
                    type = project.getType();
                }
                Object value = parameterValues.get( 0 ).get( oldIndex );
                projects.add( new RexDynamicParam( type, newIndex ) );
                parameterValues.get( 0 ).put( newIndex, value );
            }
        }

        LogicalValues logicalValues = LogicalValues.createOneRow( node.getCluster() );
        LogicalProject newProject = new LogicalProject(
                node.getCluster(),
                node.getTraitSet(),
                logicalValues,
                projects,
                node.getRowType()
        );
        return super.handleGeneric( newProject, builder );
    }

}
