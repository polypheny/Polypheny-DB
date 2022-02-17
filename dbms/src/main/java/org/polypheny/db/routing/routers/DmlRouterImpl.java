/*
 * Copyright 2019-2021 The Polypheny Project
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
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.BatchIterator;
import org.polypheny.db.algebra.core.ConditionalExecute;
import org.polypheny.db.algebra.core.ConstraintEnforcer;
import org.polypheny.db.algebra.core.TableModify;
import org.polypheny.db.algebra.core.TableModify.Operation;
import org.polypheny.db.algebra.logical.LogicalBatchIterator;
import org.polypheny.db.algebra.logical.LogicalConditionalExecute;
import org.polypheny.db.algebra.logical.LogicalConstraintEnforcer;
import org.polypheny.db.algebra.logical.LogicalDocuments;
import org.polypheny.db.algebra.logical.LogicalFilter;
import org.polypheny.db.algebra.logical.LogicalModifyCollect;
import org.polypheny.db.algebra.logical.LogicalProject;
import org.polypheny.db.algebra.logical.LogicalTableModify;
import org.polypheny.db.algebra.logical.LogicalTableScan;
import org.polypheny.db.algebra.logical.LogicalValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.partition.PartitionManager;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.prepare.AlgOptTableImpl;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.processing.WhereClauseVisitor;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.routing.DmlRouter;
import org.polypheny.db.routing.LogicalQueryInformation;
import org.polypheny.db.routing.RoutingManager;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.PolySchemaBuilder;
import org.polypheny.db.schema.Table;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.RoutedAlgBuilder;
import org.polypheny.db.transaction.Statement;

@Slf4j
public class DmlRouterImpl extends BaseRouter implements DmlRouter {

    /**
     * Default implementation: Execute DML on all placements
     *
     * @return
     */
    @Override
    public AlgNode routeDml( AlgNode node, Statement statement ) {
        AlgOptCluster cluster = node.getCluster();

        if ( node.getTable() != null ) {
            AlgOptTableImpl table = (AlgOptTableImpl) node.getTable();
            if ( table.getTable() instanceof LogicalTable ) {
                LogicalTable t = ((LogicalTable) table.getTable());
                // Get placements of this table
                CatalogTable catalogTable = catalog.getTable( t.getTableId() );

                // Make sure that this table can be modified
                if ( !catalogTable.modifiable ) {
                    if ( catalogTable.tableType == TableType.TABLE ) {
                        throw new RuntimeException( "Unable to modify a table marked as read-only!" );
                    } else if ( catalogTable.tableType == TableType.SOURCE ) {
                        throw new RuntimeException( "The table '" + catalogTable.name + "' is provided by a data source which does not support data modification." );
                    } else if ( catalogTable.tableType == TableType.VIEW ) {
                        throw new RuntimeException( "Polypheny-DB does not support modifying views." );
                    }
                    throw new RuntimeException( "Unknown table type: " + catalogTable.tableType.name() );
                }

                long pkid = catalogTable.primaryKey;
                List<Long> pkColumnIds = catalog.getPrimaryKey( pkid ).columnIds;
                CatalogColumn pkColumn = catalog.getColumn( pkColumnIds.get( 0 ) );

                // Essentially gets a list of all stores where this table resides
                List<CatalogColumnPlacement> pkPlacements = catalog.getColumnPlacement( pkColumn.id );

                if ( catalogTable.isPartitioned && log.isDebugEnabled() ) {
                    log.debug( "\nListing all relevant stores for table: '{}' and all partitions: {}", catalogTable.name, catalogTable.partitionProperty.partitionGroupIds );
                    for ( CatalogColumnPlacement dataPlacement : pkPlacements ) {
                        log.debug(
                                "\t\t -> '{}' {}\t{}",
                                dataPlacement.adapterUniqueName,
                                catalog.getPartitionGroupsOnDataPlacement( dataPlacement.adapterId, dataPlacement.tableId ),
                                catalog.getPartitionGroupsIndexOnDataPlacement( dataPlacement.adapterId, dataPlacement.tableId ) );
                    }
                }

                // Execute on all primary key placements
                List<AlgNode> modifies = new ArrayList<>();

                // Needed for partitioned updates when source partition and target partition are not equal
                // SET Value is the new partition, where clause is the source
                boolean operationWasRewritten = false;
                List<Map<Long, Object>> tempParamValues = null;

                Map<Long, Object> newParameterValues = new HashMap<>();
                for ( CatalogColumnPlacement pkPlacement : pkPlacements ) {

                    CatalogReader catalogReader = statement.getTransaction().getCatalogReader();

                    // Get placements on store
                    List<CatalogColumnPlacement> placementsOnAdapter = catalog.getColumnPlacementsOnAdapterPerTable( pkPlacement.adapterId, catalogTable.id );

                    // If this is a update, check whether we need to execute on this store at all
                    List<String> updateColumnList = ((LogicalTableModify) node).getUpdateColumnList();
                    List<RexNode> sourceExpressionList = ((LogicalTableModify) node).getSourceExpressionList();
                    if ( placementsOnAdapter.size() != catalogTable.columnIds.size() ) {

                        if ( ((LogicalTableModify) node).getOperation() == Operation.UPDATE ) {
                            updateColumnList = new LinkedList<>( ((LogicalTableModify) node).getUpdateColumnList() );
                            sourceExpressionList = new LinkedList<>( ((LogicalTableModify) node).getSourceExpressionList() );
                            Iterator<String> updateColumnListIterator = updateColumnList.iterator();
                            Iterator<RexNode> sourceExpressionListIterator = sourceExpressionList.iterator();
                            while ( updateColumnListIterator.hasNext() ) {
                                String columnName = updateColumnListIterator.next();
                                sourceExpressionListIterator.next();
                                try {
                                    CatalogColumn catalogColumn = catalog.getColumn( catalogTable.id, columnName );
                                    if ( !catalog.checkIfExistsColumnPlacement( pkPlacement.adapterId, catalogColumn.id ) ) {
                                        updateColumnListIterator.remove();
                                        sourceExpressionListIterator.remove();
                                    }
                                } catch ( UnknownColumnException e ) {
                                    throw new RuntimeException( e );
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
                    if ( catalogTable.isPartitioned ) {
                        boolean worstCaseRouting = false;
                        Set<Long> identifiedPartitionsInFilter = new HashSet<>();

                        PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
                        PartitionManager partitionManager = partitionManagerFactory.getPartitionManager( catalogTable.partitionType );

                        WhereClauseVisitor whereClauseVisitor = new WhereClauseVisitor( statement, catalogTable.columnIds.indexOf( catalogTable.partitionColumnId ) );
                        node.accept( new AlgShuttleImpl() {
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

                        if ( ((LogicalTableModify) node).getOperation() == Operation.UPDATE ) {
                            // In case of update always use worst case routing for now.
                            // Since you have to identify the current partition to delete the entry and then create a new entry on the correct partitions
                            int index = 0;

                            for ( String cn : updateColumnList ) {
                                try {
                                    if ( catalog.getColumn( catalogTable.id, cn ).id == catalogTable.partitionColumnId ) {
                                        if ( log.isDebugEnabled() ) {
                                            log.debug( " UPDATE: Found PartitionColumnID Match: '{}' at index: {}", catalogTable.partitionColumnId, index );
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
                                } catch ( UnknownColumnException e ) {
                                    throw new RuntimeException( e );
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

                                        /* TODO add possibility to substitute the update as a insert into target partition from all source partitions
                                        // IS currently blocked
                                        // needs to to a insert into target partitions select from all other partitions first and then delete on source partitions
                                        worstCaseRouting = false;
                                        log.debug( "oldValue and new value reside on same partition: " + identifiedPartitionForSetValue );

                                        //Substitute UPDATE operation with DELETE on all partitionIds of WHERE Clause
                                        for ( long currentPart : identifiedPartitionsInFilter ) {

                                            if ( !catalog.getPartitionsOnDataPlacement( pkPlacement.adapterId, catalogTable.id ).contains( currentPart ) ) {
                                                continue;
                                            }

                                            List<String> qualifiedTableName = ImmutableList.of(
                                                    PolySchemaBuilder.buildAdapterSchemaName(
                                                            pkPlacement.adapterUniqueName,
                                                            catalogTable.getSchemaName(),
                                                            pkPlacement.physicalSchemaName ),
                                                    t.getLogicalTableName() + "_" + partitionId) );
                                            RelOptTable physical = catalogReader.getTableForMember( qualifiedTableName );
                                            ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );

                                            {@link AlgNode} input = buildDml(
                                                    recursiveCopy( node.getInput( 0 ) ),
                                                    AlgBuilder.create( statement, cluster ),
                                                    catalogTable,
                                                    placementsOnAdapter,
                                                    catalog.getPartitionPlacement( pkPlacement.adapterId, currentPart ),
                                                    statement,
                                                    cluster ).build();

                                            TableModify deleteModify = LogicalTableModify.create(
                                                    physical,
                                                    catalogReader,
                                                    input,
                                                    Operation.DELETE,
                                                    null,
                                                    null,
                                                    ((LogicalTableModify) node).isFlattened() );

                                            modifies.add( deleteModify );


                                        }

                                                //Inject INSERT statement for identified SET partitionId
                                                //Otherwise data migrator would be needed
                                        if ( catalog.getPartitionsOnDataPlacement( pkPlacement.adapterId, catalogTable.id ).contains( identifiedPartitionForSetValue ) ) {

                                           /* List<String> qualifiedTableName = ImmutableList.of(
                                                    PolySchemaBuilder.buildAdapterSchemaName(
                                                            pkPlacement.adapterUniqueName,
                                                            catalogTable.getSchemaName(),
                                                            pkPlacement.physicalSchemaName,
                                                            identifiedPartitionForSetValue ),
                                                    t.getLogicalTableName() );
                                            RelOptTable physical = catalogReader.getTableForMember( qualifiedTableName );
                                            ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );

                                            {@link AlgNode} input = buildDml(
                                                    recursiveCopy( node.getInput( 0 ) ),
                                                    AlgBuilder.create( statement, cluster ),
                                                    catalogTable,
                                                    placementsOnAdapter,
                                                    catalog.getPartitionPlacement( pkPlacement.adapterId, identifiedPartitionForSetValue ),
                                                    statement,
                                                    cluster ).build();

                                            TableModify insertModify = modifiableTable.toModificationRel(
                                                    cluster,
                                                    physical,
                                                    catalogReader,
                                                    input,
                                                    Operation.INSERT,
                                                    null,
                                                    null,
                                                    ((LogicalTableModify) node).isFlattened()
                                            );

                                            modifies.add( insertModify );
                                        }
                                                //operationWasRewritten = true;

                                         */
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

                                // Data Migrate copy of all other partitions beside the identifed on towards the identified one
                                // Then inject a DELETE statement for all those partitions

                                // Do the update only on the identified partition

                            }// If nothing has been specified
                            //Partition functionality cannot be used --> worstCase --> send query to every partition
                            else {
                                worstCaseRouting = true;
                                accessedPartitionList = new HashSet<>( catalogTable.partitionProperty.partitionIds );
                            }

                        } else if ( ((LogicalTableModify) node).getOperation() == Operation.INSERT ) {
                            int i;

                            if ( ((LogicalTableModify) node).getInput() instanceof LogicalValues ) {

                                // Get fieldList and map columns to index since they could be in arbitrary order
                                int partitionColumnIndex = -1;
                                Map<Long, Integer> resultColMapping = new HashMap<>();
                                for ( int j = 0; j < (((LogicalTableModify) node).getInput()).getRowType().getFieldList().size(); j++ ) {
                                    String columnFieldName = (((LogicalTableModify) node).getInput()).getRowType().getFieldList().get( j ).getKey();

                                    // Retrieve columnId of fieldName and map it to its fieldList location of INSERT Stmt
                                    int columnIndex = catalogTable.getColumnNames().indexOf( columnFieldName );
                                    resultColMapping.put( catalogTable.columnIds.get( columnIndex ), j );

                                    // Determine location of partitionColumn in fieldList
                                    if ( catalogTable.columnIds.get( columnIndex ) == catalogTable.partitionColumnId ) {
                                        partitionColumnIndex = columnIndex;
                                        if ( log.isDebugEnabled() ) {
                                            log.debug( "INSERT: Found PartitionColumnID: '{}' at column index: {}", catalogTable.partitionColumnId, j );
                                            worstCaseRouting = false;

                                        }
                                    }
                                }

                                // Will executed all required tuples that belong on the same partition jointly
                                Map<Long, List<ImmutableList<RexLiteral>>> tuplesOnPartition = new HashMap<>();
                                for ( ImmutableList<RexLiteral> currentTuple : ((LogicalValues) ((LogicalTableModify) node).getInput()).tuples ) {

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

                                    if ( !catalog.getPartitionsOnDataPlacement( pkPlacement.adapterId, catalogTable.id ).contains( currentPartitionId ) ) {
                                        continue;
                                    }

                                    for ( ImmutableList<RexLiteral> row : partitionMapping.getValue() ) {
                                        LogicalValues newLogicalValues = new LogicalValues(
                                                cluster,
                                                cluster.traitSet(),
                                                (((LogicalTableModify) node).getInput()).getRowType(),
                                                ImmutableList.copyOf( ImmutableList.of( row ) ) );

                                        AlgNode input = buildDml(
                                                newLogicalValues,
                                                RoutedAlgBuilder.create( statement, cluster ),
                                                catalogTable,
                                                placementsOnAdapter,
                                                catalog.getPartitionPlacement( pkPlacement.adapterId, currentPartitionId ),
                                                statement,
                                                cluster,
                                                true,
                                                statement.getDataContext().getParameterValues() ).build();

                                        List<String> qualifiedTableName = ImmutableList.of(
                                                PolySchemaBuilder.buildAdapterSchemaName(
                                                        pkPlacement.adapterUniqueName,
                                                        catalogTable.getSchemaName(),
                                                        pkPlacement.physicalSchemaName
                                                ),
                                                t.getLogicalTableName() + "_" + currentPartitionId );
                                        AlgOptTable physical = catalogReader.getTableForMember( qualifiedTableName );
                                        ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );

                                        // Build DML
                                        TableModify modify;

                                        modify = modifiableTable.toModificationAlg(
                                                cluster,
                                                physical,
                                                catalogReader,
                                                input,
                                                ((LogicalTableModify) node).getOperation(),
                                                updateColumnList,
                                                sourceExpressionList,
                                                ((LogicalTableModify) node).isFlattened() );

                                        modifies.add( modify );

                                    }
                                }
                                operationWasRewritten = true;

                            } else if ( ((LogicalTableModify) node).getInput() instanceof LogicalProject
                                    && ((LogicalProject) ((LogicalTableModify) node).getInput()).getInput() instanceof LogicalValues ) {

                                String partitionColumnName = catalog.getColumn( catalogTable.partitionColumnId ).name;
                                List<String> fieldNames = ((LogicalTableModify) node).getInput().getRowType().getFieldNames();

                                LogicalTableModify ltm = ((LogicalTableModify) node);
                                LogicalProject lproject = (LogicalProject) ltm.getInput();

                                List<RexNode> fieldValues = lproject.getProjects();

                                for ( i = 0; i < fieldNames.size(); i++ ) {
                                    String columnName = fieldNames.get( i );

                                    if ( partitionColumnName.equals( columnName ) ) {

                                        if ( ((LogicalTableModify) node).getInput().getChildExps().get( i ).getKind().equals( Kind.DYNAMIC_PARAM ) ) {

                                            // Needed to identify the column which contains the partition value
                                            long partitionValueIndex = ((RexDynamicParam) fieldValues.get( i )).getIndex();

                                            long tempPartitionId = 0;
                                            // Get partitionValue per row/tuple to be inserted
                                            // Create as many independent TableModifies as there are entries in getParameterValues

                                            for ( Map<Long, Object> currentRow : statement.getDataContext().getParameterValues() ) {

                                                tempPartitionId = partitionManager.getTargetPartitionId( catalogTable, currentRow.get( partitionValueIndex ).toString() );

                                                if ( !catalog.getPartitionsOnDataPlacement( pkPlacement.adapterId, catalogTable.id ).contains( tempPartitionId ) ) {
                                                    continue;
                                                }

                                                List<Map<Long, Object>> parameterValues = new ArrayList<>();
                                                parameterValues.add( new HashMap<>( newParameterValues ) );
                                                parameterValues.get( 0 ).putAll( currentRow );

                                                AlgNode input = buildDml(
                                                        super.recursiveCopy( node.getInput( 0 ) ),
                                                        RoutedAlgBuilder.create( statement, cluster ),
                                                        catalogTable,
                                                        placementsOnAdapter,
                                                        catalog.getPartitionPlacement( pkPlacement.adapterId, tempPartitionId ),
                                                        statement,
                                                        cluster,
                                                        true,
                                                        parameterValues ).build();

                                                newParameterValues.putAll( parameterValues.get( 0 ) );

                                                List<String> qualifiedTableName = ImmutableList.of(
                                                        PolySchemaBuilder.buildAdapterSchemaName(
                                                                pkPlacement.adapterUniqueName,
                                                                catalogTable.getSchemaName(),
                                                                pkPlacement.physicalSchemaName
                                                        ),
                                                        t.getLogicalTableName() + "_" + tempPartitionId );
                                                AlgOptTable physical = catalogReader.getTableForMember( qualifiedTableName );
                                                ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );

                                                // Build DML
                                                TableModify modify;

                                                modify = modifiableTable.toModificationAlg(
                                                        cluster,
                                                        physical,
                                                        catalogReader,
                                                        input,
                                                        ((LogicalTableModify) node).getOperation(),
                                                        updateColumnList,
                                                        sourceExpressionList,
                                                        ((LogicalTableModify) node).isFlattened() );

                                                modifies.add( modify );
                                            }

                                            operationWasRewritten = true;
                                            worstCaseRouting = false;
                                        } else {
                                            partitionValue = ((LogicalTableModify) node).getInput().getChildExps().get( i ).toString().replace( "'", "" );
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
                                String partitionColumnName = catalog.getColumn( catalogTable.partitionColumnId ).name;
                                String partitionName = catalog.getPartitionGroup( identPart ).partitionGroupName;
                                log.debug( "INSERT: partitionColumn-value: '{}' should be put on partition: {} ({}), which is partitioned with column {}",
                                        partitionValue, identPart, partitionName, partitionColumnName );
                            }


                        } else if ( ((LogicalTableModify) node).getOperation() == Operation.DELETE ) {
                            if ( whereClauseValues == null ) {
                                worstCaseRouting = true;
                            } else {
                                if ( whereClauseValues.size() >= 4 ) {
                                    worstCaseRouting = true;
                                } else {
                                    worstCaseRouting = false;
                                }
                            }

                        }

                        if ( worstCaseRouting ) {
                            log.debug( "PartitionColumnID was not an explicit part of statement, partition routing will therefore assume worst-case: Routing to ALL PARTITIONS" );
                            accessedPartitionList = catalogTable.partitionProperty.partitionIds.stream().collect( Collectors.toSet() );
                        }
                    } else {
                        // unpartitioned tables only have one partition anyway
                        identPart = catalogTable.partitionProperty.partitionIds.get( 0 );
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

                            if ( !catalog.getPartitionsOnDataPlacement( pkPlacement.adapterId, catalogTable.id ).contains( partitionId ) ) {
                                continue;
                            }

                            List<String> qualifiedTableName = ImmutableList.of(
                                    PolySchemaBuilder.buildAdapterSchemaName(
                                            pkPlacement.adapterUniqueName,
                                            catalogTable.getSchemaName(),
                                            pkPlacement.physicalSchemaName
                                    ),
                                    t.getLogicalTableName() + "_" + partitionId );
                            AlgOptTable physical = catalogReader.getTableForMember( qualifiedTableName );
                            ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );

                            // Build DML
                            TableModify modify;
                            AlgNode input = buildDml(
                                    super.recursiveCopy( node.getInput( 0 ) ),
                                    RoutedAlgBuilder.create( statement, cluster ),
                                    catalogTable,
                                    placementsOnAdapter,
                                    catalog.getPartitionPlacement( pkPlacement.adapterId, partitionId ),
                                    statement,
                                    cluster,
                                    false,
                                    statement.getDataContext().getParameterValues() ).build();
                            if ( modifiableTable != null && modifiableTable == physical.unwrap( Table.class ) ) {
                                modify = modifiableTable.toModificationAlg(
                                        cluster,
                                        physical,
                                        catalogReader,
                                        input,
                                        ((LogicalTableModify) node).getOperation(),
                                        updateColumnList,
                                        sourceExpressionList,
                                        ((LogicalTableModify) node).isFlattened()
                                );
                            } else {
                                modify = LogicalTableModify.create(
                                        physical,
                                        catalogReader,
                                        input,
                                        ((LogicalTableModify) node).getOperation(),
                                        updateColumnList,
                                        sourceExpressionList,
                                        ((LogicalTableModify) node).isFlattened()
                                );
                            }
                            modifies.add( modify );
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
                    RoutedAlgBuilder builder = RoutedAlgBuilder.create( statement, cluster );
                    for ( int i = 0; i < modifies.size(); i++ ) {
                        if ( i == 0 ) {
                            builder.push( modifies.get( i ) );
                        } else {
                            builder.push( modifies.get( i ) );
                            builder.replaceTop( LogicalModifyCollect.create(
                                    ImmutableList.of( builder.peek( 1 ), builder.peek( 0 ) ),
                                    true ) );
                        }
                    }
                    return builder.build();
                }
            } else {
                throw new RuntimeException( "Unexpected table. Only logical tables expected here!" );
            }
        }
        throw new RuntimeException( "Unexpected operator!" );
    }


    @Override
    public AlgNode handleConditionalExecute( AlgNode node, Statement statement, LogicalQueryInformation queryInformation ) {
        LogicalConditionalExecute lce = (LogicalConditionalExecute) node;
        RoutedAlgBuilder builder = RoutedAlgBuilder.create( statement, node.getCluster() );
        builder = RoutingManager.getInstance().getFallbackRouter().routeFirst( lce.getLeft(), builder, statement, node.getCluster(), queryInformation );
        AlgNode action;
        if ( lce.getRight() instanceof LogicalConditionalExecute ) {
            action = handleConditionalExecute( lce.getRight(), statement, queryInformation );
        } else if ( lce.getRight() instanceof TableModify ) {
            action = routeDml( lce.getRight(), statement );
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

        if ( constraint.getLeft() instanceof TableModify ) {
            return LogicalConstraintEnforcer.create(
                    routeDml( constraint.getLeft(), statement ),
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
        if ( iterator.getInput() instanceof TableModify ) {
            input = routeDml( iterator.getInput(), statement );
        } else if ( iterator.getInput() instanceof ConditionalExecute ) {
            input = handleConditionalExecute( iterator.getInput(), statement, queryInformation );
        } else if ( iterator.getInput() instanceof ConstraintEnforcer ) {
            input = handleConstraintEnforcer( iterator.getInput(), statement, queryInformation );
        } else {
            throw new RuntimeException( "BachIterator had an unknown child!" );
        }

        return LogicalBatchIterator.create( input, statement );
    }


    private AlgBuilder buildDml(
            AlgNode node,
            RoutedAlgBuilder builder,
            CatalogTable catalogTable,
            List<CatalogColumnPlacement> placements,
            CatalogPartitionPlacement partitionPlacement,
            Statement statement,
            AlgOptCluster cluster,
            boolean remapParameterValues,
            List<Map<Long, Object>> parameterValues ) {
        for ( int i = 0; i < node.getInputs().size(); i++ ) {
            buildDml( node.getInput( i ), builder, catalogTable, placements, partitionPlacement, statement, cluster, remapParameterValues, parameterValues );
        }

        if ( log.isDebugEnabled() ) {
            log.debug( "List of Store specific ColumnPlacements: " );
            for ( CatalogColumnPlacement ccp : placements ) {
                log.debug( "{}.{}", ccp.adapterUniqueName, ccp.getLogicalColumnName() );
            }
        }

        if ( node instanceof LogicalTableScan && node.getTable() != null ) {
            AlgOptTableImpl table = (AlgOptTableImpl) node.getTable();

            if ( table.getTable() instanceof LogicalTable ) {
                // Special handling for INSERT INTO foo SELECT * FROM foo2
                if ( ((LogicalTable) table.getTable()).getTableId() != catalogTable.id ) {
                    // TODO: how build select from here?
                    // return buildSelect( node, builder, statement, cluster );
                }

                builder = super.handleTableScan(
                        builder,
                        placements.get( 0 ).tableId,
                        placements.get( 0 ).adapterUniqueName,
                        catalogTable.getSchemaName(),
                        catalogTable.name,
                        placements.get( 0 ).physicalSchemaName,
                        partitionPlacement.physicalTableName,
                        partitionPlacement.partitionId );

                return builder;

            } else {
                throw new RuntimeException( "Unexpected table. Only logical tables expected here!" );
            }
        } else if ( node instanceof LogicalValues ) {
            if ( node.getModel() == SchemaType.DOCUMENT ) {
                return handleDocuments( (LogicalDocuments) node, builder );
            }
            builder = super.handleValues( (LogicalValues) node, builder );
            if ( catalogTable.columnIds.size() == placements.size() ) { // full placement, no additional checks required
                return builder;
            } else if ( node.getRowType().toString().equals( "RecordType(INTEGER ZERO)" ) ) {
                // This is a prepared statement. Actual values are in the project. Do nothing
                return builder;
            } else { // partitioned, add additional project
                ArrayList<RexNode> rexNodes = new ArrayList<>();
                for ( CatalogColumnPlacement ccp : placements ) {
                    rexNodes.add( builder.field( ccp.getLogicalColumnName() ) );
                }
                return builder.project( rexNodes );
            }
        } else if ( node instanceof LogicalProject ) {
            if ( catalogTable.columnIds.size() == placements.size() ) { // full placement, generic handling is sufficient
                if ( catalogTable.isPartitioned && remapParameterValues ) {  //  && ((LogicalProject) node).getInput().getRowType().toString().equals( "RecordType(INTEGER ZERO)" )
                    return remapParameterizedDml( node, builder, statement, parameterValues );
                } else {
                    return super.handleGeneric( node, builder );
                }
            } else { // vertically partitioned, adjust project
                if ( ((LogicalProject) node).getInput().getRowType().toString().equals( "RecordType(INTEGER ZERO)" ) ) {
                    if ( catalogTable.isPartitioned && remapParameterValues ) {
                        builder = remapParameterizedDml( node, builder, statement, parameterValues );
                    }
                    builder.push( node.copy( node.getTraitSet(), ImmutableList.of( builder.peek( 0 ) ) ) );
                    ArrayList<RexNode> rexNodes = new ArrayList<>();
                    for ( CatalogColumnPlacement ccp : placements ) {
                        rexNodes.add( builder.field( ccp.getLogicalColumnName() ) );
                    }
                    return builder.project( rexNodes );
                } else {
                    ArrayList<RexNode> rexNodes = new ArrayList<>();
                    for ( CatalogColumnPlacement ccp : placements ) {
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
            if ( catalogTable.columnIds.size() != placements.size() ) { // partitioned, check if there is a illegal condition
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


    private void dmlConditionCheck( LogicalFilter node, CatalogTable catalogTable, List<CatalogColumnPlacement> placements, RexNode operand ) {
        if ( operand instanceof RexInputRef ) {
            int index = ((RexInputRef) operand).getIndex();
            AlgDataTypeField field = node.getInput().getRowType().getFieldList().get( index );
            CatalogColumn column;
            try {
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
                    if ( !catalogTable.getSchemaName().equalsIgnoreCase( columnNames[0] ) ) {
                        throw new RuntimeException( "Schema name does not match expected schema name: " + field.getName() );
                    }
                    if ( !catalogTable.name.equalsIgnoreCase( columnNames[1] ) ) {
                        throw new RuntimeException( "Table name does not match expected table name: " + field.getName() );
                    }
                    columnName = columnNames[2];
                } else {
                    throw new RuntimeException( "Invalid column name: " + field.getName() );
                }
                column = catalog.getColumn( catalogTable.id, columnName );
            } catch ( UnknownColumnException e ) {
                throw new RuntimeException( e );
            }
            if ( !catalog.checkIfExistsColumnPlacement( placements.get( 0 ).adapterId, column.id ) ) {
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
        if ( parameterValues.size() != 1 ) {
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
