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
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.partition.PartitionManager;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.prepare.RelOptTableImpl;
import org.polypheny.db.processing.WhereClauseVisitor;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelShuttleImpl;
import org.polypheny.db.rel.core.TableModify;
import org.polypheny.db.rel.core.TableModify.Operation;
import org.polypheny.db.rel.logical.LogicalConditionalExecute;
import org.polypheny.db.rel.logical.LogicalFilter;
import org.polypheny.db.rel.logical.LogicalModifyCollect;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rel.logical.LogicalTableModify;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.logical.LogicalValues;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.routing.DmlRouter;
import org.polypheny.db.routing.LogicalQueryInformation;
import org.polypheny.db.routing.Router;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.PolySchemaBuilder;
import org.polypheny.db.schema.Table;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.tools.RoutedRelBuilder;
import org.polypheny.db.transaction.Statement;

@Slf4j
public class DmlRouterImpl extends BaseRouter implements DmlRouter {

    // Default implementation: Execute DML on all placements
    @Override
    public RoutedRelBuilder routeDml( RelNode node, Statement statement ) {
        RelOptCluster cluster = node.getCluster();

        if ( node.getTable() != null ) {
            RelOptTableImpl table = (RelOptTableImpl) node.getTable();
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

                //Essentially gets a list of all stores where this table resides
                List<CatalogColumnPlacement> pkPlacements = catalog.getColumnPlacement( pkColumn.id );

                if ( catalogTable.isPartitioned && log.isDebugEnabled() ) {
                    log.debug( "\nListing all relevant stores for table: '{}' and all partitions: {}", catalogTable.name, catalogTable.partitionProperty.partitionGroupIds );
                    for ( CatalogColumnPlacement dataPlacement : pkPlacements ) {
                        log.debug( "\t\t -> '{}' {}\t{}",
                                dataPlacement.adapterUniqueName,
                                catalog.getPartitionGroupsOnDataPlacement( dataPlacement.adapterId, dataPlacement.tableId ),
                                catalog.getPartitionGroupsIndexOnDataPlacement( dataPlacement.adapterId, dataPlacement.tableId ) );
                    }
                }

                // Execute on all primary key placements
                List<TableModify> modifies = new ArrayList<>();

                //Needed for partitioned updates when source partition and target partition are not equal
                //SET Value is the new partition, where clause is the source
                boolean operationWasRewritten = false;
                List<Map<Long, Object>> tempParamValues = null;

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
                        //partitionManager.validatePartitionGroupDistribution( catalogTable );

                        WhereClauseVisitor whereClauseVisitor = new WhereClauseVisitor( statement, catalogTable.columnIds.indexOf( catalogTable.partitionColumnId ) );
                        node.accept( new RelShuttleImpl() {
                            @Override
                            public RelNode visit( LogicalFilter filter ) {
                                super.visit( filter );
                                filter.accept( whereClauseVisitor );
                                return filter;
                            }
                        } );

                        List<String> whereClauseValues = null;
                        if ( !whereClauseVisitor.getValues().isEmpty() ) {
                            // if ( whereClauseVisitor.getValues().size() == 1 ) {
                            whereClauseValues = whereClauseVisitor.getValues().stream()
                                    .map( Object::toString )
                                    .collect( Collectors.toList() );
                            log.debug( "Found Where Clause Values: {}", whereClauseValues );
                            worstCaseRouting = true;
                            // }
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
                        //set true if partitionColumn is part of UPDATE Statement, else assume worst case routing
                        boolean partitionColumnIdentified = false;

                        if ( ((LogicalTableModify) node).getOperation() == Operation.UPDATE ) {
                            // In case of update always use worst case routing for now.
                            // Since you have to identify the current partition to delete the entry and then create a new entry on the correct partitions
                            int index = 0;

                            for ( String cn : updateColumnList ) {
                                try {
                                    if ( catalog.getColumn( catalogTable.id, cn ).id == catalogTable.partitionColumnId ) {
                                        log.debug( " UPDATE: Found PartitionColumnID Match: '{}' at index: {}", catalogTable.partitionColumnId, index );

                                        //Routing/Locking can now be executed on certain partitions
                                        partitionColumnIdentified = true;
                                        partitionValue = sourceExpressionList.get( index ).toString().replace( "'", "" );
                                        if ( log.isDebugEnabled() ) {
                                            log.debug( "UPDATE: partitionColumn-value: '{}' should be put on partition: {}",
                                                    partitionValue,
                                                    partitionManager.getTargetPartitionId( catalogTable, partitionValue ) );
                                        }
                                        identPart = (int) partitionManager.getTargetPartitionId( catalogTable, partitionValue );
                                        //needed to verify if UPDATE shall be executed on two partitions or not
                                        identifiedPartitionForSetValue = identPart;
                                        accessedPartitionList.add( identPart );
                                        break;
                                    }
                                } catch ( UnknownColumnException e ) {
                                    throw new RuntimeException( e );
                                }
                                index++;
                            }

                            //If WHERE clause has any value for partition column
                            if ( identifiedPartitionsInFilter.size() > 0 ) {

                                //Partition has been identified in SET
                                if ( identifiedPartitionForSetValue != -1 ) {

                                    //SET value and single WHERE clause point to same partition.
                                    //Inplace update possible
                                    if ( identifiedPartitionsInFilter.size() == 1 && identifiedPartitionsInFilter.contains( identifiedPartitionForSetValue ) ) {
                                        log.debug( "oldValue and new value reside on same partition: " + identifiedPartitionForSetValue );
                                        worstCaseRouting = false;
                                    } else {
                                        throw new RuntimeException( "Updating partition key is not allowed" );

                                        /* TODO add possibility to substitute the update as a insert into target partitoin from all source parttions
                                        // IS currently blocked
                                        //needs to to a insert into target partition select from all other partitoins first and then delte on source partiitons
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

                                            RelNode input = buildDml(
                                                    recursiveCopy( node.getInput( 0 ) ),
                                                    RoutedRelBuilder.create( statement, cluster ),
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

                                            RelNode input = buildDml(
                                                    recursiveCopy( node.getInput( 0 ) ),
                                                    RoutedRelBuilder.create( statement, cluster ),
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
                                }//WHERE clause only
                                else {
                                    throw new RuntimeException( "Updating partition key is not allowed" );

                                    //Simply execute the UPDATE on all identified partitions
                                    //Nothing to do
                                    //worstCaseRouting = false;
                                }
                            }// If only SET is specified
                            //changes the value of partition column of complete table to only reside on one partition
                            else if ( identifiedPartitionForSetValue != -1 ) {

                                //Data Migrate copy of all other partitions beside the identifed on towards the identified one
                                //Then inject a DELETE statement for all those partitions

                                //Do the update only on the identified partition

                            }// If nothing has been specified
                            //Partition functionality cannot be used --> worstCase --> send query to every partition
                            else {
                                worstCaseRouting = true;
                                accessedPartitionList = catalogTable.partitionProperty.partitionIds.stream().collect( Collectors.toSet() );
                            }

                        } else if ( ((LogicalTableModify) node).getOperation() == Operation.INSERT ) {
                            int i;

                            if ( ((LogicalTableModify) node).getInput() instanceof LogicalValues ) {

                                for ( ImmutableList<RexLiteral> currentTuple : ((LogicalValues) ((LogicalTableModify) node).getInput()).tuples ) {

                                    for ( i = 0; i < catalogTable.columnIds.size(); i++ ) {
                                        if ( catalogTable.columnIds.get( i ) == catalogTable.partitionColumnId ) {
                                            log.debug( "INSERT: Found PartitionColumnID: '{}' at column index: {}", catalogTable.partitionColumnId, i );
                                            worstCaseRouting = false;
                                            partitionValue = currentTuple.get( i ).toString().replace( "'", "" );
                                            identPart = (int) partitionManager.getTargetPartitionId( catalogTable, partitionValue );
                                            accessedPartitionList.add( identPart );
                                            break;
                                        }
                                    }
                                }
                            } else if ( ((LogicalTableModify) node).getInput() instanceof LogicalProject
                                    && ((LogicalProject) ((LogicalTableModify) node).getInput()).getInput() instanceof LogicalValues ) {

                                String partitionColumnName = catalog.getColumn( catalogTable.partitionColumnId ).name;
                                List<String> fieldNames = ((LogicalTableModify) node).getInput().getRowType().getFieldNames();

                                LogicalTableModify ltm = ((LogicalTableModify) node);
                                LogicalProject lproject = (LogicalProject) ltm.getInput();

                                List<RexNode> fieldValues = lproject.getProjects();
                                Map<Long, RexDynamicParam> indexRemap = new HashMap<>();

                                //Retrieve RexDynamicParams and their param index position
                                for ( int j = 0; j < fieldNames.size(); j++ ) {
                                    if ( fieldValues.get( j ) instanceof RexDynamicParam ) {
                                        long valueIndex = ((RexDynamicParam) fieldValues.get( j )).getIndex();
                                        RelDataType type = fieldValues.get( j ).getType();

                                        indexRemap.put( valueIndex, (RexDynamicParam) fieldValues.get( j ) );
                                    }
                                }

                                for ( i = 0; i < fieldNames.size(); i++ ) {
                                    String columnName = fieldNames.get( i );

                                    if ( partitionColumnName.equals( columnName ) ) {

                                        if ( ((LogicalTableModify) node).getInput().getChildExps().get( i ).getKind().equals( SqlKind.DYNAMIC_PARAM ) ) {

                                            //Needed to identify the column which contains the partition value
                                            long partitionValueIndex = ((RexDynamicParam) fieldValues.get( i )).getIndex();

                                            if ( tempParamValues == null ) {
                                                statement.getDataContext().backupParameterValues();
                                                tempParamValues = statement.getDataContext().getParameterValues().stream().collect( Collectors.toList() );
                                            }
                                            statement.getDataContext().resetParameterValues();
                                            long tempPartitionId = 0;
                                            //Get partitionValue per row/tuple to be inserted
                                            //Create as many independent TableModifies as there are entries in getParameterValues

                                            for ( Map<Long, Object> currentRow : tempParamValues ) {

                                                tempPartitionId = partitionManager.getTargetPartitionId( catalogTable, currentRow.get( partitionValueIndex ).toString() );

                                                if ( !catalog.getPartitionsOnDataPlacement( pkPlacement.adapterId, catalogTable.id ).contains( tempPartitionId ) ) {
                                                    continue;
                                                }
                                                statement.getDataContext().resetParameterValues();
                                                for ( Entry<Long, RexDynamicParam> param : indexRemap.entrySet() ) {

                                                    List<Object> singleDataObject = new ArrayList<>();

                                                    long paramIndexPos = param.getKey();
                                                    RelDataType paramType = param.getValue().getType();

                                                    singleDataObject.add( currentRow.get( paramIndexPos ) );

                                                    statement.getDataContext().addParameterValues( paramIndexPos, paramType, singleDataObject );

                                                }

                                                RelNode input = buildDml(
                                                        super.recursiveCopy( node.getInput( 0 ) ),
                                                        RoutedRelBuilder.create( statement, cluster ),
                                                        catalogTable,
                                                        placementsOnAdapter,
                                                        catalog.getPartitionPlacement( pkPlacement.adapterId, tempPartitionId ),
                                                        statement,
                                                        cluster ).build();

                                                List<String> qualifiedTableName = ImmutableList.of(
                                                        PolySchemaBuilder.buildAdapterSchemaName(
                                                                pkPlacement.adapterUniqueName,
                                                                catalogTable.getSchemaName(),
                                                                pkPlacement.physicalSchemaName),
                                                        t.getLogicalTableName()  + "_" + tempPartitionId );
                                                RelOptTable physical = catalogReader.getTableForMember( qualifiedTableName );
                                                ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );

                                                // Build DML
                                                TableModify modify;

                                                modify = modifiableTable.toModificationRel(
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

                                            partitionColumnIdentified = true;
                                            operationWasRewritten = true;
                                            worstCaseRouting = false;
                                        } else {
                                            partitionColumnIdentified = true;
                                            partitionValue = ((LogicalTableModify) node).getInput().getChildExps().get( i ).toString().replace( "'", "" );
                                            identPart = (int) partitionManager.getTargetPartitionId( catalogTable, partitionValue );
                                            accessedPartitionList.add( identPart );
                                            worstCaseRouting = false;
                                        }
                                        break;
                                    } else {
                                        //when loop is finished
                                        if ( i == fieldNames.size() - 1 && !partitionColumnIdentified ) {

                                            worstCaseRouting = true;
                                            //Because partitionColumn has not been specified in insert
                                        }
                                    }
                                }
                            } else {
                                worstCaseRouting = true;
                            }

                            if ( log.isDebugEnabled() ) {
                                String partitionColumnName = catalog.getColumn( catalogTable.partitionColumnId ).name;
                                String partitionName = catalog.getPartitionGroup( identPart ).partitionGroupName;
                                log.debug( "INSERT: partitionColumn-value: '{}' should be put on partition: {} ({}), which is partitioned with column",
                                        partitionValue, identPart, partitionName, partitionColumnName );
                            }


                        } else if ( ((LogicalTableModify) node).getOperation() == Operation.DELETE ) {
                            if ( whereClauseValues == null ) {
                                worstCaseRouting = true;
                            } else {
                                if ( whereClauseValues.size() >= 4 ) {
                                    worstCaseRouting = true;
                                    partitionColumnIdentified = false;
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
                        //unpartitioned tables only have one partition anyway
                        identPart = catalogTable.partitionProperty.partitionIds.get( 0 );
                        accessedPartitionList.add( identPart );

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
                                            pkPlacement.physicalSchemaName),
                                    t.getLogicalTableName() + "_" + partitionId );
                            RelOptTable physical = catalogReader.getTableForMember( qualifiedTableName );
                            ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );

                            // Build DML
                            TableModify modify;
                            RelNode input = buildDml(
                                    super.recursiveCopy( node.getInput( 0 ) ),
                                    RoutedRelBuilder.create( statement, cluster ),
                                    catalogTable,
                                    placementsOnAdapter,
                                    catalog.getPartitionPlacement( pkPlacement.adapterId, partitionId ),
                                    statement,
                                    cluster ).build();
                            if ( modifiableTable != null && modifiableTable == physical.unwrap( Table.class ) ) {
                                modify = modifiableTable.toModificationRel(
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

                if ( statement.getDataContext().wasBackuped() ) {
                    statement.getDataContext().restoreParameterValues();
                }

                /*if ( modifies.size() == 1 ) {
                    return modifies.get( 0 );
                } else {*/
                RoutedRelBuilder builder = RoutedRelBuilder.create( statement, cluster );
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
                return builder;
                //}
            } else {
                throw new RuntimeException( "Unexpected table. Only logical tables expected here!" );
            }
        }
        throw new RuntimeException( "Unexpected operator!" );
    }


    @Override
    public RelNode handleConditionalExecute( RelNode node, Statement statement, Router router, LogicalQueryInformation queryInformation ) {
        LogicalConditionalExecute lce = (LogicalConditionalExecute) node;
        RoutedRelBuilder builder = RoutedRelBuilder.create( statement, node.getCluster() );
        builder = router.buildSelect( lce.getLeft(), builder, statement, node.getCluster(), queryInformation );
        RelNode action;
        if ( lce.getRight() instanceof LogicalConditionalExecute ) {
            action = handleConditionalExecute( lce.getRight(), statement, router, queryInformation );
        } else if ( lce.getRight() instanceof TableModify ) {
            action = routeDml( lce.getRight(), statement ).build();
        } else {
            throw new IllegalArgumentException();
        }

        return LogicalConditionalExecute.create( builder.build(), action, lce );
    }


    private RelBuilder buildDml( RelNode node, RoutedRelBuilder builder, CatalogTable catalogTable, List<CatalogColumnPlacement> placements, CatalogPartitionPlacement partitionPlacement, Statement statement, RelOptCluster cluster ) {
        for ( int i = 0; i < node.getInputs().size(); i++ ) {
            buildDml( node.getInput( i ), builder, catalogTable, placements, partitionPlacement, statement, cluster );
        }

        if ( log.isDebugEnabled() ) {
            log.debug( "List of Store specific ColumnPlacements: " );
            for ( CatalogColumnPlacement ccp : placements ) {
                log.debug( "{}.{}", ccp.adapterUniqueName, ccp.getLogicalColumnName() );
            }
        }

        if ( node instanceof LogicalTableScan && node.getTable() != null ) {
            RelOptTableImpl table = (RelOptTableImpl) node.getTable();

            if ( table.getTable() instanceof LogicalTable ) {
                // Special handling for INSERT INTO foo SELECT * FROM foo2
                if ( ((LogicalTable) table.getTable()).getTableId() != catalogTable.id ) {
                    // TODO: how build select from here?
                    // return buildSelect( node, builder, statement, cluster );
                }

                builder = super.handleTableScan(
                        builder,
                        placements.get( 0 ).adapterUniqueName,
                        catalogTable.getSchemaName(),
                        catalogTable.name,
                        placements.get( 0 ).physicalSchemaName,
                        partitionPlacement.partitionId );

                return builder;

            } else {
                throw new RuntimeException( "Unexpected table. Only logical tables expected here!" );
            }
        } else if ( node instanceof LogicalValues ) {
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
                return super.handleGeneric( node, builder );
            } else { // partitioned, adjust project
                if ( ((LogicalProject) node).getInput().getRowType().toString().equals( "RecordType(INTEGER ZERO)" ) ) {
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
            RelDataTypeField field = node.getInput().getRowType().getFieldList().get( index );
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


}
