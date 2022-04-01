/*
 * Copyright 2019-2022 The Polypheny Project
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
import org.polypheny.db.algebra.core.Modify;
import org.polypheny.db.algebra.core.Modify.Operation;
import org.polypheny.db.algebra.logical.LogicalBatchIterator;
import org.polypheny.db.algebra.logical.LogicalConditionalExecute;
import org.polypheny.db.algebra.logical.LogicalConstraintEnforcer;
import org.polypheny.db.algebra.logical.LogicalDocuments;
import org.polypheny.db.algebra.logical.LogicalFilter;
import org.polypheny.db.algebra.logical.LogicalModify;
import org.polypheny.db.algebra.logical.LogicalModifyCollect;
import org.polypheny.db.algebra.logical.LogicalProject;
import org.polypheny.db.algebra.logical.LogicalScan;
import org.polypheny.db.algebra.logical.LogicalStreamer;
import org.polypheny.db.algebra.logical.LogicalValues;
import org.polypheny.db.algebra.logical.graph.GraphProject;
import org.polypheny.db.algebra.logical.graph.GraphScan;
import org.polypheny.db.algebra.logical.graph.GraphValues;
import org.polypheny.db.algebra.logical.graph.LogicalGraphModify;
import org.polypheny.db.algebra.logical.graph.LogicalGraphScan;
import org.polypheny.db.algebra.logical.graph.LogicalGraphTransformer;
import org.polypheny.db.algebra.logical.graph.LogicalGraphValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.EntityType;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.CatalogGraphDatabase;
import org.polypheny.db.catalog.entity.CatalogGraphMapping;
import org.polypheny.db.catalog.entity.CatalogGraphPlacement;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.partition.PartitionManager;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.prepare.AlgOptTableImpl;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.prepare.Prepare.PreparingTable;
import org.polypheny.db.processing.WhereClauseVisitor;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.routing.DmlRouter;
import org.polypheny.db.routing.LogicalQueryInformation;
import org.polypheny.db.routing.RoutingManager;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.schema.ModelTrait;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.PolySchemaBuilder;
import org.polypheny.db.schema.Table;
import org.polypheny.db.schema.graph.Graph;
import org.polypheny.db.schema.graph.ModifiableGraph;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.RoutedAlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyType;

@Slf4j
public class DmlRouterImpl extends BaseRouter implements DmlRouter {

    /**
     * Default implementation: Execute DML on all placements
     *
     * @return
     */
    @Override
    public AlgNode routeDml( LogicalModify modify, Statement statement ) {
        AlgOptCluster cluster = modify.getCluster();

        if ( modify.getTable() != null ) {
            AlgOptTableImpl table = (AlgOptTableImpl) modify.getTable();
            if ( table.getTable() instanceof LogicalTable ) {
                LogicalTable t = ((LogicalTable) table.getTable());
                // Get placements of this table
                CatalogEntity catalogEntity = catalog.getTable( t.getTableId() );

                // Make sure that this table can be modified
                if ( !catalogEntity.modifiable ) {
                    if ( catalogEntity.entityType == EntityType.ENTITY ) {
                        throw new RuntimeException( "Unable to modify a table marked as read-only!" );
                    } else if ( catalogEntity.entityType == EntityType.SOURCE ) {
                        throw new RuntimeException( "The table '" + catalogEntity.name + "' is provided by a data source which does not support data modification." );
                    } else if ( catalogEntity.entityType == EntityType.VIEW ) {
                        throw new RuntimeException( "Polypheny-DB does not support modifying views." );
                    }
                    throw new RuntimeException( "Unknown table type: " + catalogEntity.entityType.name() );
                }

                long pkid = catalogEntity.primaryKey;
                List<Long> pkColumnIds = catalog.getPrimaryKey( pkid ).columnIds;
                CatalogColumn pkColumn = catalog.getField( pkColumnIds.get( 0 ) );

                // Essentially gets a list of all stores where this table resides
                List<CatalogColumnPlacement> pkPlacements = catalog.getColumnPlacement( pkColumn.id );

                if ( catalogEntity.partitionProperty.isPartitioned && log.isDebugEnabled() ) {
                    log.debug( "\nListing all relevant stores for table: '{}' and all partitions: {}", catalogEntity.name, catalogEntity.partitionProperty.partitionGroupIds );
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
                    List<CatalogColumnPlacement> placementsOnAdapter = catalog.getColumnPlacementsOnAdapterPerTable( pkPlacement.adapterId, catalogEntity.id );

                    // If this is an update, check whether we need to execute on this store at all
                    List<String> updateColumnList = modify.getUpdateColumnList();
                    List<RexNode> sourceExpressionList = modify.getSourceExpressionList();
                    if ( placementsOnAdapter.size() != catalogEntity.fieldIds.size() ) {

                        if ( modify.getOperation() == Operation.UPDATE ) {
                            updateColumnList = new LinkedList<>( modify.getUpdateColumnList() );
                            sourceExpressionList = new LinkedList<>( modify.getSourceExpressionList() );
                            Iterator<String> updateColumnListIterator = updateColumnList.iterator();
                            Iterator<RexNode> sourceExpressionListIterator = sourceExpressionList.iterator();
                            while ( updateColumnListIterator.hasNext() ) {
                                String columnName = updateColumnListIterator.next();
                                sourceExpressionListIterator.next();
                                try {
                                    CatalogColumn catalogColumn = catalog.getField( catalogEntity.id, columnName );
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
                    if ( catalogEntity.partitionProperty.isPartitioned ) {
                        boolean worstCaseRouting = false;
                        Set<Long> identifiedPartitionsInFilter = new HashSet<>();

                        PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
                        PartitionManager partitionManager = partitionManagerFactory.getPartitionManager( catalogEntity.partitionProperty.partitionType );

                        WhereClauseVisitor whereClauseVisitor = new WhereClauseVisitor( statement, catalogEntity.fieldIds.indexOf( catalogEntity.partitionProperty.partitionColumnId ) );
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
                                identPart = (int) partitionManager.getTargetPartitionId( catalogEntity, value );
                                accessedPartitionList.add( identPart );
                                identifiedPartitionsInFilter.add( identPart );
                            }
                        }

                        String partitionValue = "";
                        // Set true if partitionColumn is part of UPDATE Statement, else assume worst case routing

                        if ( modify.getOperation() == Operation.UPDATE ) {
                            // In case of update always use worst case routing for now.
                            // Since you have to identify the current partition to delete the entry and then create a new entry on the correct partitions
                            int index = 0;

                            for ( String cn : updateColumnList ) {
                                try {
                                    if ( catalog.getField( catalogEntity.id, cn ).id == catalogEntity.partitionProperty.partitionColumnId ) {
                                        if ( log.isDebugEnabled() ) {
                                            log.debug( " UPDATE: Found PartitionColumnID Match: '{}' at index: {}", catalogEntity.partitionProperty.partitionColumnId, index );
                                        }
                                        // Routing/Locking can now be executed on certain partitions
                                        partitionValue = sourceExpressionList.get( index ).toString().replace( "'", "" );
                                        if ( log.isDebugEnabled() ) {
                                            log.debug(
                                                    "UPDATE: partitionColumn-value: '{}' should be put on partition: {}",
                                                    partitionValue,
                                                    partitionManager.getTargetPartitionId( catalogEntity, partitionValue ) );
                                        }
                                        identPart = (int) partitionManager.getTargetPartitionId( catalogEntity, partitionValue );
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

                                            if ( !catalog.getPartitionsOnDataPlacement( pkPlacement.adapterId, catalogEntity.id ).contains( currentPart ) ) {
                                                continue;
                                            }

                                            List<String> qualifiedTableName = ImmutableList.of(
                                                    PolySchemaBuilder.buildAdapterSchemaName(
                                                            pkPlacement.adapterUniqueName,
                                                            catalogEntity.getSchemaName(),
                                                            pkPlacement.physicalSchemaName ),
                                                    t.getLogicalTableName() + "_" + partitionId) );
                                            RelOptTable physical = catalogReader.getTableForMember( qualifiedTableName );
                                            ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );

                                            {@link AlgNode} input = buildDml(
                                                    recursiveCopy( node.getInput( 0 ) ),
                                                    AlgBuilder.create( statement, cluster ),
                                                    catalogEntity,
                                                    placementsOnAdapter,
                                                    catalog.getPartitionPlacement( pkPlacement.adapterId, currentPart ),
                                                    statement,
                                                    cluster ).build();

                                            Modify deleteModify = LogicalModify.create(
                                                    physical,
                                                    catalogReader,
                                                    input,
                                                    Operation.DELETE,
                                                    null,
                                                    null,
                                                    ((LogicalModify) node).isFlattened() );

                                            modifies.add( deleteModify );


                                        }

                                                //Inject INSERT statement for identified SET partitionId
                                                //Otherwise data migrator would be needed
                                        if ( catalog.getPartitionsOnDataPlacement( pkPlacement.adapterId, catalogEntity.id ).contains( identifiedPartitionForSetValue ) ) {

                                           /* List<String> qualifiedTableName = ImmutableList.of(
                                                    PolySchemaBuilder.buildAdapterSchemaName(
                                                            pkPlacement.adapterUniqueName,
                                                            catalogEntity.getSchemaName(),
                                                            pkPlacement.physicalSchemaName,
                                                            identifiedPartitionForSetValue ),
                                                    t.getLogicalTableName() );
                                            RelOptTable physical = catalogReader.getTableForMember( qualifiedTableName );
                                            ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );

                                            {@link AlgNode} input = buildDml(
                                                    recursiveCopy( node.getInput( 0 ) ),
                                                    AlgBuilder.create( statement, cluster ),
                                                    catalogEntity,
                                                    placementsOnAdapter,
                                                    catalog.getPartitionPlacement( pkPlacement.adapterId, identifiedPartitionForSetValue ),
                                                    statement,
                                                    cluster ).build();

                                            Modify insertModify = modifiableTable.toModificationRel(
                                                    cluster,
                                                    physical,
                                                    catalogReader,
                                                    input,
                                                    Operation.INSERT,
                                                    null,
                                                    null,
                                                    ((LogicalModify) node).isFlattened()
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

                                // Data Migrate copy of all other partitions beside the identified on towards the identified one
                                // Then inject a DELETE statement for all those partitions

                                // Do the update only on the identified partition

                            }// If nothing has been specified
                            //Partition functionality cannot be used --> worstCase --> send query to every partition
                            else {
                                worstCaseRouting = true;
                                accessedPartitionList = new HashSet<>( catalogEntity.partitionProperty.partitionIds );
                            }

                        } else if ( modify.getOperation() == Operation.INSERT ) {
                            int i;

                            if ( modify.getInput() instanceof LogicalValues ) {

                                // Get fieldList and map columns to index since they could be in arbitrary order
                                int partitionColumnIndex = -1;
                                Map<Long, Integer> resultColMapping = new HashMap<>();
                                for ( int j = 0; j < (modify.getInput()).getRowType().getFieldList().size(); j++ ) {
                                    String columnFieldName = (modify.getInput()).getRowType().getFieldList().get( j ).getKey();

                                    // Retrieve columnId of fieldName and map it to its fieldList location of INSERT Stmt
                                    int columnIndex = catalogEntity.getColumnNames().indexOf( columnFieldName );
                                    resultColMapping.put( catalogEntity.fieldIds.get( columnIndex ), j );

                                    // Determine location of partitionColumn in fieldList
                                    if ( catalogEntity.fieldIds.get( columnIndex ) == catalogEntity.partitionProperty.partitionColumnId ) {
                                        partitionColumnIndex = columnIndex;
                                        if ( log.isDebugEnabled() ) {
                                            log.debug( "INSERT: Found PartitionColumnID: '{}' at column index: {}", catalogEntity.partitionProperty.partitionColumnId, j );
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
                                    identPart = (int) partitionManager.getTargetPartitionId( catalogEntity, partitionValue );
                                    accessedPartitionList.add( identPart );

                                    if ( !tuplesOnPartition.containsKey( identPart ) ) {
                                        tuplesOnPartition.put( identPart, new ArrayList<>() );
                                    }
                                    tuplesOnPartition.get( identPart ).add( currentTuple );

                                }

                                for ( Map.Entry<Long, List<ImmutableList<RexLiteral>>> partitionMapping : tuplesOnPartition.entrySet() ) {
                                    Long currentPartitionId = partitionMapping.getKey();

                                    if ( !catalog.getPartitionsOnDataPlacement( pkPlacement.adapterId, catalogEntity.id ).contains( currentPartitionId ) ) {
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
                                                catalogEntity,
                                                placementsOnAdapter,
                                                catalog.getPartitionPlacement( pkPlacement.adapterId, currentPartitionId ),
                                                statement,
                                                cluster,
                                                true,
                                                statement.getDataContext().getParameterValues() ).build();

                                        List<String> qualifiedTableName = ImmutableList.of(
                                                PolySchemaBuilder.buildAdapterSchemaName(
                                                        pkPlacement.adapterUniqueName,
                                                        catalogEntity.getNamespaceName(),
                                                        pkPlacement.physicalSchemaName
                                                ),
                                                t.getLogicalTableName() + "_" + currentPartitionId );
                                        AlgOptTable physical = catalogReader.getTableForMember( qualifiedTableName );
                                        ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );

                                        // Build DML
                                        Modify adjustedModify = modifiableTable.toModificationAlg(
                                                cluster,
                                                physical,
                                                catalogReader,
                                                input,
                                                modify.getOperation(),
                                                updateColumnList,
                                                sourceExpressionList,
                                                modify.isFlattened() );

                                        modifies.add( adjustedModify );

                                    }
                                }
                                operationWasRewritten = true;

                            } else if ( modify.getInput() instanceof LogicalProject
                                    && ((LogicalProject) modify.getInput()).getInput() instanceof LogicalValues ) {

                                String partitionColumnName = catalog.getField( catalogEntity.partitionProperty.partitionColumnId ).name;
                                List<String> fieldNames = modify.getInput().getRowType().getFieldNames();

                                LogicalModify ltm = modify;
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

                                            for ( Map<Long, Object> currentRow : statement.getDataContext().getParameterValues() ) {

                                                tempPartitionId = partitionManager.getTargetPartitionId( catalogEntity, currentRow.get( partitionValueIndex ).toString() );

                                                if ( !catalog.getPartitionsOnDataPlacement( pkPlacement.adapterId, catalogEntity.id ).contains( tempPartitionId ) ) {
                                                    continue;
                                                }

                                                List<Map<Long, Object>> parameterValues = new ArrayList<>();
                                                parameterValues.add( new HashMap<>( newParameterValues ) );
                                                parameterValues.get( 0 ).putAll( currentRow );

                                                AlgNode input = buildDml(
                                                        super.recursiveCopy( modify.getInput( 0 ) ),
                                                        RoutedAlgBuilder.create( statement, cluster ),
                                                        catalogEntity,
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
                                                                catalogEntity.getNamespaceName(),
                                                                pkPlacement.physicalSchemaName
                                                        ),
                                                        t.getLogicalTableName() + "_" + tempPartitionId );
                                                AlgOptTable physical = catalogReader.getTableForMember( qualifiedTableName );
                                                ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );

                                                // Build DML
                                                Modify adjustedModify = modifiableTable.toModificationAlg(
                                                        cluster,
                                                        physical,
                                                        catalogReader,
                                                        input,
                                                        modify.getOperation(),
                                                        updateColumnList,
                                                        sourceExpressionList,
                                                        modify.isFlattened() );

                                                modifies.add( adjustedModify );
                                            }

                                            operationWasRewritten = true;
                                            worstCaseRouting = false;
                                        } else {
                                            partitionValue = ((LogicalProject) modify.getInput()).getProjects().get( i ).toString().replace( "'", "" );
                                            identPart = (int) partitionManager.getTargetPartitionId( catalogEntity, partitionValue );
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
                                String partitionColumnName = catalog.getField( catalogEntity.partitionProperty.partitionColumnId ).name;
                                String partitionName = catalog.getPartitionGroup( identPart ).partitionGroupName;
                                log.debug( "INSERT: partitionColumn-value: '{}' should be put on partition: {} ({}), which is partitioned with column {}",
                                        partitionValue, identPart, partitionName, partitionColumnName );
                            }


                        } else if ( modify.getOperation() == Operation.DELETE ) {
                            if ( whereClauseValues == null ) {
                                worstCaseRouting = true;
                            } else {
                                worstCaseRouting = whereClauseValues.size() >= 4;
                            }

                        }

                        if ( worstCaseRouting ) {
                            log.debug( "PartitionColumnID was not an explicit part of statement, partition routing will therefore assume worst-case: Routing to ALL PARTITIONS" );
                            accessedPartitionList = catalogEntity.partitionProperty.partitionIds.stream().collect( Collectors.toSet() );
                        }
                    } else {
                        // un-partitioned tables only have one partition anyway
                        identPart = catalogEntity.partitionProperty.partitionIds.get( 0 );
                        accessedPartitionList.add( identPart );
                    }

                    if ( statement.getMonitoringEvent() != null ) {
                        statement.getMonitoringEvent()
                                .updateAccessedPartitions(
                                        Collections.singletonMap( catalogEntity.id, accessedPartitionList )
                                );
                    }

                    if ( !operationWasRewritten ) {

                        for ( long partitionId : accessedPartitionList ) {

                            if ( !catalog.getPartitionsOnDataPlacement( pkPlacement.adapterId, catalogEntity.id ).contains( partitionId ) ) {
                                continue;
                            }

                            List<String> qualifiedTableName = ImmutableList.of(
                                    PolySchemaBuilder.buildAdapterSchemaName(
                                            pkPlacement.adapterUniqueName,
                                            catalogEntity.getNamespaceName(),
                                            pkPlacement.physicalSchemaName
                                    ),
                                    t.getLogicalTableName() + "_" + partitionId );
                            AlgOptTable physical = catalogReader.getTableForMember( qualifiedTableName );

                            // Build DML
                            Modify adjustedModify;
                            AlgNode input = buildDml(
                                    super.recursiveCopy( modify.getInput( 0 ) ),
                                    RoutedAlgBuilder.create( statement, cluster ),
                                    catalogEntity,
                                    placementsOnAdapter,
                                    catalog.getPartitionPlacement( pkPlacement.adapterId, partitionId ),
                                    statement,
                                    cluster,
                                    false,
                                    statement.getDataContext().getParameterValues() ).build();

                            ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );

                            if ( modifiableTable != null && modifiableTable == physical.unwrap( Table.class ) ) {
                                adjustedModify = modifiableTable.toModificationAlg(
                                        cluster,
                                        physical,
                                        catalogReader,
                                        input,
                                        modify.getOperation(),
                                        updateColumnList,
                                        sourceExpressionList,
                                        modify.isFlattened()
                                );
                            } else {
                                adjustedModify = LogicalModify.create(
                                        physical,
                                        catalogReader,
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
        } else if ( lce.getRight() instanceof LogicalModify ) {
            action = routeDml( (LogicalModify) lce.getRight(), statement );
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

        if ( constraint.getLeft() instanceof Modify ) {
            return LogicalConstraintEnforcer.create(
                    routeDml( (LogicalModify) constraint.getLeft(), statement ),
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
        if ( iterator.getInput() instanceof Modify ) {
            input = routeDml( (LogicalModify) iterator.getInput(), statement );
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
    public AlgNode routeGraphDml( LogicalGraphModify alg, Statement statement ) {
        if ( alg.getGraph() == null ) {
            throw new RuntimeException( "Error while routing graph" );
        }

        PolyphenyDbCatalogReader reader = statement.getTransaction().getCatalogReader();

        CatalogGraphDatabase catalogGraph = Catalog.getInstance().getGraph( alg.getGraph().getId() );
        for ( int adapterId : catalogGraph.placements ) {
            CatalogAdapter adapter = Catalog.getInstance().getAdapter( adapterId );
            CatalogGraphPlacement graphPlacement = Catalog.getInstance().getGraphPlacement( catalogGraph.id, adapterId );
            String name = PolySchemaBuilder.buildAdapterSchemaName( adapter.uniqueName, catalogGraph.name, graphPlacement.physicalName );

            Graph graph = reader.getGraph( name );
            if ( graph == null ) {
                return attachRelationalModify( alg, statement );
            }

            if ( !(graph instanceof ModifiableGraph) ) {
                throw new RuntimeException( "Graph is not modifiable." );
            }

            return ((ModifiableGraph) graph).toModificationAlg(
                    alg.getCluster(),
                    alg.getTraitSet(),
                    graph,
                    alg.catalogReader,
                    buildGraphDml( alg.getInput(), statement ),
                    alg.operation,
                    alg.ids,
                    alg.operations );

        }

        return alg;
    }


    private AlgNode buildGraphDml( AlgNode node, Statement statement ) {
        if ( node instanceof GraphScan ) {
            return super.handleGraphScan( (LogicalGraphScan) node, statement );
        }
        int i = 0;
        for ( AlgNode input : node.getInputs() ) {
            node.replaceInput( i, buildGraphDml( input, statement ) );
            i++;
        }
        return node;
    }


    private AlgNode attachRelationalModify( LogicalGraphModify alg, Statement statement ) {
        CatalogGraphMapping mapping = Catalog.getInstance().getGraphMapping( alg.getGraph().getId() );

        PreparingTable nodesTable = getSubstitutionTable( statement, mapping.nodesId, mapping.idNodeId );
        PreparingTable nodePropertiesTable = getSubstitutionTable( statement, mapping.nodesPropertyId, mapping.idNodesPropertyId );
        PreparingTable edgesTable = getSubstitutionTable( statement, mapping.edgesId, mapping.idEdgeId );
        PreparingTable edgePropertiesTable = getSubstitutionTable( statement, mapping.edgesPropertyId, mapping.idEdgesPropertyId );

        List<AlgNode> inputs = new ArrayList<>();
        switch ( alg.operation ) {
            case INSERT:
                if ( alg.getInput() instanceof GraphValues ) {
                    // simple value insert
                    inputs.addAll( ((LogicalGraphValues) alg.getInput()).getRelationalEquivalent( List.of(), List.of( nodesTable, nodePropertiesTable, edgesTable, edgePropertiesTable ) ) );
                }
                if ( alg.getInput() instanceof GraphProject ) {
                    return attachRelationalRelatedInsert( alg, statement, nodesTable, nodePropertiesTable, edgesTable, edgePropertiesTable );
                    //inputs.addAll( ((LogicalGraphProject)alg.getInput()).ge );
                }

                break;
            case UPDATE:
                break;
            case DELETE:
                break;
            case MERGE:
                break;
        }

        List<AlgNode> modifies = new ArrayList<>();
        if ( inputs.get( 0 ) != null ) {
            modifies.add( getModify( nodesTable, inputs.get( 0 ), statement, alg.operation ) );
        }

        if ( inputs.get( 1 ) != null ) {
            modifies.add( getModify( nodePropertiesTable, inputs.get( 1 ), statement, alg.operation ) );
        }

        if ( inputs.size() > 2 ) {
            if ( inputs.get( 2 ) != null ) {
                modifies.add( getModify( edgesTable, inputs.get( 2 ), statement, alg.operation ) );
            }

            if ( inputs.get( 3 ) != null ) {
                modifies.add( getModify( edgePropertiesTable, inputs.get( 3 ), statement, alg.operation ) );
            }
        }

        return new LogicalModifyCollect( alg.getCluster(), alg.getTraitSet().replace( ModelTrait.GRAPH ), modifies, true );
    }


    private AlgNode attachRelationalRelatedInsert( LogicalGraphModify alg, Statement statement, PreparingTable nodesTable, PreparingTable nodePropertiesTable, PreparingTable edgesTable, PreparingTable edgePropertiesTable ) {
        AlgNode project = buildGraphDml( alg.getInput(), statement );

        List<AlgNode> inputs = new ArrayList<>();
        List<PolyType> sequence = new ArrayList<>();
        for ( AlgDataTypeField field : project.getRowType().getFieldList() ) {
            sequence.add( field.getType().getPolyType() );
            if ( field.getType().getPolyType() == PolyType.EDGE ) {
                inputs.addAll( attachPreparedGraphEdgeModify( alg.getCluster(), edgesTable, edgePropertiesTable, statement ) );
            } else if ( field.getType().getPolyType() == PolyType.NODE ) {
                inputs.addAll( attachPreparedGraphNodeModify( alg.getCluster(), nodesTable, nodePropertiesTable, statement ) );
            } else {
                throw new RuntimeException( "Graph insert of non-graph elements is not possible." );
            }
        }
        AlgRecordType updateRowType = new AlgRecordType( List.of( new AlgDataTypeFieldImpl( "ROWCOUNT", 0, alg.getCluster().getTypeFactory().createPolyType( PolyType.BIGINT ) ) ) );
        LogicalGraphTransformer transformer = new LogicalGraphTransformer( alg.getCluster(), alg.getTraitSet(), inputs, updateRowType, sequence );
        return new LogicalStreamer( alg.getCluster(), alg.getTraitSet(), project, transformer );
    }


    private List<AlgNode> attachPreparedGraphNodeModify( AlgOptCluster cluster, PreparingTable nodesTable, PreparingTable nodePropertiesTable, Statement statement ) {
        AlgBuilder algBuilder = AlgBuilder.create( statement );
        RexBuilder rexBuilder = algBuilder.getRexBuilder();
        AlgDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

        List<AlgNode> inputs = new ArrayList<>();
        LogicalProject preparedNodes = LogicalProject.create(
                LogicalValues.createOneRow( cluster ),
                List.of(
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 36 ), 0 ), // id
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 255 ), 1 ) ), // label
                nodesTable.getRowType() );

        inputs.add( getModify( nodesTable, preparedNodes, statement, Operation.INSERT ) );

        LogicalProject preparedNProperties = LogicalProject.create(
                LogicalValues.createOneRow( cluster ),
                List.of(
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 36 ), 0 ), // id
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 255 ), 1 ), // key
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 255 ), 2 ) ), // value
                nodePropertiesTable.getRowType() );

        inputs.add( getModify( nodePropertiesTable, preparedNProperties, statement, Operation.INSERT ) );

        return inputs;
    }


    private List<AlgNode> attachPreparedGraphEdgeModify( AlgOptCluster cluster, PreparingTable edgesTable, PreparingTable edgePropertiesTable, Statement statement ) {
        AlgBuilder algBuilder = AlgBuilder.create( statement );
        RexBuilder rexBuilder = algBuilder.getRexBuilder();
        AlgDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

        List<AlgNode> inputs = new ArrayList<>();
        LogicalProject preparedEdges = LogicalProject.create(
                LogicalValues.createOneRow( cluster ),
                List.of(
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 36 ), 0 ), // id
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 255 ), 1 ), // label
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 36 ), 2 ), // source
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 36 ), 3 ) ), // target
                edgesTable.getRowType() );

        inputs.add( getModify( edgesTable, preparedEdges, statement, Operation.INSERT ) );

        LogicalProject preparedEProperties = LogicalProject.create(
                LogicalValues.createOneRow( cluster ),
                List.of(
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 36 ), 0 ), // id
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 255 ), 1 ), // key
                        rexBuilder.makeDynamicParam( typeFactory.createPolyType( PolyType.VARCHAR, 255 ), 2 ) ), // value
                edgePropertiesTable.getRowType() );

        inputs.add( getModify( edgePropertiesTable, preparedEProperties, statement, Operation.INSERT ) );

        return inputs;

    }


    private Modify getModify( AlgOptTable table, AlgNode input, Statement statement, Operation operation ) {
        return table.unwrap( ModifiableTable.class ).toModificationAlg( input.getCluster(), table, statement.getTransaction().getCatalogReader(), input, operation, null, null, true );
    }


    private AlgBuilder buildDml(
            AlgNode node,
            RoutedAlgBuilder builder,
            CatalogEntity catalogEntity,
            List<CatalogColumnPlacement> placements,
            CatalogPartitionPlacement partitionPlacement,
            Statement statement,
            AlgOptCluster cluster,
            boolean remapParameterValues,
            List<Map<Long, Object>> parameterValues ) {
        for ( int i = 0; i < node.getInputs().size(); i++ ) {
            buildDml( node.getInput( i ), builder, catalogEntity, placements, partitionPlacement, statement, cluster, remapParameterValues, parameterValues );
        }

        if ( log.isDebugEnabled() ) {
            log.debug( "List of Store specific ColumnPlacements: " );
            for ( CatalogColumnPlacement ccp : placements ) {
                log.debug( "{}.{}", ccp.adapterUniqueName, ccp.getLogicalColumnName() );
            }
        }

        if ( node instanceof LogicalScan && node.getTable() != null ) {
            AlgOptTableImpl table = (AlgOptTableImpl) node.getTable();

            if ( table.getTable() instanceof LogicalTable ) {
                // Special handling for INSERT INTO foo SELECT * FROM foo2
                if ( ((LogicalTable) table.getTable()).getTableId() != catalogEntity.id ) {
                    // TODO: how build select from here?
                    // return buildSelect( node, builder, statement, cluster );
                }

                builder = super.handleScan(
                        builder,
                        placements.get( 0 ).tableId,
                        placements.get( 0 ).adapterUniqueName,
                        catalogEntity.getNamespaceName(),
                        catalogEntity.name,
                        placements.get( 0 ).physicalSchemaName,
                        partitionPlacement.physicalTableName,
                        partitionPlacement.partitionId );

                return builder;

            } else {
                throw new RuntimeException( "Unexpected table. Only logical tables expected here!" );
            }
        } else if ( node instanceof LogicalValues ) {
            if ( node.getModel() == NamespaceType.DOCUMENT ) {
                return handleDocuments( (LogicalDocuments) node, builder );
            }

            LogicalValues values = (LogicalValues) node;

            builder = super.handleValues( values, builder );

            if ( catalogEntity.fieldIds.size() == placements.size() ) { // full placement, no additional checks required
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
            if ( catalogEntity.fieldIds.size() == placements.size() ) { // full placement, generic handling is sufficient
                if ( catalogEntity.partitionProperty.isPartitioned && remapParameterValues ) {  //  && ((LogicalProject) node).getInput().getRowType().toString().equals( "RecordType(INTEGER ZERO)" )
                    return remapParameterizedDml( node, builder, statement, parameterValues );
                } else {
                    return super.handleGeneric( node, builder );
                }
            } else { // vertically partitioned, adjust project
                if ( ((LogicalProject) node).getInput().getRowType().toString().equals( "RecordType(INTEGER ZERO)" ) ) {
                    if ( catalogEntity.partitionProperty.isPartitioned && remapParameterValues ) {
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
            if ( catalogEntity.fieldIds.size() != placements.size() ) { // partitioned, check if there is a illegal condition
                RexCall call = ((RexCall) ((LogicalFilter) node).getCondition());

                for ( RexNode operand : call.operands ) {
                    dmlConditionCheck( (LogicalFilter) node, catalogEntity, placements, operand );
                }
            }
            return super.handleGeneric( node, builder );
        } else {
            return super.handleGeneric( node, builder );
        }
    }


    private void dmlConditionCheck( LogicalFilter node, CatalogEntity catalogEntity, List<CatalogColumnPlacement> placements, RexNode operand ) {
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
                    if ( !catalogEntity.name.equalsIgnoreCase( columnNames[0] ) ) {
                        throw new RuntimeException( "Table name does not match expected table name: " + field.getName() );
                    }
                    columnName = columnNames[1];
                } else if ( columnNames.length == 3 ) { // schemaName.tableName.columnName
                    if ( !catalogEntity.getNamespaceName().equalsIgnoreCase( columnNames[0] ) ) {
                        throw new RuntimeException( "Schema name does not match expected schema name: " + field.getName() );
                    }
                    if ( !catalogEntity.name.equalsIgnoreCase( columnNames[1] ) ) {
                        throw new RuntimeException( "Table name does not match expected table name: " + field.getName() );
                    }
                    columnName = columnNames[2];
                } else {
                    throw new RuntimeException( "Invalid column name: " + field.getName() );
                }
                column = catalog.getField( catalogEntity.id, columnName );
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
                dmlConditionCheck( node, catalogEntity, placements, o );
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
