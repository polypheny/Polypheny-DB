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

package org.polypheny.db.router;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.partition.PartitionManager;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.prepare.RelOptTableImpl;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.RelShuttleImpl;
import org.polypheny.db.rel.core.ConditionalExecute;
import org.polypheny.db.rel.core.JoinRelType;
import org.polypheny.db.rel.core.SetOp;
import org.polypheny.db.rel.core.TableModify;
import org.polypheny.db.rel.core.TableModify.Operation;
import org.polypheny.db.rel.logical.LogicalConditionalExecute;
import org.polypheny.db.rel.logical.LogicalFilter;
import org.polypheny.db.rel.logical.LogicalModifyCollect;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rel.logical.LogicalTableModify;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.logical.LogicalValues;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.routing.Router;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.PolySchemaBuilder;
import org.polypheny.db.schema.Table;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.transaction.Statement;


@Slf4j
public abstract class AbstractRouter implements Router {

    protected ExecutionTimeMonitor executionTimeMonitor;

    protected InformationPage page = null;

    final Catalog catalog = Catalog.getInstance();


    private final Map<Integer, List<String>> filterMap = new HashMap<>();
    private static final Cache<Integer, RelNode> joinedTableScanCache = CacheBuilder.newBuilder()
            .maximumSize( RuntimeConfig.JOINED_TABLE_SCAN_CACHE_SIZE.getInteger() )
            .build();


    // For reporting purposes
    protected Map<Long, SelectedAdapterInfo> selectedAdapter;


    @Override
    public RelRoot route( RelRoot logicalRoot, Statement statement, ExecutionTimeMonitor executionTimeMonitor ) {
        this.executionTimeMonitor = executionTimeMonitor;
        this.selectedAdapter = new HashMap<>();

        if ( statement.getTransaction().isAnalyze() ) {
            InformationManager queryAnalyzer = statement.getTransaction().getQueryAnalyzer();
            page = new InformationPage( "Routing" );
            page.fullWidth();
            queryAnalyzer.addPage( page );
        }

        RelNode routed;
        analyze( statement, logicalRoot );
        if ( logicalRoot.rel instanceof LogicalTableModify ) {

            routed = routeDml( logicalRoot.rel, statement );
        } else if ( logicalRoot.rel instanceof ConditionalExecute ) {
            routed = handleConditionalExecute( logicalRoot.rel, statement );
        } else {
            RelBuilder builder = RelBuilder.create( statement, logicalRoot.rel.getCluster() );
            builder = buildDql( logicalRoot.rel, builder, statement, logicalRoot.rel.getCluster() );
            routed = builder.build();
        }

        wrapUp( statement, routed );

        // Add information to query analyzer
        if ( statement.getTransaction().isAnalyze() ) {
            InformationGroup group = new InformationGroup( page, "Selected Adapters" );
            statement.getTransaction().getQueryAnalyzer().addGroup( group );
            InformationTable table = new InformationTable(
                    group,
                    ImmutableList.of( "Table", "Adapter", "Physical Name" ) );
            selectedAdapter.forEach( ( k, v ) -> {
                CatalogTable catalogTable = Catalog.getInstance().getTable( k );
                table.addRow( catalogTable.getSchemaName() + "." + catalogTable.name, v.uniqueName, v.physicalSchemaName + "." + v.physicalTableName );
            } );
            statement.getTransaction().getQueryAnalyzer().registerInformation( table );
        }

        return new RelRoot(
                routed,
                logicalRoot.validatedRowType,
                logicalRoot.kind,
                logicalRoot.fields,
                logicalRoot.collation );
    }


    protected abstract void analyze( Statement statement, RelRoot logicalRoot );

    protected abstract void wrapUp( Statement statement, RelNode routed );

    // Select the placement on which a table scan should be executed
    protected abstract List<CatalogColumnPlacement> selectPlacement( RelNode node, CatalogTable catalogTable );


    protected RelBuilder buildDql( RelNode node, RelBuilder builder, Statement statement, RelOptCluster cluster ) {
        if ( node instanceof SetOp ) {
            return buildSetOp( node, builder, statement, cluster );
        } else {
            return buildSelect( node, builder, statement, cluster );
        }
    }


    protected RelBuilder buildSelect( RelNode node, RelBuilder builder, Statement statement, RelOptCluster cluster ) {
        for ( int i = 0; i < node.getInputs().size(); i++ ) {
            // Check if partition used in general to reduce overhead if not for un-partitioned
            if ( node instanceof LogicalFilter && ((LogicalFilter) node).getInput().getTable() != null ) {

                RelOptTableImpl table = (RelOptTableImpl) ((LogicalFilter) node).getInput().getTable();

                if ( table.getTable() instanceof LogicalTable ) {

                    // TODO Routing of partitioned tables is very limited. This should be improved to also apply sophisticated
                    //  routing strategies, especially when we also get rid of the worst-case routing.

                    LogicalTable t = ((LogicalTable) table.getTable());
                    CatalogTable catalogTable;
                    catalogTable = Catalog.getInstance().getTable( t.getTableId() );
                    if ( catalogTable.isPartitioned ) {
                        WhereClauseVisitor whereClauseVisitor = new WhereClauseVisitor( statement, catalogTable.columnIds.indexOf( catalogTable.partitionColumnId ) );
                        node.accept( new RelShuttleImpl() {
                            @Override
                            public RelNode visit( LogicalFilter filter ) {
                                super.visit( filter );
                                filter.accept( whereClauseVisitor );
                                return filter;
                            }
                        } );

                        if ( whereClauseVisitor.valueIdentified ) {
                            List<Object> values = whereClauseVisitor.getValues().stream()
                                    .map( Object::toString )
                                    .collect( Collectors.toList() );
                            int scanId = 0;
                            if ( !values.isEmpty() ) {
                                scanId = ((LogicalFilter) node).getInput().getId();
                                filterMap.put( scanId, whereClauseVisitor.getValues().stream()
                                        .map( Object::toString )
                                        .collect( Collectors.toList() ) );
                            }
                            buildDql( node.getInput( i ), builder, statement, cluster );
                            filterMap.remove( scanId );
                        } else {
                            buildDql( node.getInput( i ), builder, statement, cluster );
                        }
                    } else {
                        buildDql( node.getInput( i ), builder, statement, cluster );
                    }
                }
            } else {
                buildDql( node.getInput( i ), builder, statement, cluster );
            }
        }

        if ( node instanceof LogicalTableScan && node.getTable() != null ) {
            RelOptTableImpl table = (RelOptTableImpl) node.getTable();

            if ( table.getTable() instanceof LogicalTable ) {
                LogicalTable t = ((LogicalTable) table.getTable());
                CatalogTable catalogTable;
                List<CatalogColumnPlacement> placements;
                catalogTable = Catalog.getInstance().getTable( t.getTableId() );

                // Check if table is even partitioned
                if ( catalogTable.isPartitioned ) {

                    // TODO Routing of partitioned tables is very limited. This should be improved to also apply sophisticated
                    //  routing strategies, especially when we also get rid of the worst-case routing.

                    if ( log.isDebugEnabled() ) {
                        log.debug( "VALUE from Map: {} id: {}", filterMap.get( node.getId() ), node.getId() );
                    }
                    List<String> partitionValues = filterMap.get( node.getId() );

                    PartitionManagerFactory partitionManagerFactory = new PartitionManagerFactory();
                    PartitionManager partitionManager = partitionManagerFactory.getInstance( catalogTable.partitionType );
                    if ( partitionValues != null ) {
                        if ( log.isDebugEnabled() ) {
                            log.debug( "TableID: {} is partitioned on column: {} - {}",
                                    t.getTableId(),
                                    catalogTable.partitionColumnId,
                                    catalog.getColumn( catalogTable.partitionColumnId ).name );
                        }
                        if ( partitionValues.size() == 1 ) {
                            List<Long> identPartitions = new ArrayList<>();
                            for ( String partitionValue : partitionValues ) {
                                log.debug( "Extracted PartitionValue: {}", partitionValue );
                                long identPart = partitionManager.getTargetPartitionId( catalogTable, partitionValue );
                                identPartitions.add( identPart );
                                log.debug( "Identified PartitionId: {} for value: {}", identPart, partitionValue );
                            }
                            placements = partitionManager.getRelevantPlacements( catalogTable, identPartitions );
                        } else {
                            placements = partitionManager.getRelevantPlacements( catalogTable, null );
                        }
                    } else {
                        // TODO Change to worst-case
                        placements = partitionManager.getRelevantPlacements( catalogTable, null );
                        //placements = selectPlacement( node, catalogTable );
                    }

                } else {
                    log.debug( "{} is NOT partitioned - Routing will be easy", catalogTable.name );
                    placements = selectPlacement( node, catalogTable );
                }

                return builder.push( buildJoinedTableScan( statement, cluster, placements ) );

            } else {
                throw new RuntimeException( "Unexpected table. Only logical tables expected here!" );
            }
        } else if ( node instanceof LogicalValues ) {
            return handleValues( (LogicalValues) node, builder );
        } else {
            return handleGeneric( node, builder );
        }
    }


    protected RelBuilder buildSetOp( RelNode node, RelBuilder builder, Statement statement, RelOptCluster cluster ) {
        buildDql( node.getInput( 0 ), builder, statement, cluster );

        RelBuilder builder0 = RelBuilder.create( statement, cluster );
        buildDql( node.getInput( 1 ), builder0, statement, cluster );

        builder.replaceTop( node.copy( node.getTraitSet(), ImmutableList.of( builder.peek(), builder0.build() ) ) );
        return builder;
    }


    protected RelNode recursiveCopy( RelNode node ) {
        List<RelNode> inputs = new LinkedList<>();
        if ( node.getInputs() != null && node.getInputs().size() > 0 ) {
            for ( RelNode input : node.getInputs() ) {
                inputs.add( recursiveCopy( input ) );
            }
        }
        return node.copy( node.getTraitSet(), inputs );
    }


    protected RelNode handleConditionalExecute( RelNode node, Statement statement ) {
        LogicalConditionalExecute lce = (LogicalConditionalExecute) node;
        RelBuilder builder = RelBuilder.create( statement, node.getCluster() );
        buildSelect( lce.getLeft(), builder, statement, node.getCluster() );
        RelNode action;
        if ( lce.getRight() instanceof LogicalConditionalExecute ) {
            action = handleConditionalExecute( lce.getRight(), statement );
        } else if ( lce.getRight() instanceof TableModify ) {
            action = routeDml( lce.getRight(), statement );
        } else {
            throw new IllegalArgumentException();
        }
        return LogicalConditionalExecute.create( builder.build(), action, lce );
    }


    // Default implementation: Execute DML on all placements
    protected RelNode routeDml( RelNode node, Statement statement ) {
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
                List<Long> pkColumnIds = Catalog.getInstance().getPrimaryKey( pkid ).columnIds;
                CatalogColumn pkColumn = Catalog.getInstance().getColumn( pkColumnIds.get( 0 ) );
                List<CatalogColumnPlacement> pkPlacements = catalog.getColumnPlacements( pkColumn.id );

                if ( catalogTable.isPartitioned && log.isDebugEnabled() ) {
                    log.debug( "\nListing all relevant stores for table: '{}' and all partitions: {}", catalogTable.name, catalogTable.partitionIds );
                    for ( CatalogColumnPlacement dataPlacement : pkPlacements ) {
                        log.debug( "\t\t -> '{}' {}\t{}",
                                dataPlacement.adapterUniqueName,
                                catalog.getPartitionsOnDataPlacement( dataPlacement.adapterId, dataPlacement.tableId ),
                                catalog.getPartitionsIndexOnDataPlacement( dataPlacement.adapterId, dataPlacement.tableId ) );
                    }
                }

                // Execute on all primary key placements
                List<TableModify> modifies = new ArrayList<>( pkPlacements.size() );
                for ( CatalogColumnPlacement pkPlacement : pkPlacements ) {
                    CatalogReader catalogReader = statement.getTransaction().getCatalogReader();

                    List<String> qualifiedTableName = ImmutableList.of(
                            PolySchemaBuilder.buildAdapterSchemaName(
                                    pkPlacement.adapterUniqueName,
                                    catalogTable.getSchemaName(),
                                    pkPlacement.physicalSchemaName ),
                            t.getLogicalTableName() );
                    RelOptTable physical = catalogReader.getTableForMember( qualifiedTableName );
                    ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );

                    // Get placements on store
                    List<CatalogColumnPlacement> placementsOnAdapter = catalog.getColumnPlacementsOnAdapter( pkPlacement.adapterId, catalogTable.id );

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

                    // Identify where clause of UPDATE
                    if ( catalogTable.isPartitioned ) {
                        boolean worstCaseRouting = false;

                        PartitionManagerFactory partitionManagerFactory = new PartitionManagerFactory();
                        PartitionManager partitionManager = partitionManagerFactory.getInstance( catalogTable.partitionType );
                        partitionManager.validatePartitionDistribution( catalogTable );

                        WhereClauseVisitor whereClauseVisitor = new WhereClauseVisitor( statement, catalogTable.columnIds.indexOf( catalogTable.partitionColumnId ) );
                        node.accept( new RelShuttleImpl() {
                            @Override
                            public RelNode visit( LogicalFilter filter ) {
                                super.visit( filter );
                                filter.accept( whereClauseVisitor );
                                return filter;
                            }
                        } );

                        List<String> whereClauseValue = null;
                        if ( !whereClauseVisitor.getValues().isEmpty() ) {
                            if ( whereClauseVisitor.getValues().size() == 1 ) {
                                whereClauseValue = whereClauseVisitor.getValues().stream()
                                        .map( Object::toString )
                                        .collect( Collectors.toList() );
                                log.debug( "Found Where Clause Values: {}", whereClauseValue );
                                worstCaseRouting = true;
                            }
                        }

                        long identPart = -1;

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
                                        break;
                                    }
                                } catch ( UnknownColumnException e ) {
                                    throw new RuntimeException( e );
                                }
                                index++;
                            }

                            // If only one where clause op
                            if ( whereClauseValue != null && partitionColumnIdentified ) {
                                if ( whereClauseValue.size() == 1 && identPart == partitionManager.getTargetPartitionId( catalogTable, whereClauseValue.get( 0 ) ) ) {
                                    worstCaseRouting = false;
                                } else {
                                    worstCaseRouting = true;
                                    log.debug( "Activate WORST-CASE ROUTING" );
                                }
                            } else if ( whereClauseValue == null ) {
                                worstCaseRouting = true;
                                log.debug( "Activate WORST-CASE ROUTING! No WHERE clause specified for partition column" );
                            } else if ( whereClauseValue != null && !partitionColumnIdentified ) {
                                if ( whereClauseValue.size() == 1 ) {
                                    identPart = (int) partitionManager.getTargetPartitionId( catalogTable, whereClauseValue.get( 0 ) );
                                    worstCaseRouting = false;
                                } else {
                                    worstCaseRouting = true;
                                }
                            }
                            // Since update needs to take current partition and target partition into account
                            //partitionColumnIdentified = false;

                        } else if ( ((LogicalTableModify) node).getOperation() == Operation.INSERT ) {
                            int i;
                            if ( ((LogicalTableModify) node).getInput() instanceof LogicalValues ) {

                                if ( ((LogicalValues) ((LogicalTableModify) node).getInput()).tuples.size() == 1 ) {
                                    for ( i = 0; i < catalogTable.columnIds.size(); i++ ) {
                                        if ( catalogTable.columnIds.get( i ) == catalogTable.partitionColumnId ) {
                                            log.debug( "INSERT: Found PartitionColumnID: '{}' at column index: {}", catalogTable.partitionColumnId, i );
                                            partitionColumnIdentified = true;
                                            worstCaseRouting = false;
                                            partitionValue = ((LogicalValues) ((LogicalTableModify) node).getInput()).tuples.get( 0 ).get( i ).toString().replace( "'", "" );
                                            identPart = (int) partitionManager.getTargetPartitionId( catalogTable, partitionValue );
                                            break;
                                        }
                                    }
                                } else {
                                    worstCaseRouting = true;
                                }
                            } else if ( ((LogicalTableModify) node).getInput() instanceof LogicalProject
                                    && ((LogicalProject) ((LogicalTableModify) node).getInput()).getInput() instanceof LogicalValues ) {

                                String partitionColumnName = catalog.getColumn( catalogTable.partitionColumnId ).name;
                                List<String> fieldNames = ((LogicalTableModify) node).getInput().getRowType().getFieldNames();
                                for ( i = 0; i < fieldNames.size(); i++ ) {
                                    String columnName = fieldNames.get( i );
                                    if ( partitionColumnName.equals( columnName ) ) {
                                        if ( ((LogicalTableModify) node).getInput().getChildExps().get( i ).getKind().equals( SqlKind.DYNAMIC_PARAM ) ) {
                                            worstCaseRouting = true;
                                        } else {
                                            partitionColumnIdentified = true;
                                            partitionValue = ((LogicalTableModify) node).getInput().getChildExps().get( i ).toString().replace( "'", "" );
                                            identPart = (int) partitionManager.getTargetPartitionId( catalogTable, partitionValue );
                                        }
                                        break;
                                    }
                                }
                            } else {
                                worstCaseRouting = true;
                            }

                            if ( log.isDebugEnabled() ) {
                                String partitionColumnName = catalog.getColumn( catalogTable.partitionColumnId ).name;
                                String partitionName = catalog.getPartition( identPart ).partitionName;
                                log.debug( "INSERT: partitionColumn-value: '{}' should be put on partition: {} ({}), which is partitioned with column",
                                        partitionValue, identPart, partitionName, partitionColumnName );
                            }


                        } else if ( ((LogicalTableModify) node).getOperation() == Operation.DELETE ) {
                            if ( whereClauseValue == null ) {
                                worstCaseRouting = true;
                            } else {
                                if ( whereClauseValue.size() >= 2 ) {
                                    worstCaseRouting = true;
                                    partitionColumnIdentified = false;
                                } else {
                                    worstCaseRouting = false;
                                    identPart = (int) partitionManager.getTargetPartitionId( catalogTable, whereClauseValue.get( 0 ) );
                                }
                            }

                        }

                        if ( !worstCaseRouting ) {
                            log.debug( "Get all Placements by identified Partition: {}", identPart );
                            if ( !catalog.getPartitionsOnDataPlacement( pkPlacement.adapterId, pkPlacement.tableId ).contains( identPart ) ) {
                                if ( log.isDebugEnabled() ) {
                                    log.debug( "DataPlacement: {}.{} SKIPPING since it does NOT contain identified partition: '{}' {}",
                                            pkPlacement.adapterUniqueName,
                                            pkPlacement.physicalTableName,
                                            identPart,
                                            catalog.getPartitionsOnDataPlacement( pkPlacement.adapterId, pkPlacement.tableId ) );
                                }
                                continue;
                            } else {
                                if ( log.isDebugEnabled() ) {
                                    log.debug( "DataPlacement: {}.{} contains identified partition: '{}' {}",
                                            pkPlacement.adapterUniqueName,
                                            pkPlacement.physicalTableName,
                                            identPart,
                                            catalog.getPartitionsOnDataPlacement( pkPlacement.adapterId, pkPlacement.tableId ) );
                                }
                            }
                        } else {
                            log.debug( "PartitionColumnID was not an explicit part of statement, partition routing will therefore assume worst-case: Routing to ALL PARTITIONS" );
                        }
                    }

                    // Build DML
                    TableModify modify;
                    RelNode input = buildDml(
                            recursiveCopy( node.getInput( 0 ) ),
                            RelBuilder.create( statement, cluster ),
                            catalogTable,
                            placementsOnAdapter,
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
                if ( modifies.size() == 1 ) {
                    return modifies.get( 0 );
                } else {
                    RelBuilder builder = RelBuilder.create( statement, cluster );
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


    protected RelBuilder buildDml( RelNode node, RelBuilder builder, CatalogTable catalogTable, List<CatalogColumnPlacement> placements, Statement statement, RelOptCluster cluster ) {
        for ( int i = 0; i < node.getInputs().size(); i++ ) {
            buildDml( node.getInput( i ), builder, catalogTable, placements, statement, cluster );
        }

        if ( log.isDebugEnabled() ) {
            log.debug( "List of Store specific ColumnPlacements: " );
            for ( CatalogColumnPlacement ccp : placements ) {
                log.debug( "{}.{}.{}", ccp.adapterUniqueName, ccp.physicalTableName, ccp.getLogicalColumnName() );
            }
        }

        if ( node instanceof LogicalTableScan && node.getTable() != null ) {
            RelOptTableImpl table = (RelOptTableImpl) node.getTable();
            if ( table.getTable() instanceof LogicalTable ) {
                // Special handling for INSERT INTO foo SELECT * FROM foo2
                if ( ((LogicalTable) table.getTable()).getTableId() != catalogTable.id ) {
                    return buildSelect( node, builder, statement, cluster );
                }
                builder = handleTableScan(
                        builder,
                        placements.get( 0 ).tableId,
                        placements.get( 0 ).adapterUniqueName,
                        catalogTable.getSchemaName(),
                        catalogTable.name,
                        placements.get( 0 ).physicalSchemaName,
                        placements.get( 0 ).physicalTableName );
                return builder;
            } else {
                throw new RuntimeException( "Unexpected table. Only logical tables expected here!" );
            }
        } else if ( node instanceof LogicalValues ) {
            builder = handleValues( (LogicalValues) node, builder );
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
                return handleGeneric( node, builder );
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
            return handleGeneric( node, builder );
        } else {
            return handleGeneric( node, builder );
        }
    }


    private void dmlConditionCheck( LogicalFilter node, CatalogTable catalogTable, List<CatalogColumnPlacement> placements, RexNode operand ) {
        if ( operand instanceof RexInputRef ) {
            int index = ((RexInputRef) operand).getIndex();
            RelDataTypeField field = node.getInput().getRowType().getFieldList().get( index );
            CatalogColumn column;
            try {
                column = Catalog.getInstance().getColumn( catalogTable.id, field.getName() );
            } catch ( UnknownColumnException e ) {
                throw new RuntimeException( e );
            }
            if ( !Catalog.getInstance().checkIfExistsColumnPlacement( placements.get( 0 ).adapterId, column.id ) ) {
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


    @Override
    public RelNode buildJoinedTableScan( Statement statement, RelOptCluster cluster, List<CatalogColumnPlacement> placements ) {
        RelBuilder builder = RelBuilder.create( statement, cluster );

        if ( RuntimeConfig.JOINED_TABLE_SCAN_CACHE.getBoolean() ) {
            RelNode cachedNode = joinedTableScanCache.getIfPresent( placements.hashCode() );
            if ( cachedNode != null ) {
                return cachedNode;
            }
        }

        // Sort by adapter
        Map<Integer, List<CatalogColumnPlacement>> placementsByAdapter = new HashMap<>();
        for ( CatalogColumnPlacement placement : placements ) {
            if ( !placementsByAdapter.containsKey( placement.adapterId ) ) {
                placementsByAdapter.put( placement.adapterId, new LinkedList<>() );
            }
            placementsByAdapter.get( placement.adapterId ).add( placement );
        }

        if ( placementsByAdapter.size() == 1 ) {
            List<CatalogColumnPlacement> ccp = placementsByAdapter.values().iterator().next();
            builder = handleTableScan(
                    builder,
                    ccp.get( 0 ).tableId,
                    ccp.get( 0 ).adapterUniqueName,
                    ccp.get( 0 ).getLogicalSchemaName(),
                    ccp.get( 0 ).getLogicalTableName(),
                    ccp.get( 0 ).physicalSchemaName,
                    ccp.get( 0 ).physicalTableName );
            // final project
            ArrayList<RexNode> rexNodes = new ArrayList<>();
            List<CatalogColumnPlacement> placementList = placements.stream()
                    .sorted( Comparator.comparingInt( p -> Catalog.getInstance().getColumn( p.columnId ).position ) )
                    .collect( Collectors.toList() );
            for ( CatalogColumnPlacement catalogColumnPlacement : placementList ) {
                rexNodes.add( builder.field( catalogColumnPlacement.getLogicalColumnName() ) );
            }
            builder.project( rexNodes );
        } else if ( placementsByAdapter.size() > 1 ) {
            // We need to join placements on different adapters

            // Get primary key
            long pkid = catalog.getTable( placements.get( 0 ).tableId ).primaryKey;
            List<Long> pkColumnIds = Catalog.getInstance().getPrimaryKey( pkid ).columnIds;
            List<CatalogColumn> pkColumns = new LinkedList<>();
            for ( long pkColumnId : pkColumnIds ) {
                pkColumns.add( Catalog.getInstance().getColumn( pkColumnId ) );
            }

            // Add primary key
            for ( Entry<Integer, List<CatalogColumnPlacement>> entry : placementsByAdapter.entrySet() ) {
                for ( CatalogColumn pkColumn : pkColumns ) {
                    CatalogColumnPlacement pkPlacement = Catalog.getInstance().getColumnPlacement( entry.getKey(), pkColumn.id );
                    if ( !entry.getValue().contains( pkPlacement ) ) {
                        entry.getValue().add( pkPlacement );
                    }
                }
            }

            Deque<String> queue = new LinkedList<>();
            boolean first = true;
            for ( List<CatalogColumnPlacement> ccps : placementsByAdapter.values() ) {
                handleTableScan(
                        builder,
                        ccps.get( 0 ).tableId,
                        ccps.get( 0 ).adapterUniqueName,
                        ccps.get( 0 ).getLogicalSchemaName(),
                        ccps.get( 0 ).getLogicalTableName(),
                        ccps.get( 0 ).physicalSchemaName,
                        ccps.get( 0 ).physicalTableName );
                if ( first ) {
                    first = false;
                } else {
                    ArrayList<RexNode> rexNodes = new ArrayList<>();
                    for ( CatalogColumnPlacement p : ccps ) {
                        if ( pkColumnIds.contains( p.columnId ) ) {
                            String alias = ccps.get( 0 ).adapterUniqueName + "_" + p.getLogicalColumnName();
                            rexNodes.add( builder.alias( builder.field( p.getLogicalColumnName() ), alias ) );
                            queue.addFirst( alias );
                            queue.addFirst( p.getLogicalColumnName() );
                        } else {
                            rexNodes.add( builder.field( p.getLogicalColumnName() ) );
                        }
                    }
                    builder.project( rexNodes );
                    List<RexNode> joinConditions = new LinkedList<>();
                    for ( int i = 0; i < pkColumnIds.size(); i++ ) {
                        joinConditions.add( builder.call(
                                SqlStdOperatorTable.EQUALS,
                                builder.field( 2, ccps.get( 0 ).getLogicalTableName(), queue.removeFirst() ),
                                builder.field( 2, ccps.get( 0 ).getLogicalTableName(), queue.removeFirst() ) ) );
                    }
                    builder.join( JoinRelType.INNER, joinConditions );

                }
            }
            // final project
            ArrayList<RexNode> rexNodes = new ArrayList<>();
            List<CatalogColumnPlacement> placementList = placements.stream()
                    .sorted( Comparator.comparingInt( p -> Catalog.getInstance().getColumn( p.columnId ).position ) )
                    .collect( Collectors.toList() );
            for ( CatalogColumnPlacement ccp : placementList ) {
                rexNodes.add( builder.field( ccp.getLogicalColumnName() ) );
            }
            builder.project( rexNodes );
        } else {
            throw new RuntimeException( "The table '" + placements.get( 0 ).getLogicalTableName() + "' seems to have no placement. This should not happen!" );
        }
        RelNode node = builder.build();
        if ( RuntimeConfig.JOINED_TABLE_SCAN_CACHE.getBoolean() ) {
            joinedTableScanCache.put( placements.hashCode(), node );
        }
        return node;
    }


    protected RelBuilder handleTableScan(
            RelBuilder builder,
            long tableId,
            String storeUniqueName,
            String logicalSchemaName,
            String logicalTableName,
            String physicalSchemaName,
            String physicalTableName ) {
        if ( selectedAdapter != null ) {
            selectedAdapter.put( tableId, new SelectedAdapterInfo( storeUniqueName, physicalSchemaName, physicalTableName ) );
        }
        return builder.scan( ImmutableList.of(
                PolySchemaBuilder.buildAdapterSchemaName( storeUniqueName, logicalSchemaName, physicalSchemaName ),
                logicalTableName ) );
    }


    protected RelBuilder handleValues( LogicalValues node, RelBuilder builder ) {
        return builder.values( node.tuples, node.getRowType() );
    }


    protected RelBuilder handleGeneric( RelNode node, RelBuilder builder ) {
        if ( node.getInputs().size() == 1 ) {
            builder.replaceTop( node.copy( node.getTraitSet(), ImmutableList.of( builder.peek( 0 ) ) ) );
        } else if ( node.getInputs().size() == 2 ) { // Joins, SetOperations
            builder.replaceTop( node.copy( node.getTraitSet(), ImmutableList.of( builder.peek( 1 ), builder.peek( 0 ) ) ) );
        } else {
            throw new RuntimeException( "Unexpected number of input elements: " + node.getInputs().size() );
        }
        return builder;
    }


    @Override
    public void resetCaches() {
        joinedTableScanCache.invalidateAll();
    }


    @AllArgsConstructor
    @Getter
    private static class SelectedAdapterInfo {

        private final String uniqueName;
        private final String physicalSchemaName;
        private final String physicalTableName;

    }


    private static class WhereClauseVisitor extends RexShuttle {

        private final Statement statement;

        private Object value = null;
        @Getter
        private final List<Object> values = new ArrayList<>();
        private final long partitionColumnIndex;
        @Getter
        private boolean valueIdentified = false;


        public WhereClauseVisitor( Statement statement, long partitionColumnIndex ) {
            super();
            this.statement = statement;
            this.partitionColumnIndex = partitionColumnIndex;
        }


        @Override
        public RexNode visitCall( final RexCall call ) {
            super.visitCall( call );

            if ( call.operands.size() == 2 ) {

                if ( call.operands.get( 0 ) instanceof RexInputRef ) {
                    if ( ((RexInputRef) call.operands.get( 0 )).getIndex() == partitionColumnIndex ) {
                        if ( call.operands.get( 1 ) instanceof RexLiteral ) {
                            value = ((RexLiteral) call.operands.get( 1 )).getValueForQueryParameterizer();
                            values.add( value );
                            valueIdentified = true;
                        } else if ( call.operands.get( 1 ) instanceof RexDynamicParam ) {
                            long index = ((RexDynamicParam) call.operands.get( 1 )).getIndex();
                            value = statement.getDataContext().getParameterValue( index );//.get("?" + index);
                            values.add( value );
                            valueIdentified = true;
                        }
                    }
                } else if ( call.operands.get( 1 ) instanceof RexInputRef ) {

                    if ( ((RexInputRef) call.operands.get( 1 )).getIndex() == partitionColumnIndex ) {
                        if ( call.operands.get( 0 ) instanceof RexLiteral ) {
                            value = ((RexLiteral) call.operands.get( 0 )).getValueForQueryParameterizer();
                            values.add( value );
                            valueIdentified = true;
                        } else if ( call.operands.get( 0 ) instanceof RexDynamicParam ) {
                            long index = ((RexDynamicParam) call.operands.get( 0 )).getIndex();
                            value = statement.getDataContext().getParameterValue( index );//get("?" + index); //.getParameterValues //
                            values.add( value );
                            valueIdentified = true;
                        }
                    }
                }
            }
            return call;
        }

    }

}
