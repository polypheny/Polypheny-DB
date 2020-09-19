/*
 * Copyright 2019-2020 The Polypheny Project
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
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownKeyException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.prepare.RelOptTableImpl;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.core.JoinRelType;
import org.polypheny.db.rel.core.SetOp;
import org.polypheny.db.rel.core.TableModify;
import org.polypheny.db.rel.core.TableModify.Operation;
import org.polypheny.db.rel.logical.LogicalFilter;
import org.polypheny.db.rel.logical.LogicalModifyCollect;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rel.logical.LogicalTableModify;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.logical.LogicalValues;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.routing.Router;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.PolySchemaBuilder;
import org.polypheny.db.schema.Table;
import org.polypheny.db.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.transaction.Statement;

public abstract class AbstractRouter implements Router {

    protected ExecutionTimeMonitor executionTimeMonitor;

    protected InformationPage page = null;

    final Catalog catalog = Catalog.getInstance();

    // For reporting purposes
    protected Map<Long, SelectedStoreInfo> selectedStores;


    @Override
    public RelRoot route( RelRoot logicalRoot, Statement statement, ExecutionTimeMonitor executionTimeMonitor ) {
        this.executionTimeMonitor = executionTimeMonitor;
        this.selectedStores = new HashMap<>();

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
        } else {
            RelBuilder builder = RelBuilder.create( statement, logicalRoot.rel.getCluster() );
            builder = buildDql( logicalRoot.rel, builder, statement, logicalRoot.rel.getCluster() );
            routed = builder.build();
        }

        wrapUp( statement, routed );

        // Add information to query analyzer
        if ( statement.getTransaction().isAnalyze() ) {
            InformationGroup group = new InformationGroup( page, "Selected Stores" );
            statement.getTransaction().getQueryAnalyzer().addGroup( group );
            InformationTable table = new InformationTable(
                    group,
                    ImmutableList.of( "Table", "Store", "Physical Name" ) );
            selectedStores.forEach( ( k, v ) -> {
                try {
                    CatalogTable catalogTable = Catalog.getInstance().getTable( k );
                    table.addRow( catalogTable.getSchemaName() + "." + catalogTable.name, v.storeName, v.physicalSchemaName + "." + v.physicalTableName );
                } catch ( UnknownTableException | GenericCatalogException e ) {
                    throw new RuntimeException( e );
                }
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
            buildDql( node.getInput( i ), builder, statement, cluster );
        }
        if ( node instanceof LogicalTableScan && node.getTable() != null ) {
            RelOptTableImpl table = (RelOptTableImpl) node.getTable();
            if ( table.getTable() instanceof LogicalTable ) {
                LogicalTable t = ((LogicalTable) table.getTable());
                CatalogTable catalogTable;
                try {
                    catalogTable = Catalog.getInstance().getTable( t.getTableId() );
                } catch ( UnknownTableException | GenericCatalogException e ) {
                    throw new RuntimeException( "Unknown table" );
                }
                List<CatalogColumnPlacement> placements = selectPlacement( node, catalogTable );
                return buildJoinedTableScan( builder, placements );
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


    // Default implementation: Execute DML on all placements
    protected RelNode routeDml( RelNode node, Statement statement ) {
        RelOptCluster cluster = node.getCluster();
        if ( node.getTable() != null ) {
            RelOptTableImpl table = (RelOptTableImpl) node.getTable();
            if ( table.getTable() instanceof LogicalTable ) {
                LogicalTable t = ((LogicalTable) table.getTable());
                // Get placements of this table
                CatalogTable catalogTable;
                List<CatalogColumnPlacement> pkPlacements;
                try {
                    catalogTable = catalog.getTable( t.getTableId() );
                    long pkid = catalogTable.primaryKey;
                    List<Long> pkColumnIds = Catalog.getInstance().getPrimaryKey( pkid ).columnIds;
                    CatalogColumn pkColumn = Catalog.getInstance().getColumn( pkColumnIds.get( 0 ) );
                    pkPlacements = catalog.getColumnPlacements( pkColumn.id );
                } catch ( GenericCatalogException | UnknownTableException | UnknownKeyException e ) {
                    throw new RuntimeException( e );
                }

                // Execute on all primary key placements
                List<TableModify> modifies = new ArrayList<>( pkPlacements.size() );
                for ( CatalogColumnPlacement pkPlacement : pkPlacements ) {
                    CatalogReader catalogReader = statement.getTransaction().getCatalogReader();

                    List<String> qualifiedTableName = ImmutableList.of(
                            PolySchemaBuilder.buildStoreSchemaName(
                                    pkPlacement.storeUniqueName,
                                    catalogTable.getSchemaName(),
                                    pkPlacement.physicalSchemaName ),
                            t.getLogicalTableName() );
                    RelOptTable physical = catalogReader.getTableForMember( qualifiedTableName );
                    ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );

                    // Get placements on store
                    List<CatalogColumnPlacement> placementsOnStore = catalog.getColumnPlacementsOnStore( pkPlacement.storeId, catalogTable.id );

                    // If this is a update, check whether we need to execute on this store at all
                    List<String> updateColumnList = ((LogicalTableModify) node).getUpdateColumnList();
                    List<RexNode> sourceExpressionList = ((LogicalTableModify) node).getSourceExpressionList();
                    if ( placementsOnStore.size() != catalogTable.columnIds.size() ) {
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
                                    if ( !catalog.checkIfExistsColumnPlacement( pkPlacement.storeId, catalogColumn.id ) ) {
                                        updateColumnListIterator.remove();
                                        sourceExpressionListIterator.remove();
                                    }
                                } catch ( GenericCatalogException | UnknownColumnException e ) {
                                    throw new RuntimeException( e );
                                }
                            }
                            if ( updateColumnList.size() == 0 ) {
                                continue;
                            }
                        }
                    }

                    // Build DML
                    TableModify modify;
                    RelNode input = buildDml(
                            recursiveCopy( node.getInput( 0 ) ),
                            RelBuilder.create( statement, cluster ),
                            catalogTable,
                            placementsOnStore,
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
                        placements.get( 0 ).storeUniqueName,
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
        } else if ( node instanceof LogicalFilter ) {
            if ( catalogTable.columnIds.size() != placements.size() ) { // partitioned, check if there is a illegal condition
                RexCall call = ((RexCall) ((LogicalFilter) node).getCondition());
                for ( RexNode operand : call.operands ) {
                    if ( operand instanceof RexInputRef ) {
                        int index = ((RexInputRef) operand).getIndex();
                        RelDataTypeField field = ((LogicalFilter) node).getInput().getRowType().getFieldList().get( index );
                        CatalogColumn column;
                        try {
                            column = Catalog.getInstance().getColumn( catalogTable.id, field.getName() );
                        } catch ( GenericCatalogException | UnknownColumnException e ) {
                            throw new RuntimeException( e );
                        }
                        if ( !Catalog.getInstance().checkIfExistsColumnPlacement( placements.get( 0 ).storeId, column.id ) ) {
                            throw new RuntimeException( "Current implementation of vertical partitioning does not allow conditions on partitioned columns. " );
                            // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                            // TODO: Use indexes
                        }
                    }
                }
            }
            return handleGeneric( node, builder );
        } else {
            return handleGeneric( node, builder );
        }
    }


    public RelBuilder buildJoinedTableScan( RelBuilder builder, List<CatalogColumnPlacement> placements ) {
        // Sort by store
        Map<Integer, List<CatalogColumnPlacement>> placementsByStore = new HashMap<>();
        for ( CatalogColumnPlacement placement : placements ) {
            if ( !placementsByStore.containsKey( placement.storeId ) ) {
                placementsByStore.put( placement.storeId, new LinkedList<>() );
            }
            placementsByStore.get( placement.storeId ).add( placement );
        }

        if ( placementsByStore.size() == 1 ) {
            List<CatalogColumnPlacement> ccp = placementsByStore.values().iterator().next();
            builder = handleTableScan(
                    builder,
                    ccp.get( 0 ).tableId,
                    ccp.get( 0 ).storeUniqueName,
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
            return builder;
        } else {
            // We need to join placements on different stores

            // Get primary key
            List<CatalogColumn> pkColumns = new LinkedList<>();
            List<Long> pkColumnIds;
            try {
                long pkid = catalog.getTable( placements.get( 0 ).tableId ).primaryKey;
                pkColumnIds = Catalog.getInstance().getPrimaryKey( pkid ).columnIds;
                for ( long pkColumnId : pkColumnIds ) {
                    pkColumns.add( Catalog.getInstance().getColumn( pkColumnId ) );
                }
            } catch ( GenericCatalogException | UnknownTableException | UnknownKeyException e ) {
                throw new RuntimeException( e );
            }

            // Add primary key
            try {
                for ( Entry<Integer, List<CatalogColumnPlacement>> entry : placementsByStore.entrySet() ) {
                    for ( CatalogColumn pkColumn : pkColumns ) {
                        CatalogColumnPlacement pkPlacement = Catalog.getInstance().getColumnPlacement( entry.getKey(), pkColumn.id );
                        if ( !entry.getValue().contains( pkPlacement ) ) {
                            entry.getValue().add( pkPlacement );
                        }
                    }
                }
            } catch ( GenericCatalogException e ) {
                throw new RuntimeException( e );
            }

            Deque<String> queue = new LinkedList<>();
            boolean first = true;
            for ( List<CatalogColumnPlacement> ccps : placementsByStore.values() ) {
                handleTableScan(
                        builder,
                        ccps.get( 0 ).tableId,
                        ccps.get( 0 ).storeUniqueName,
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
                            String alias = ccps.get( 0 ).storeUniqueName + "_" + p.getLogicalColumnName();
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
            return builder;
        }
    }


    protected RelBuilder handleTableScan(
            RelBuilder builder,
            long tableId,
            String storeUniqueName,
            String logicalSchemaName,
            String logicalTableName,
            String physicalSchemaName,
            String physicalTableName ) {
        if ( selectedStores != null ) {
            selectedStores.put( tableId, new SelectedStoreInfo( storeUniqueName, physicalSchemaName, physicalTableName ) );
        }
        return builder.scan( ImmutableList.of(
                PolySchemaBuilder.buildStoreSchemaName( storeUniqueName, logicalSchemaName, physicalSchemaName ),
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


    @AllArgsConstructor
    @Getter
    private static class SelectedStoreInfo {

        private final String storeName;
        private final String physicalSchemaName;
        private final String physicalTableName;
    }

}
