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
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import org.polypheny.db.rel.core.TableModify;
import org.polypheny.db.rel.logical.LogicalModifyCollect;
import org.polypheny.db.rel.logical.LogicalTableModify;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.logical.LogicalValues;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.routing.Router;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.PolySchemaBuilder;
import org.polypheny.db.schema.Table;
import org.polypheny.db.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.transaction.Transaction;

public abstract class AbstractRouter implements Router {

    protected ExecutionTimeMonitor executionTimeMonitor;

    protected InformationPage page = null;

    final Catalog catalog = Catalog.getInstance();

    // For reporting purposes
    protected Map<RelOptTable, SelectedStoreInfo> selectedStores;


    @Override
    public RelRoot route( RelRoot logicalRoot, Transaction transaction, ExecutionTimeMonitor executionTimeMonitor ) {
        this.executionTimeMonitor = executionTimeMonitor;
        this.selectedStores = new HashMap<>();

        if ( transaction.isAnalyze() ) {
            InformationManager queryAnalyzer = transaction.getQueryAnalyzer();
            page = new InformationPage( "Routing" );
            page.fullWidth();
            queryAnalyzer.addPage( page );
        }

        RelNode routed;
        analyze( transaction, logicalRoot );
        if ( logicalRoot.rel instanceof LogicalTableModify ) {
            routed = routeDml( logicalRoot.rel, transaction );
        } else {
            RelBuilder builder = RelBuilder.create( transaction, logicalRoot.rel.getCluster() );
            builder = buildSelect( logicalRoot.rel, builder, transaction );
            routed = builder.build();
        }

        wrapUp( transaction, routed );

        // Add information to query analyzer
        if ( transaction.isAnalyze() ) {
            InformationGroup group = new InformationGroup( page, "Selected Stores" );
            transaction.getQueryAnalyzer().addGroup( group );
            InformationTable table = new InformationTable(
                    group,
                    ImmutableList.of( "Table", "Store", "Physical Name" ) );
            selectedStores.forEach( ( k, v ) -> {
                table.addRow( k.getQualifiedName(), v.storeName, v.physicalSchemaName + "." + v.physicalTableName );
            } );
            transaction.getQueryAnalyzer().registerInformation( table );
        }

        return new RelRoot(
                routed,
                logicalRoot.validatedRowType,
                logicalRoot.kind,
                logicalRoot.fields,
                logicalRoot.collation );
    }


    protected abstract void analyze( Transaction transaction, RelRoot logicalRoot );

    protected abstract void wrapUp( Transaction transaction, RelNode routed );

    // Select the placement on which a table scan should be executed
    protected abstract List<CatalogColumnPlacement> selectPlacement( RelNode node, CatalogTable catalogTable );


    protected RelBuilder buildSelect( RelNode node, RelBuilder builder, Transaction transaction ) {
        for ( int i = 0; i < node.getInputs().size(); i++ ) {
            buildSelect( node.getInput( i ), builder, transaction );
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
                return buildJoinedTableScan( builder, table, placements );
            } else {
                throw new RuntimeException( "Unexpected table. Only logical tables expected here!" );
            }
        } else if ( node instanceof LogicalValues ) {
            return handleValues( (LogicalValues) node, builder );
        } else {
            return handleGeneric( node, builder );
        }
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
    protected RelNode routeDml( RelNode node, Transaction transaction ) {
        RelOptCluster cluster = node.getCluster();
        if ( node.getTable() != null ) {
            RelOptTableImpl table = (RelOptTableImpl) node.getTable();
            if ( table.getTable() instanceof LogicalTable ) {
                LogicalTable t = ((LogicalTable) table.getTable());
                // Get placements of this table
                // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                // TODO: This assumes there are only full table placements !!!!!!!!!!!!!!!!!!
                List<CatalogColumnPlacement> placements = catalog.getColumnPlacements( t.getColumnIds().get( 0 ) );

                // Execute on all placements
                List<TableModify> modifies = new ArrayList<>( placements.size() );
                for ( CatalogColumnPlacement placement : placements ) {

                    CatalogReader catalogReader = transaction.getCatalogReader();
                    CatalogTable catalogTable;
                    try {
                        catalogTable = catalog.getTable( placement.tableId );
                    } catch ( UnknownTableException | GenericCatalogException e ) {
                        // This should not happen
                        throw new RuntimeException( e );
                    }

                    List<String> tableNames = ImmutableList.of(
                            PolySchemaBuilder.buildStoreSchemaName(
                                    placement.storeUniqueName,
                                    catalogTable.getSchemaName(),
                                    placement.physicalSchemaName ),
                            t.getLogicalTableName() );
                    RelOptTable physical = catalogReader.getTableForMember( tableNames );
                    ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );
                    TableModify modify;
                    RelNode input = buildDml(
                            recursiveCopy( node.getInput( 0 ) ),
                            RelBuilder.create( transaction, cluster ),
                            placement ).build();
                    if ( modifiableTable != null && modifiableTable == physical.unwrap( Table.class ) ) {
                        modify = modifiableTable.toModificationRel(
                                cluster,
                                physical,
                                catalogReader,
                                input,
                                ((LogicalTableModify) node).getOperation(),
                                ((LogicalTableModify) node).getUpdateColumnList(),
                                ((LogicalTableModify) node).getSourceExpressionList(),
                                ((LogicalTableModify) node).isFlattened()
                        );
                    } else {
                        modify = LogicalTableModify.create(
                                physical,
                                catalogReader,
                                input,
                                ((LogicalTableModify) node).getOperation(),
                                ((LogicalTableModify) node).getUpdateColumnList(),
                                ((LogicalTableModify) node).getSourceExpressionList(),
                                ((LogicalTableModify) node).isFlattened()
                        );
                    }
                    modifies.add( modify );
                }
                if ( modifies.size() == 1 ) {
                    return modifies.get( 0 );
                } else {
                    RelBuilder builder = RelBuilder.create( transaction, cluster );
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


    protected RelBuilder buildDml( RelNode node, RelBuilder builder, CatalogColumnPlacement placement ) {
        for ( int i = 0; i < node.getInputs().size(); i++ ) {
            buildDml( node.getInput( i ), builder, placement );
        }
        if ( node instanceof LogicalTableScan && node.getTable() != null ) {
            RelOptTableImpl table = (RelOptTableImpl) node.getTable();
            if ( table.getTable() instanceof LogicalTable ) {
                return handleTableScan( builder, table, placement );
            } else {
                throw new RuntimeException( "Unexpected table. Only logical tables expected here!" );
            }
        } else if ( node instanceof LogicalValues ) {
            return handleValues( (LogicalValues) node, builder );
        } else {
            return handleGeneric( node, builder );
        }
    }


    protected RelBuilder buildJoinedTableScan( RelBuilder builder, RelOptTableImpl table, List<CatalogColumnPlacement> placements ) {
        // Sort by store
        Map<Integer, List<CatalogColumnPlacement>> sortedPlacements = new HashMap<>();
        for ( CatalogColumnPlacement placement : placements ) {
            if ( !sortedPlacements.containsKey( placement.storeId ) ) {
                sortedPlacements.put( placement.storeId, new LinkedList<>() );
            }
            sortedPlacements.get( placement.storeId ).add( placement );
        }

        if ( sortedPlacements.size() == 1 ) {
            List<CatalogColumnPlacement> ccp = sortedPlacements.values().iterator().next();
            return handleTableScan(
                    builder,
                    table,
                    ccp.get( 0 ).storeUniqueName,
                    ((LogicalTable) table.getTable()).getLogicalSchemaName(),
                    ((LogicalTable) table.getTable()).getLogicalTableName(),
                    ccp.get( 0 ).physicalSchemaName,
                    ccp.get( 0 ).physicalTableName );
        } else {
            // We need to join placements on different stores

            // Get primary key
            CatalogColumn pkColumn;
            try {
                long pkid = catalog.getTable( placements.get( 0 ).tableId ).primaryKey;
                List<Long> pkColumnIds = Catalog.getInstance().getPrimaryKey( pkid ).columnIds;
                if ( pkColumnIds.size() != 1 ) {
                    throw new RuntimeException( "Vertical portioning is not supported for tables with composite primary keys!" );
                }
                pkColumn = Catalog.getInstance().getColumn( pkColumnIds.get( 0 ) );
            } catch ( GenericCatalogException | UnknownTableException | UnknownColumnException | UnknownKeyException e ) {
                throw new RuntimeException( e );
            }

            // Add primary key
            try {
                for ( Entry<Integer, List<CatalogColumnPlacement>> entry : sortedPlacements.entrySet() ) {
                    CatalogColumnPlacement pkPlacement = Catalog.getInstance().getColumnPlacement( entry.getKey(), pkColumn.id );
                    if ( !entry.getValue().contains( pkPlacement ) ) {
                        entry.getValue().add( pkPlacement );
                    }
                }
            } catch ( GenericCatalogException e ) {
                throw new RuntimeException( e );
            }

            Deque<String> queue = new LinkedList<>();
            boolean first = true;
            for ( List<CatalogColumnPlacement> ccps : sortedPlacements.values() ) {
                handleTableScan(
                        builder,
                        table,
                        ccps.get( 0 ).storeUniqueName,
                        ((LogicalTable) table.getTable()).getLogicalSchemaName(),
                        ((LogicalTable) table.getTable()).getLogicalTableName(),
                        ccps.get( 0 ).physicalSchemaName,
                        ccps.get( 0 ).physicalTableName );
                if ( first ) {
                    first = false;
                } else {
                    queue.addFirst( ccps.get( 0 ).storeUniqueName + "_" + pkColumn.name );
                    ArrayList<RexNode> rexNodes = new ArrayList<>();
                    for ( CatalogColumnPlacement p : ccps ) {
                        if ( p.columnId == pkColumn.id ) {
                            rexNodes.add( builder.alias(
                                    builder.field( p.getLogicalColumnName() ),
                                    ccps.get( 0 ).storeUniqueName + "_" + p.getLogicalColumnName() ) );
                        } else {
                            rexNodes.add( builder.field( p.getLogicalColumnName() ) );
                        }
                    }
                    builder.project( rexNodes );
                    builder.join(
                            JoinRelType.INNER,
                            builder.call(
                                    SqlStdOperatorTable.EQUALS,
                                    builder.field( 2, pkColumn.getTableName(), queue.removeFirst() ),
                                    builder.field( 2, pkColumn.getTableName(), pkColumn.name ) ) );
                }
            }
            // final project
            ArrayList<RexNode> rexNodes = new ArrayList<>();
            for ( CatalogColumnPlacement p : placements ) {
                rexNodes.add( builder.field( p.getLogicalColumnName() ) );
            }
            builder.project( rexNodes );
            return builder;
        }
    }


    protected RelBuilder handleTableScan( RelBuilder builder, RelOptTableImpl table, CatalogColumnPlacement placement ) {
        return handleTableScan(
                builder,
                table,
                placement.storeUniqueName,
                ((LogicalTable) table.getTable()).getLogicalSchemaName(),
                ((LogicalTable) table.getTable()).getLogicalTableName(),
                placement.physicalSchemaName,
                placement.physicalTableName );
    }


    protected RelBuilder handleTableScan(
            RelBuilder builder,
            RelOptTableImpl table,
            String storeUniqueName,
            String logicalSchemaName,
            String logicalTableName,
            String physicalSchemaName,
            String physicalTableName ) {
        selectedStores.put( table, new SelectedStoreInfo( storeUniqueName, physicalSchemaName, physicalTableName ) );
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
