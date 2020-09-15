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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.Adler32;
import java.util.zip.CRC32;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogPartition;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.*;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.partition.PartitionHelper;
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
    protected Map<RelOptTable, SelectedStoreInfo> selectedStores;


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
                table.addRow( k.getQualifiedName(), v.storeName, v.physicalSchemaName + "." + v.physicalTableName );
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
                    //HENNLO
                    //Check if table is even partitoned
                    if ( catalogTable.isPartitioned ) {
                        System.out.println("HENNLO AbstractRouter: buildSelect() TableID: "+ t.getTableId() + " is partitioned on column: "
                                + catalogTable.partitionColumnId + " - " + catalog.getColumn(catalogTable.partitionColumnId).name);
                        System.out.println("HENNLO AbstractRouter: buildSelect() Retrieving all Partitions for table with id: " + catalogTable.id);
                        for (CatalogPartition cp : catalog.getPartitions(catalogTable.id)
                        ) {
                            System.out.println("HENNLO AbstractRouter: " + cp.tableId + " " + (cp.id+1) + "/" + catalogTable.numPartitions);
                        }
                    }
                    else{
                        System.out.println("HENNLO AbstractRouter: " + catalogTable.name + " is NOT partitioned...\n\t\tRouting will be easy");
                    }

                    //
                } catch (UnknownTableException | GenericCatalogException | UnknownColumnException e ) {
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
        PartitionHelper partHelper = new PartitionHelper();


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
                } catch ( GenericCatalogException | UnknownTableException | UnknownColumnException | UnknownKeyException e ) {
                    throw new RuntimeException( e );
                }


                if(catalogTable.isPartitioned) {
                    System.out.println("\nHENNLO AbstractRouter: routeDml(): Listing all relevant stores for table: '" + catalogTable.name
                            + "' and all partitions: " + catalogTable.partitionIds);
                    for (CatalogColumnPlacement dataPlacement : pkPlacements) {
                        //Check
                        System.out.println("\t\t -> '" + dataPlacement.storeUniqueName + "' " +
                                catalog.getPartitionsOnDataPlacement(dataPlacement.storeId, dataPlacement.tableId));
                    }
                }


                // Execute on all primary key placements
                List<TableModify> modifies = new ArrayList<>( pkPlacements.size() );
                for ( CatalogColumnPlacement pkPlacement : pkPlacements ) {
                    CatalogReader catalogReader = statement.getTransaction().getCatalogReader();
                    List<String> tableNames = ImmutableList.of(
                            PolySchemaBuilder.buildStoreSchemaName(
                                    pkPlacement.storeUniqueName,
                                    catalogTable.getSchemaName(),
                                    pkPlacement.physicalSchemaName ),
                            t.getLogicalTableName() );
                    RelOptTable physical = catalogReader.getTableForMember( tableNames );
                    ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );

                    // Get placements on store
                    List<CatalogColumnPlacement> placementsOnStore = catalog.getColumnPlacementsOnStore( pkPlacement.storeId, catalogTable.id );
                    // If this is a update, check whether we need to execute on this store at all
                    List<String> updateColumnList = ((LogicalTableModify) node).getUpdateColumnList();
                    List<RexNode> sourceExpressionList = ((LogicalTableModify) node).getSourceExpressionList();
                    if ( placementsOnStore.size() != catalogTable.columnIds.size() ) {
                        if ( ((LogicalTableModify) node).getOperation() == Operation.UPDATE ) {
                            System.out.println("HENNLO AbstractRouter: routeDML(): columns to be updated:");
                            updateColumnList = new LinkedList<>( ((LogicalTableModify) node).getUpdateColumnList() );
                            sourceExpressionList = new LinkedList<>( ((LogicalTableModify) node).getSourceExpressionList() );
                            Iterator<String> updateColumnListIterator = updateColumnList.iterator();
                            Iterator<RexNode> sourceExpressionListIterator = sourceExpressionList.iterator();
                            while ( updateColumnListIterator.hasNext() ) {
                                String columnName = updateColumnListIterator.next();
                                System.out.println("HENNLO AbstractRouter: routeDML(): column: " + columnName);
                                sourceExpressionListIterator.next();
                                try {
                                    CatalogColumn catalogColumn = catalog.getColumn( catalogTable.id, columnName );
                                    System.out.println("HENNLO AbstractRouter: routeDML(): column: " + catalogColumn.id);
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

                    // Identify where clause of UPDATE

                    //TODO HENNLO This is an rather uncharming workaround
                    if ( catalogTable.isPartitioned ) {
                        String partitionValue ="";
                        //set true if partitionColumn is part of UPDATE Statement, else assume worst case routing
                        boolean partitionColumnIdentified = false;
                        if (((LogicalTableModify) node).getOperation() == Operation.UPDATE) {
                            // In case of update always use worst case routing for now.
                            //Since you have to identify the current partition to delete the entry and then create a new entry on the correct partitions
                            int index = 0;
                            for (String cn : updateColumnList) {
                                try {
                                    System.out.println("HENNLO AbstractRouter: routeDML(): UPDATE: column: " + cn + " " +
                                            catalog.getColumn(catalogTable.id, cn).id);
                                    if (catalog.getColumn(catalogTable.id, cn).id == catalogTable.partitionColumnId) {
                                        System.out.println("HENNLO: AbstractRouter: : routeDML(): UPDATE: Found PartitionColumnID Match: '"
                                                + catalogTable.partitionColumnId + "' at index: " + index);
                                        //Routing/Locking can now be executed on certain partitions
                                        partitionColumnIdentified = true;
                                        partitionValue = sourceExpressionList.get(index).toString().replace("'", "");
                                        System.out.println("HENNLO AbstractRouter: routeDML(): UPDATE: partitionColumn-value: '" + partitionValue + "' should be put on partition: "
                                                + partHelper.getPartitionHash(catalogTable, partitionValue));
                                        break;
                                    }
                                } catch (GenericCatalogException | UnknownColumnException e) {
                                    e.printStackTrace();
                                }
                                index++;
                            }
                            //Since update needs to take current partition and target partition into account
                            //partitionColumnIdentified = false;


                        } else if (((LogicalTableModify) node).getOperation() == Operation.INSERT) {
                            int i;
                            for (i = 0; i < catalogTable.columnIds.size(); i++) {
                                if (catalogTable.columnIds.get(i) == catalogTable.partitionColumnId) {
                                    System.out.println("HENNLO: AbstractRouter: : routeDML(): INSERT: Found PartitionColumnID: '"
                                            + catalogTable.partitionColumnId + "' at column index: " + i);
                                    partitionColumnIdentified = true;
                                    break;
                                }
                            }
                            //TODO Get the value of partitionColumnId ---  but first find if of partitionColumn inside table
                            partitionValue = ((LogicalValues) node.getInput(0)).tuples.get(0).get(i).toString().replace("'", "");
                            System.out.println("HENNLO AbstractRouter: routeDML(): INSERT: partitionColumn-value: '" + partitionValue + "' should be put on partition: "
                                    + partHelper.getPartitionHash(catalogTable, partitionValue));
                        }
                        else if (((LogicalTableModify) node).getOperation() == Operation.DELETE) {
                            System.out.println("HENNLO AbstractRouter: routeDML(): DELETE ");

                            //DELETE will always be worst case routing
                            partitionColumnIdentified = false;
                        }


                        //No worst case routing
                        if ( partitionColumnIdentified ) {

                            //Returns partitionID
                            long identPart = (int) partHelper.getPartitionHash(catalogTable, partitionValue);

                            System.out.println("HENNLO AbstractRouter(): Gather all relevant placements to execute the statement on");
                            System.out.println("HENNLO AbstractRouter(): GET all Placements by identified Partition: " +identPart);
                            /*for (CatalogColumnPlacement partitionPlace: catalog.getColumnPlacementsByPartition(identPart)) {
                                System.out.println("\t\t Statement will be relevant for placement store=" +
                                        partitionPlace.storeUniqueName + " tableName='"+ partitionPlace.physicalTableName + " column: '"+partitionPlace.getLogicalColumnName() + "' by identified Partiton: " + identPart );
                            }*/ //needs try_catch
                            if (!catalog.getPartitionsOnDataPlacement(pkPlacement.storeId, pkPlacement.tableId).contains(identPart) ) {
                                System.out.println("HENNLO AbstractRouter(): DataPacement: " + pkPlacement.storeUniqueName +"."
                                        +pkPlacement.physicalTableName +" SKIPPING since it does NOT contain identified partition: '"+ identPart +"' " +  catalog.getPartitionsOnDataPlacement(pkPlacement.storeId, pkPlacement.tableId));
                                continue;
                            }else{
                                System.out.println("HENNLO AbstractRouter(): DataPacement: " + pkPlacement.storeUniqueName +"."
                                        +pkPlacement.physicalTableName +" contains identified partition: '"+ identPart +"' "+ catalog.getPartitionsOnDataPlacement(pkPlacement.storeId, pkPlacement.tableId));
                            }
                        }
                        else{
                            System.out.println("HENNLO AbstractRouter(): PartitionColumnID was not an explicit part of statement, partition routing will therefore assume worstcase: Routing to ALL PARTITIONS");
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
        System.out.println("\n-->HENNLO buildDml(): List of StoreColumnPlacements: "+ node.getRelTypeName());
        for (CatalogColumnPlacement ccp: placements) {
            System.out.println("HENNLO buildDml(): " + ccp.storeUniqueName+"."+ ccp.physicalTableName + "." +ccp.getLogicalColumnName());
        }
        System.out.println("<--\n");

        if ( node instanceof LogicalTableScan && node.getTable() != null ) {
            RelOptTableImpl table = (RelOptTableImpl) node.getTable();
            if ( table.getTable() instanceof LogicalTable ) {
                // Special handling for INSERT INTO foo SELECT * FROM foo2
                if ( ((LogicalTable) table.getTable()).getTableId() != catalogTable.id ) {
                    return buildSelect( node, builder, statement, cluster );
                }
                builder = handleTableScan(
                        builder,
                        table,
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
            List<CatalogColumn> pkColumns = new LinkedList<>();
            List<Long> pkColumnIds;
            try {
                long pkid = catalog.getTable( placements.get( 0 ).tableId ).primaryKey;
                pkColumnIds = Catalog.getInstance().getPrimaryKey( pkid ).columnIds;
                for ( long pkColumnId : pkColumnIds ) {
                    pkColumns.add( Catalog.getInstance().getColumn( pkColumnId ) );
                }
            } catch ( GenericCatalogException | UnknownTableException | UnknownColumnException | UnknownKeyException e ) {
                throw new RuntimeException( e );
            }

            // Add primary key
            try {
                for ( Entry<Integer, List<CatalogColumnPlacement>> entry : sortedPlacements.entrySet() ) {
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
            for ( CatalogColumnPlacement p : placements ) {
                rexNodes.add( builder.field( p.getLogicalColumnName() ) );
            }
            builder.project( rexNodes );
            return builder;
        }
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
