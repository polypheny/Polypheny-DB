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

package org.polypheny.db.replication;


import com.google.common.collect.ImmutableList;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.AlgStructuredTypeFlattener;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.TableModify.Operation;
import org.polypheny.db.algebra.logical.LogicalValues;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.ReplicationStrategy;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogDataPlacement;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.replication.cdc.ChangeDataReplicationObject;
import org.polypheny.db.replication.cdc.DeleteReplicationObject;
import org.polypheny.db.replication.cdc.InsertReplicationObject;
import org.polypheny.db.replication.cdc.UpdateReplicationObject;
import org.polypheny.db.replication.properties.exception.OutdatedReplicationException;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.PolySchemaBuilder;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.util.Pair;


@Slf4j
public class DataReplicatorImpl implements DataReplicator {

    private Catalog catalog = Catalog.getInstance();

    // TODO @HENNLO TestCase Stop active replication, execute insert, change will be captured, remove adapter physically
    // enable replication, it should now not be able to find the adapter.

    // TODO @HENNLO TestCase When we delete placements, adapters or partitions we need to drop the pending replications


    @Override
    public long replicateData( Transaction transaction, ChangeDataReplicationObject dataReplicationObject, long replicationId ) throws OutdatedReplicationException {

        Pair<Long, Integer> targetPartitionPlacementIdentifier = dataReplicationObject.getDependentReplicationIds().get( replicationId );

        long tableId = dataReplicationObject.getTableId();
        long targetPartitionId = targetPartitionPlacementIdentifier.left;
        int targetAdapterId = targetPartitionPlacementIdentifier.right;

        CatalogDataPlacement dataPlacement = catalog.getDataPlacement( targetAdapterId, tableId );
        CatalogPartitionPlacement targetPartitionPlacement = catalog.getPartitionPlacement( targetAdapterId, targetPartitionId );

        // Verify that this placement is indeed Outdated && has not received this update yet && is indeed considered to receive lazy replications
        if ( !dataPlacement.replicationStrategy.equals( ReplicationStrategy.EAGER ) &&
                targetPartitionPlacement.updateInformation.commitTimestamp <= dataReplicationObject.getCommitTimestamp() ) {

            Statement targetStatement = transaction.createStatement();

            AlgRoot targetAlg;
            switch ( dataReplicationObject.getOperation() ) {

                case INSERT:
                    targetAlg = buildInsertStatement( targetStatement, (InsertReplicationObject) dataReplicationObject, dataPlacement, targetPartitionPlacement );
                    break;

                case UPDATE:
                    targetAlg = buildUpdateStatement( targetStatement, (UpdateReplicationObject) dataReplicationObject, dataPlacement, targetPartitionPlacement );
                    break;

                case DELETE:
                    targetAlg = buildDeleteStatement( targetStatement, (DeleteReplicationObject) dataReplicationObject, dataPlacement, targetPartitionPlacement );
                    break;

                default:
                    throw new RuntimeException( "Unsupported Operation" );
            }
            return executeQuery( targetStatement, targetAlg, dataReplicationObject );
        }
        log.warn( "Received an outdated replication for object that is already on a newer state. This might happen if there were still pending transactions after manual refresh." );
        log.warn( "Current timestamp '{}' is greater than timestamp: '{}' of new replication."
                , new Timestamp( targetPartitionPlacement.updateInformation.commitTimestamp )
                , new Timestamp( dataReplicationObject.getCommitTimestamp() ) );

        throw new OutdatedReplicationException();
    }


    @Override
    public AlgRoot buildInsertStatement( Statement statement, InsertReplicationObject dataReplicationObject, CatalogDataPlacement dataPlacement, CatalogPartitionPlacement targetPartitionPlacement ) {

        CatalogTable catalogTable = catalog.getTable( dataPlacement.tableId );

        List<String> qualifiedTableName = ImmutableList.of(
                PolySchemaBuilder.buildAdapterSchemaName(
                        targetPartitionPlacement.adapterUniqueName,
                        dataPlacement.getLogicalSchemaName(),
                        targetPartitionPlacement.physicalSchemaName ),
                dataPlacement.getLogicalTableName() + "_" + targetPartitionPlacement.partitionId );
        AlgOptTable physical = statement.getTransaction().getCatalogReader().getTableForMember( qualifiedTableName );
        ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );

        AlgOptCluster cluster = AlgOptCluster.create(
                statement.getQueryProcessor().getPlanner(),
                new RexBuilder( statement.getTransaction().getTypeFactory() ) );
        AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );

        List<String> columnNames = catalogTable.getColumnNames();
        List<RexNode> values = new LinkedList<>();

        int columnIndex = 0;
        for ( long columnId : catalogTable.columnIds ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( columnId );
            values.add( new RexDynamicParam( catalogColumn.getAlgDataType( typeFactory ), columnIndex ) );//(int) catalogColumn.id ) );
            columnIndex++;
        }

        AlgBuilder builder = AlgBuilder.create( statement, cluster );
        builder.push( LogicalValues.createOneRow( cluster ) );
        builder.project( values, columnNames );

        AlgNode node = modifiableTable.toModificationAlg(
                cluster,
                physical,
                statement.getTransaction().getCatalogReader(),
                builder.build(),
                Operation.INSERT,
                null,
                null,
                true
        );

        return AlgRoot.of( node, Kind.INSERT );
    }


    @Override
    public AlgRoot buildUpdateStatement( Statement statement, UpdateReplicationObject dataReplicationObject, CatalogDataPlacement dataPlacement, CatalogPartitionPlacement targetPartitionPlacement ) {

        CatalogTable catalogTable = catalog.getTable( dataPlacement.tableId );

        List<String> qualifiedTableName = ImmutableList.of(
                PolySchemaBuilder.buildAdapterSchemaName(
                        targetPartitionPlacement.adapterUniqueName,
                        dataPlacement.getLogicalSchemaName(),
                        targetPartitionPlacement.physicalSchemaName ),
                dataPlacement.getLogicalTableName() + "_" + targetPartitionPlacement.partitionId );
        AlgOptTable physical = statement.getTransaction().getCatalogReader().getTableForMember( qualifiedTableName );
        ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );

        AlgOptCluster cluster = AlgOptCluster.create(
                statement.getQueryProcessor().getPlanner(),
                new RexBuilder( statement.getTransaction().getTypeFactory() ) );
        AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );

        AlgBuilder builder = AlgBuilder.create( statement, cluster );
        builder.scan( qualifiedTableName );

        RexNode capturedCondition = dataReplicationObject.getCondition();
        // build condition
        RexNode condition = null;
        CatalogPrimaryKey primaryKey = Catalog.getInstance().getPrimaryKey( catalogTable.primaryKey );
        int columnIndex1 = 0;
        for ( long cid : primaryKey.columnIds ) {
            CatalogColumnPlacement ccp = Catalog.getInstance().getColumnPlacement( dataPlacement.adapterId, cid );
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( cid );
            RexNode c = builder.equals(
                    builder.field( ccp.getLogicalColumnName() ),
                    new RexDynamicParam( catalogColumn.getAlgDataType( typeFactory ), columnIndex1 )
            );
            if ( condition == null ) {
                condition = c;
            } else {
                condition = builder.and( condition, c );
            }
            columnIndex1++;
        }
        builder = builder.filter( condition );

        List<String> columnNames = catalogTable.getColumnNames();
        List<RexNode> values = new LinkedList<>();

        int columnIndex = 0;
        for ( long columnId : catalogTable.columnIds ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( columnId );
            values.add( new RexDynamicParam( catalogColumn.getAlgDataType( typeFactory ), columnIndex ) );//(int) catalogColumn.id ) );
            columnIndex++;
        }

        builder.projectPlus( values );

        AlgNode node = modifiableTable.toModificationAlg(
                cluster,
                physical,
                statement.getTransaction().getCatalogReader(),
                builder.build(),
                Operation.UPDATE,
                columnNames,
                values,
                false
        );
        AlgRoot algRoot = AlgRoot.of( node, Kind.UPDATE );
        AlgStructuredTypeFlattener typeFlattener = new AlgStructuredTypeFlattener(
                AlgBuilder.create( statement, algRoot.alg.getCluster() ),
                algRoot.alg.getCluster().getRexBuilder(),
                algRoot.alg::getCluster,
                true );
        return algRoot.withAlg( typeFlattener.rewrite( algRoot.alg ) );
    }


    @Override
    public AlgRoot buildDeleteStatement( Statement statement, DeleteReplicationObject dataReplicationObject, CatalogDataPlacement dataPlacement, CatalogPartitionPlacement targetPartitionPlacement ) {
        List<String> qualifiedTableName = ImmutableList.of(
                PolySchemaBuilder.buildAdapterSchemaName(
                        targetPartitionPlacement.adapterUniqueName,
                        dataPlacement.getLogicalSchemaName(),
                        targetPartitionPlacement.physicalSchemaName ),
                dataPlacement.getLogicalTableName() + "_" + targetPartitionPlacement.partitionId );
        AlgOptTable physical = statement.getTransaction().getCatalogReader().getTableForMember( qualifiedTableName );
        ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );

        CatalogTable catalogTable = catalog.getTable( dataPlacement.tableId );

        AlgOptCluster cluster = AlgOptCluster.create(
                statement.getQueryProcessor().getPlanner(),
                new RexBuilder( statement.getTransaction().getTypeFactory() ) );
        AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );

        List<String> columnNames = catalogTable.getColumnNames();
        List<RexNode> values = new LinkedList<>();

        int columnIndex = 0;
        for ( long columnId : catalogTable.columnIds ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( columnId );
            values.add( new RexDynamicParam( catalogColumn.getAlgDataType( typeFactory ), columnIndex ) );//(int) catalogColumn.id ) );
            columnIndex++;
        }

        AlgBuilder builder = AlgBuilder.create( statement, cluster );
        builder.push( LogicalValues.createOneRow( cluster ) );
        builder.project( values, columnNames );

        AlgNode node = modifiableTable.toModificationAlg(
                cluster,
                physical,
                statement.getTransaction().getCatalogReader(),
                builder.build(),
                Operation.DELETE,
                null,
                null,
                true
        );

        return AlgRoot.of( node, Kind.DELETE );
    }


    private long executeQuery( Statement targetStatement, AlgRoot targetAlg, ChangeDataReplicationObject dataReplicationObject ) {

        List<AlgDataTypeField> fields = targetAlg.alg.getTable().getRowType().getFieldList();

        targetStatement.getDataContext().setParameterValues( dataReplicationObject.getParameterValues() );
        targetStatement.getDataContext().setParameterTypes( dataReplicationObject.getParameterTypes() );

        log.info( "Executing Replication Statement" );
        Iterator<?> iterator = targetStatement.getQueryProcessor()
                .prepareQuery( targetAlg, targetAlg.validatedRowType, true, false, false )
                .enumerable( targetStatement.getDataContext() )
                .iterator();
        long modifications = 0;
        while ( iterator.hasNext() ) {
            modifications++;
            iterator.next();
        }
        targetStatement.getDataContext().resetParameterValues();
        log.info( "Finished Replication" );
        return modifications;
    }

}
