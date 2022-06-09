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


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import lombok.NonNull;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.ReplicationStrategy;
import org.polypheny.db.catalog.entity.CatalogDataPlacement;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.config.ConfigBoolean;
import org.polypheny.db.config.ConfigEnum;
import org.polypheny.db.config.ConfigInteger;
import org.polypheny.db.config.ConfigManager;
import org.polypheny.db.config.WebUiGroup;
import org.polypheny.db.config.WebUiPage;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.replication.cdc.ChangeDataCaptureObject;
import org.polypheny.db.replication.cdc.ChangeDataReplicationObject;
import org.polypheny.db.replication.cdc.DeleteReplicationObject;
import org.polypheny.db.replication.cdc.InsertReplicationObject;
import org.polypheny.db.replication.cdc.UpdateReplicationObject;
import org.polypheny.db.util.Pair;


public abstract class ReplicationEngine {


    public static final ConfigBoolean CAPTURE_DATA_MODIFICATIONS = new ConfigBoolean(
            "replication/captureDataModification",
            "If disabled no data modification is actively being captured. "
                    + "To refresh outdated placements a manual refresh operation needs to be executed. "
                    + "No additional memory requirements are necessary since changes are no longer cached.",
            true );

    public static final ConfigInteger REPLICATION_FAIL_COUNT_THRESHOLD = new ConfigInteger(
            "replication/failCountThreshold",
            "Threshold to determine how often a replication can be re-initiated before marking it as failed. "
                    + "When this threshold is surpassed the target placement is marked as INFINITELY OUTDATED and cannot receive "
                    + "any further replications. The refresh operation has to be triggered manually.",
            3 );

    // TODO @HENNLO Think about adding MapDB Storage of replications / Usage of WALs or any other store.
    public static final ConfigEnum REPLICATION_OBJECT_STORAGE = new ConfigEnum(
            "replication/replicationObjectStorage",
            "Determines whether the captured replication objects should be solely held in memory or persisted to available engines.",
            ReplicationObjectStorageLocation.class,
            ReplicationObjectStorageLocation.MEMORY );

    protected Catalog catalog = Catalog.getInstance();

    protected HashMap<Long, Integer> replicationFailCount = new HashMap<>();

    protected DataReplicator dataReplicator;

    private ReplicationStrategy associatedStrategy;

    // Config UI
    protected final WebUiPage replicationSettingsPage;
    protected final ConfigManager configManager = ConfigManager.getInstance();

    // Monitoring UI
    protected InformationManager im = InformationManager.getInstance();
    protected final InformationPage globalInformationPage;


    // Only needed to track changes on uniquely identifiable replicationData
    private static final AtomicLong replicationDataIdBuilder = new AtomicLong( 1 );

    // Used to track unique replication tasks
    private static final AtomicLong replicationIdBuilder = new AtomicLong( 1 );


    protected ReplicationEngine() {

        this.associatedStrategy = getAssociatedReplicationStrategy();

        // Config UI
        this.replicationSettingsPage = new WebUiPage(
                "replicationSettings",
                "Data Replication",
                "Settings for data replication." );

        final WebUiGroup generalDataReplicationGroup = new WebUiGroup( "generalReplicationSettingsGroup", replicationSettingsPage.getId() );
        generalDataReplicationGroup.withTitle( "General Data Replication Processing" );
        configManager.registerWebUiGroup( generalDataReplicationGroup );

        CAPTURE_DATA_MODIFICATIONS.withUi( generalDataReplicationGroup.getId(), 0 );
        configManager.registerConfig( CAPTURE_DATA_MODIFICATIONS );
        REPLICATION_FAIL_COUNT_THRESHOLD.withUi( generalDataReplicationGroup.getId(), 1 );
        configManager.registerConfig( REPLICATION_FAIL_COUNT_THRESHOLD );

        REPLICATION_OBJECT_STORAGE.withUi( generalDataReplicationGroup.getId(), 2 );
        configManager.registerConfig( REPLICATION_OBJECT_STORAGE );

        configManager.registerWebUiPage( replicationSettingsPage );

        // Monitoring UI
        globalInformationPage = new InformationPage( "Data Replication" );
        im.addPage( globalInformationPage );

        // General Replication Group
        InformationGroup generalGroup = new InformationGroup( globalInformationPage, "General" ).setOrder( 0 );
        im.addGroup( generalGroup );

    }


    protected void registerMonitoringGroup( InformationGroup imGroup ) {
        im.addGroup( imGroup );
    }


    protected abstract ReplicationStrategy getAssociatedReplicationStrategy();


    protected abstract void preparePlacements();

    /**
     * Is used to manually trigger the replication of all pending updates of each placement for a specific table .
     *
     * @param tableId table to refresh
     */
    public abstract void replicateChanges( long tableId );


    /**
     * Is used to manually trigger the replication of all pending updates of a specific data placement.
     *
     * @param tableId table to refresh
     * @param adapterId adapter to refresh
     */
    public abstract void replicateChanges( long tableId, int adapterId );


    /**
     * Is used to manually trigger the replication of all pending updates on an entire adapter.
     * For example when the adapter was down.
     *
     * This should not be possible to be directly executed by users. Rather with automatic system maintenance.
     *
     * @param adapterId adapter to refresh
     */
    public abstract void replicateChanges( int adapterId );


    /**
     * Places all replication Objects that originated within one Transaction to the replication engine.
     * The list order is equal to the modification order of each object. It is up to each specific Replication Engine the to distribute the changes
     *
     * @param replicationObjects Ordered list of modification changes that can be replicated to secondary placements
     */
    protected abstract void queueReplicationData( @NonNull List<ChangeDataReplicationObject> replicationObjects );


    public void registerDataCaptureObjects( List<ChangeDataCaptureObject> cdcObjects, long commitTimestamp ) {
        if ( CAPTURE_DATA_MODIFICATIONS.getBoolean() ) {
            List<ChangeDataReplicationObject> replicationObjects = new ArrayList<>();

            for ( ChangeDataCaptureObject cdc : cdcObjects ) {
                ChangeDataReplicationObject replicationObject = transformCaptureObject( cdc, commitTimestamp );
                if ( replicationObject == null ) {
                    continue;
                }
                replicationObjects.add( replicationObject );
            }
            if ( replicationObjects.isEmpty() ) {
                return;
            }
            queueReplicationData( replicationObjects );
        }
    }


    // Transforms the general purpose data capture object into a targeted Replication Object that can be directly queued for individual objects
    private ChangeDataReplicationObject transformCaptureObject( ChangeDataCaptureObject cdcObject, long commitTimestamp ) {

        long replicationDataId = replicationDataIdBuilder.getAndIncrement();

        // TODO @HENNLO As soon as policies are available check which replication strategy a given entity has or allows and then distribute
        // the changes based on the replication strategy. Each replication strategy can have different replication engines
        CatalogEntity entity = Catalog.getInstance().getTable( cdcObject.getTableId() );
        List<CatalogDataPlacement> secondaryDataPlacements = Catalog.getInstance().getDataPlacementsByReplicationStrategy( entity.id, associatedStrategy );

        // If there are no SecondaryPlacements that need to be refreshed we can skip immediately
        // Also if the parameterValues are empty this means there is nothing to be updated
        if ( !secondaryDataPlacements.isEmpty() && cdcObject.getParameterValues() != null ) {
            Set<Pair> targetPartitionPlacements = new HashSet<>();
            Map<Long, Pair> dependentReplicationIds = new HashMap<>();
            for ( CatalogDataPlacement dataPlacement : secondaryDataPlacements ) {

                List<Long> tempPartitionIds = dataPlacement.getAllPartitionIds();
                tempPartitionIds.retainAll( cdcObject.getAccessedPartitions() );

                if ( tempPartitionIds.isEmpty() ) {
                    continue;
                }
                // Gather all adapter-partition pairs to uniquely identify partition placement
                tempPartitionIds.forEach( partitionId -> targetPartitionPlacements.add( new Pair( partitionId, dataPlacement.adapterId ) ) );
            }

            // Map them to unique replicable replicationId which is going to be executed by the workers
            targetPartitionPlacements.forEach( placement -> dependentReplicationIds.put(
                    replicationIdBuilder.getAndIncrement(), placement ) );

            switch ( cdcObject.getOperation() ) {
                case INSERT:
                    return new InsertReplicationObject(
                            replicationDataId,
                            cdcObject.getParentTxId(),
                            cdcObject.getTableId(),
                            cdcObject.getOperation(),
                            cdcObject.getFieldList(),
                            cdcObject.getParameterTypes(),
                            cdcObject.getParameterValues(),
                            commitTimestamp,
                            dependentReplicationIds );

                case UPDATE:
                    return new UpdateReplicationObject(
                            replicationDataId,
                            cdcObject.getParentTxId(),
                            cdcObject.getTableId(),
                            cdcObject.getOperation(),
                            cdcObject.getUpdateColumnList(),
                            cdcObject.getSourceExpressionList(),
                            cdcObject.getCondition(),
                            cdcObject.getFieldList(),
                            cdcObject.getParameterTypes(),
                            cdcObject.getParameterValues(),
                            commitTimestamp,
                            dependentReplicationIds );

                case DELETE:
                    return new DeleteReplicationObject(
                            replicationDataId,
                            cdcObject.getParentTxId(),
                            cdcObject.getTableId(),
                            cdcObject.getOperation(),
                            cdcObject.getUpdateColumnList(),
                            cdcObject.getSourceExpressionList(),
                            cdcObject.getCondition(),
                            cdcObject.getFieldList(),
                            cdcObject.getParameterTypes(),
                            cdcObject.getParameterValues(),
                            commitTimestamp,
                            dependentReplicationIds );
            }
        }
        return null;
    }


    protected DataStore getDataStoreInstance( int storeId ) {
        Adapter adapterInstance = AdapterManager.getInstance().getAdapter( storeId );
        if ( adapterInstance == null ) {
            throw new RuntimeException( "Unknown store id: " + storeId );
        }
        // Make sure it is a data store instance
        if ( adapterInstance instanceof DataStore ) {
            return (DataStore) adapterInstance;
        } else {
            throw new RuntimeException( "Unknown kind of adapter: " + adapterInstance.getClass().getName() );
        }
    }


    public int getCurrentReplicationFailCount( long replicationId ) {
        if ( replicationFailCount.containsKey( replicationId ) ) {
            return replicationFailCount.get( replicationId );
        }
        return 0;
    }


    protected boolean increaseReplicationFailCount( long replicationId ) {

        int newFailCount;
        if ( replicationFailCount.containsKey( replicationId ) ) {

            newFailCount = replicationFailCount.get( replicationId ) + 1;
            if ( newFailCount >= REPLICATION_FAIL_COUNT_THRESHOLD.getInt() ) {
                return false;
            }
        } else {
            // First fail count for the replication
            newFailCount = 1;
        }
        replicationFailCount.put( replicationId, newFailCount );
        return true;
    }


    protected void cleanseReplicationFailCount( long replicationId ) {
        if ( replicationFailCount.containsKey( replicationId ) ) {
            replicationFailCount.remove( replicationId );
        }
    }

}
