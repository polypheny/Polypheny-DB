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
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.ReplicationStrategy;
import org.polypheny.db.catalog.entity.CatalogDataPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.config.ConfigBoolean;
import org.polypheny.db.config.ConfigManager;
import org.polypheny.db.config.WebUiGroup;
import org.polypheny.db.config.WebUiPage;
import org.polypheny.db.util.Pair;


public abstract class ReplicationEngine {


    public static final ConfigBoolean CAPTURE_DATA_MODIFICATIONS = new ConfigBoolean(
            "runtime/captureDataModification",
            "If disabled no data modification is actively being captured. "
                    + "To refresh outdated placements a manual refresh operation needs to be executed. "
                    + "No additional memory requirements are necessary since changes are no longer cached.",
            true );


    private ReplicationStrategy associatedStrategy;

    protected final WebUiPage replicationSettingsPage;
    protected final ConfigManager configManager = ConfigManager.getInstance();

    // Only needed to track changes on uniquely identifiable replicationData
    private static final AtomicLong replicationDataIdBuilder = new AtomicLong( 1 );
    private static final AtomicLong replicationIdBuilder = new AtomicLong( 1 );


    protected ReplicationEngine() {

        this.associatedStrategy = getAssociatedReplicationStrategy();

        this.replicationSettingsPage = new WebUiPage(
                "replicationSettings",
                "Data Replication",
                "Settings for data replication." );

        final WebUiGroup generalDataReplicationGroup = new WebUiGroup( "generalReplicationSettingsGroup", replicationSettingsPage.getId() );
        generalDataReplicationGroup.withTitle( "General Data Replication Processing" );
        configManager.registerWebUiGroup( generalDataReplicationGroup );
        CAPTURE_DATA_MODIFICATIONS.withUi( generalDataReplicationGroup.getId(), 0 );

        configManager.registerConfig( CAPTURE_DATA_MODIFICATIONS );
        configManager.registerWebUiPage( replicationSettingsPage );
    }


    protected abstract ReplicationStrategy getAssociatedReplicationStrategy();


    public abstract void replicateChanges();


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

        // TODO @HENNLO As soon as policies are available check which replication strategy a given table has or allows and then distribute
        // the changes based on the replication strategy. Each replication strategy can have different replication engines
        CatalogTable table = Catalog.getInstance().getTable( cdcObject.getTableId() );
        List<CatalogDataPlacement> secondaryDataPlacements = Catalog.getInstance().getDataPlacementsByReplicationStrategy( table.id, associatedStrategy );

        // If there are no SecondaryPlacements that need to be refreshed we can skip immediately
        if ( !secondaryDataPlacements.isEmpty() ) {
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
                // Map them to unique replicable replicationId which is going to be executed by the workers
                targetPartitionPlacements.forEach( placement -> dependentReplicationIds.put(
                        replicationDataIdBuilder.getAndIncrement(), placement ) );
            }

            return new ChangeDataReplicationObject(
                    replicationDataId,
                    cdcObject.getParentTxId(),
                    cdcObject.getTableId(),
                    cdcObject.getOperation(),
                    cdcObject.getUpdateColumnList(),
                    cdcObject.getSourceExpressionList(),
                    cdcObject.getParameterValues(),
                    commitTimestamp,
                    dependentReplicationIds );
        } else {
            return null;
        }
    }

}
