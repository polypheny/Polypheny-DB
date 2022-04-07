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


import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.catalog.Catalog.ReplicationStrategy;


/**
 * Is used to capture and aggregate all modification to primary placements, that shall be replicated
 */
@Slf4j
public class ChangeDataCollector {

    // Used to keep the cdc objects on memory until they are save to be committed and consequently be put into the queue
    // Or if rolled back removed from map entirely

    // Use linked HashMap internally to preserve natural insertion order that operations are correctly serialized
    private static Map<Long, LinkedHashMap<Long, ChangeDataCaptureObject>> pendingCaptures = new HashMap<>();     // TxId -> ( StmtId -> cdcObject )


    /**
     * Will be called during Alg implementation
     * Enriches the captured
     *
     * Requires ({@link #prepareCDC(ChangeDataCaptureObject)}) to be called first.
     * To initially prepare the capture object.
     */
    public static long captureChanges( DataContext dataContext ) {

        long txId = dataContext.getStatement().getTransaction().getId();
        long stmtId = dataContext.getStatement().getId();
        // Only present if prepareCDC has already been executed
        // enrich captureObject with Parameter Values
        if ( pendingCaptures.containsKey( txId ) && pendingCaptures.get( txId ).containsKey( stmtId ) ) {
            pendingCaptures.get( txId ).get( stmtId ).setParameterValues( dataContext.getParameterValues() );
        }

        return 1;
    }

    // QUEUE per DataPlacement
    // Only place in Queue as soon as TX has been commited

    // Store Capture Objects UNTIL TX is commited only then start transforming to target replicators
    // If rolled back remove objects from intermediate cache

    // If replication failed. Increase failedCount
    // And check Global configuration ALLOW_REPLICATION_FAILURES
    // As soon as limit is reached. Set target placement to INFINITELY OUTDATED
    // and remove object from capture list as well as all others with same target placement


    public static void prepareCDC( ChangeDataCaptureObject cdcObject ) {

        LinkedHashMap stmtMap = new LinkedHashMap();
        long parentTxId = cdcObject.getParentTxId();
        long stmtId = cdcObject.getStmtId();

        // Check if TX has already been initially set to capture data
        if ( pendingCaptures.containsKey( parentTxId ) ) {

            stmtMap = pendingCaptures.get( parentTxId );
            if ( !stmtMap.containsKey( stmtId ) ) {
                stmtMap.put( stmtId, cdcObject );
            } else {
                log.warn( "Pending Changes have already been added for this statement. This should not happen" );
            }
        } else {
            stmtMap.put( stmtId, cdcObject );
            pendingCaptures.put( parentTxId, stmtMap );
        }

    }


    public void abortChangeDataReplication( long txId ) {

        // Remove from pending changes
        if ( pendingCaptures.containsKey( txId ) ) {
            pendingCaptures.remove( txId );
        }

        // Log that TX was aborted and no longer requires CDC to be propagated to secondaries

    }


    /**
     * Triggered after TX has been committed.
     * To identify which transaction and which statements can be applied to secondaries
     *
     * Ultimately queues all changes that have been captured within this transaction and
     * passes them to the designated ReplicationEngine
     *
     * @param txId txId of the committed transaction
     * @param commitTimestamp The commit timestamp of the primary transaction to later on also update the secondaries with this timestamp.
     */
    public void finalizeDataCapturing( long txId, long commitTimestamp ) {

        // Check if TX is even present or something wrong occurred
        if ( pendingCaptures.containsKey( txId ) ) {

            Set<ChangeDataReplicationObject> targetReplications = new HashSet<>();
            Set<ChangeDataCaptureObject> rawSourceDataCapture = pendingCaptures.get( txId ).values().stream().collect( Collectors.toSet() );

            ReplicationEngine replicationEngine = ReplicationEngineProvider.getInstance().getReplicationEngine( ReplicationStrategy.LAZY );

            // Queue those target objects
            replicationEngine.registerDataCaptureObjects( rawSourceDataCapture.stream().collect( Collectors.toList() ), commitTimestamp );

            // Remove initial CaptureObject now from pending changes

            // If something goes wrong flag placements as INFINITELY outdated

            // Trigger copy process to bring them UPTODATE again

            // Remove entry from intermediate queue
            pendingCaptures.remove( txId );
        }
        // ELSE - LOG Warning : preparation of CDC was triggered automatically by commit but nothing was captured to replicate
    }


}
