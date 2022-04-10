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


import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.catalog.entity.CatalogDataPlacement;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.replication.cdc.ChangeDataReplicationObject;
import org.polypheny.db.replication.cdc.DeleteReplicationObject;
import org.polypheny.db.replication.cdc.InsertReplicationObject;
import org.polypheny.db.replication.cdc.UpdateReplicationObject;
import org.polypheny.db.replication.properties.exception.OutdatedReplicationException;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;


public interface DataReplicator {

    long replicateData( Transaction transaction, ChangeDataReplicationObject dataReplicationObject, long replicationId ) throws OutdatedReplicationException;

    AlgRoot buildInsertStatement( Statement statement, InsertReplicationObject dataReplicationObject, CatalogDataPlacement dataPlacement, CatalogPartitionPlacement targetPartitionPlacement );

    AlgRoot buildUpdateStatement( Statement statement, UpdateReplicationObject dataReplicationObject, CatalogDataPlacement dataPlacement, CatalogPartitionPlacement targetPartitionPlacement );

    AlgRoot buildDeleteStatement( Statement statement, DeleteReplicationObject dataReplicationObject, CatalogDataPlacement dataPlacement, CatalogPartitionPlacement targetPartitionPlacement );


}
