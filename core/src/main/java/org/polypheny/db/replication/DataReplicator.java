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
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;


public interface DataReplicator {

    void replicateData( Transaction transaction, ChangeDataReplicationObject dataReplicationObject, long replicationId );

    AlgRoot buildInsertStatement( Statement statement, CatalogDataPlacement dataPlacement, CatalogPartitionPlacement targetPartitionPlacement, ChangeDataReplicationObject dataReplicationObject );

    AlgRoot buildDeleteStatement( Statement statement, CatalogPartitionPlacement targetPartitionPlacement );

    AlgRoot buildUpdateStatement( Statement statement, CatalogPartitionPlacement targetPartitionPlacement );
}
