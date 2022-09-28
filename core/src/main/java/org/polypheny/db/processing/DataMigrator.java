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

package org.polypheny.db.processing;

import java.util.List;
import java.util.Map;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogGraphDatabase;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;


public interface DataMigrator {

    void copyData(
            Transaction transaction,
            CatalogAdapter store,
            List<CatalogColumn> columns,
            List<Long> partitionIds );

    /**
     * Currently used to transfer data if partitioned table is about to be merged.
     * For Table Partitioning use {@link #copyPartitionData(Transaction, CatalogAdapter, CatalogTable, CatalogTable, List, List, List)}  } instead
     *
     * @param transaction Transactional scope
     * @param store Target Store where data should be migrated to
     * @param sourceTable Source Table from where data is queried
     * @param targetTable Source Table from where data is queried
     * @param columns Necessary columns on target
     * @param placementDistribution Pre computed mapping of partitions and the necessary column placements
     * @param targetPartitionIds Target Partitions where data should be inserted
     */
    void copySelectiveData(
            Transaction transaction,
            CatalogAdapter store,
            CatalogTable sourceTable, CatalogTable targetTable, List<CatalogColumn> columns,
            Map<Long, List<CatalogColumnPlacement>> placementDistribution,
            List<Long> targetPartitionIds );

    /**
     * Currently used to to transfer data if unpartitioned is about to be partitioned.
     * For Table Merge use {@link #copySelectiveData(Transaction, CatalogAdapter, CatalogTable, CatalogTable, List, Map, List)}   } instead
     *
     * @param transaction Transactional scope
     * @param store Target Store where data should be migrated to
     * @param sourceTable Source Table from where data is queried
     * @param targetTable Target Table where data is to be inserted
     * @param columns Necessary columns on target
     * @param sourcePartitionIds Source Partitions which need to be considered for querying
     * @param targetPartitionIds Target Partitions where data should be inserted
     */
    void copyPartitionData(
            Transaction transaction,
            CatalogAdapter store,
            CatalogTable sourceTable,
            CatalogTable targetTable,
            List<CatalogColumn> columns,
            List<Long> sourcePartitionIds,
            List<Long> targetPartitionIds );

    AlgRoot buildInsertStatement( Statement statement, List<CatalogColumnPlacement> to, long partitionId );

    //is used within copyData
    void executeQuery( List<CatalogColumn> columns, AlgRoot sourceRel, Statement sourceStatement, Statement targetStatement, AlgRoot targetRel, boolean isMaterializedView, boolean doesSubstituteOrderBy );

    AlgRoot buildDeleteStatement( Statement statement, List<CatalogColumnPlacement> to, long partitionId );

    AlgRoot getSourceIterator( Statement statement, Map<Long, List<CatalogColumnPlacement>> placementDistribution );


    void copyGraphData( CatalogGraphDatabase graph, Transaction transaction, Integer existingAdapterId, CatalogAdapter adapter );

}
