/*
 * Copyright 2019-2024 The Polypheny Project
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
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.catalog.entity.LogicalAdapter;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationPlacement;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.routing.ColumnDistribution;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;


public interface DataMigrator {

    void copyData(
            Transaction transaction,
            LogicalAdapter store,
            LogicalTable source,
            List<LogicalColumn> columns,
            AllocationEntity target );

    void copyData(
            Transaction transaction,
            LogicalAdapter store,
            LogicalTable source,
            List<LogicalColumn> columns,
            AllocationPlacement target );

    /**
     * Currently used to transfer data if unpartitioned is about to be partitioned.
     *
     * @param transaction Transactional scope
     * @param store Target Store where data should be migrated to
     * @param sourceTables Source Table from where data is queried
     */
    void copyAllocationData(
            Transaction transaction,
            LogicalAdapter store,
            List<AllocationTable> sourceTables,
            PartitionProperty targetProperty,
            List<AllocationTable> targetTables,
            LogicalTable table );

    AlgRoot buildInsertStatement( Statement statement, List<AllocationColumn> to, AllocationEntity allocation );

    //is used within copyData
    void executeQuery(
            List<AllocationColumn> columns,
            AlgRoot sourceAlg,
            Statement sourceStatement,
            Statement targetStatement,
            AlgRoot targetAlg,
            boolean isMaterializedView,
            boolean doesSubstituteOrderBy );

    AlgRoot buildDeleteStatement( Statement statement, List<AllocationColumn> to, AllocationEntity allocation );

    AlgRoot getSourceIterator( Statement statement, ColumnDistribution columnDistribution );


    void copyGraphData( AllocationGraph to, LogicalGraph from, Transaction transaction );

    void copyDocData( AllocationCollection to, LogicalCollection from, Transaction transaction );

}
