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

package org.polypheny.db.partition;

import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;

import java.util.List;

//Possible extensions could be range partitioning and hash partitioning
//Need to check if round robin would be sufficient as well or basically just needed to
// distribute workload for LoadBalancing
//Maybe separate partition in the technical-partition itself.
//And meta information about the partiton characteristics of a table
//the latter could maybe be specified in the table as well.
public abstract class AbstractPartitionManager implements PartitionManager{


    //Not sure if ID is enough to use in sort of a hierarchical request (schema.table.partition)
    //Or if I want to use the partitionkey uniquely identifying a partiton
    //is the column used to partiton the table
    @Getter
    protected String partitionKey;

    @Getter
    protected long partitionID;

    //Identify on which Schema
    @Getter
    protected String schemaName;

    //Which table
    @Getter
    protected String tableName;

    @Getter
    protected long tableId;

    @Getter
    protected List<Long> columnIds;
    @Getter
    protected List<String> columnNames;

    @Getter
    protected boolean allowsUnboundPartition;

    //How many partitions
    //Should move to table object to obtain this
    //information from table
    //default partition or non partitioned = means 1 partition
    @Getter
    protected int partitions = 1;


    //TODO HENNLO: Maybe extend partitoning with a store to
    // uniquely identify a partition element or a column to route a query more efficientely
    //essentially needed for replicas


    //returns the Index of the partition where to place the object
    public abstract long getTargetPartitionId(CatalogTable catalogTable, String columnValue);

    public abstract boolean validatePartitionDistribution(CatalogTable table);

    public abstract boolean probePartitionDistributionChange(CatalogTable catalogTable, int storeId, long columnId);

    public abstract List<CatalogColumnPlacement> getRelevantPlacements(CatalogTable catalogTable, long partitionId);

    public  boolean validatePartitionSetup(List<String> partitionQualifiers, long numPartitions, List<String> partitionNames){

        if ( numPartitions == 0 && partitionNames.size() < 2){
            throw new RuntimeException("Partition Table failed for  Can't specify partition names with less than 2 names");
        }

        return true;
    }

    public abstract boolean allowsUnboundPartition();
}
