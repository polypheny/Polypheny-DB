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
import java.util.List;

//Possible extensions could be range partitioning and hash partitioning
//Need to check if round robin would be sufficient as well or basically just needed to
// distribute workload for LoadBalancing
//Maybe separate partition in the technical-partition itself.
//And meta information about the partiton characteristics of a table
//the latter could maybe be specified in the table as well.
public abstract class AbstractPartition {

    final Catalog catalog = Catalog.getInstance();

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

    //How many partitions
    //Should move to table object to obtain this
    //information from table
    //default partition or non partitioned = means 1 partition
    @Getter
    protected int partitions = 1;

    @Getter @Setter
    public long partitionRecords;

    //TODO HENNLO: Maybe extend partitoning with a store to
    // uniquely identify a partition element or a column to route a query more efficientely
    //essentially needed for replicas


    protected AbstractPartition(
            long tableId,
            String schemaName,
            String tableName,
            List<Long> columnIds,
            List<String> columnNames,
            String partitionKey,
            int partitions) {

        this.tableId = tableId;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnIds = columnIds;
        this.columnNames = columnNames;
        this.partitionKey = partitionKey;
        this.partitions = partitions;

        createPartition(partitions);
    }


    protected AbstractPartition(
            long tableId,
            String schemaName,
            String tableName,
            List<Long> columnIds,
            List<String> columnNames,
            String partitionKey) {

        this.tableId = tableId;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnIds = columnIds;
        this.columnNames = columnNames;
        this.partitionKey = partitionKey;

        createPartition(partitions);
    }



    protected AbstractPartition(
            long tableId,
            String schemaName,
            String tableName,
            String partitionKey) {

        this.tableId = tableId;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.partitionKey = partitionKey;

        createPartition(partitions);
    }


    private void createPartition(int partitions){

        //Every table without explicit partitioning will consist of one partition

        System.out.println("HENNLO: AbstractPartition INFO");
        //Check if already partitioned
        //Raise Runtime Exception

        if ( !checkPartitionKey(partitionKey) ){
            //TODO: raise RuntimeException "Specified partition key is not a column of table: X"
            //But should technically not be possible and handled by methods beforehand
            System.out.println("HENNLO: AbstractPartiton \"Specified partition key '" + partitionKey + "' is not a column of table: \"" + tableName);
        }
        System.out.println("Successfully created a partiton for: " + schemaName + "." + tableName + " based on column '" + partitionKey + "'");
    }



    public void modifyPartition(String partitionKey, int partitions){
        //ToDo to be implemented
        //Needed when altering a table

        //Check if already partitioned

        //if it is. Revert original partitioning OR concurrently build another form of partitioning and just redirect
        //to new partition once finished and finally removing the old partitioning

        //if not. simply change meta information and split table in new partitions
        this.partitionKey = partitionKey;
        this.partitions = partitions;
    }


    //Check if specified partitionKey/column is even part of this table
    //Does not necessariyl have to be implemneted in here
    //Depends on structure of table if it would be feasible to check there beforehand
    //TODO: HENNLO Maybe check if possible to solve with placements
    private boolean checkPartitionKey(String partitionKey){

        //TODO: To be implmented
        //For now it assumes that your specified partitionKey will always be part of the columns
        System.out.println("HENNLO: AbstractPartition checkPartitionKey");
        return true;
    }

}
