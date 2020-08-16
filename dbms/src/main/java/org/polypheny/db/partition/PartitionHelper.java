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


import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownPartitionTypeException;

//Currently exclusively used to generate hashes to identify on which partition to place
// a object
public class PartitionHelper {

    //returns the ID of the partition where to place the object
    public long getPartitionHash(CatalogTable catalogTable, String value){

        //IDEA: mySchema_testTable_sales_RANGE_100

        long partitionID = 0;
        String partitionKey = "";
        try {
            switch (catalogTable.partitionType){

                case HASH:
                    partitionKey = catalogTable.getSchemaName() + catalogTable.name + catalogTable
                                + Catalog.getInstance().getColumn(catalogTable.partitionColumnId).name + catalogTable.partitionType + value;
                    partitionID = partitionKey.hashCode()*-1 ;
                    break;

                case LIST:
                    break;

                case RANGE:
                    //Same as in HASH
                    partitionKey = catalogTable.getSchemaName() + catalogTable.name + catalogTable
                            + Catalog.getInstance().getColumn(catalogTable.partitionColumnId).name + catalogTable.partitionType + value;
                    partitionID = partitionKey.hashCode()*-1 ;
                    break;

                case ROUNDROBIN:
                    break;

                case NONE:
                    throw new RuntimeException("Table " + catalogTable.name + " is not partitioned!");
            }

        } catch (UnknownColumnException | GenericCatalogException e) {
            e.printStackTrace();
        }

        // Don't want any neg. value for now
        if (partitionID <= 0){ partitionID *= -1; }

        //finally decide on which partition to put it
        return partitionID % catalogTable.numPartitions;


    }

}
