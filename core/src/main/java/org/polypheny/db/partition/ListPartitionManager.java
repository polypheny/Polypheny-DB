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
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogPartition;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownPartitionException;
import org.polypheny.db.routing.Router;

import java.util.List;

public class ListPartitionManager extends AbstractPartitionManager{

    boolean allowsUnboundPartition = true;

    @Override
    public long getTargetPartitionId(CatalogTable catalogTable, String columnValue) {
        System.out.println("HENNLO  ListPartitionManager getPartitionId()");

        Catalog catalog = Catalog.getInstance();
        long selectedPartitionId = -1;
        long unboundPartitionId = -1;
        try {
            for(long partitionID : catalogTable.partitionIds) {

                CatalogPartition catalogPartition = catalog.getPartition(partitionID);
                System.out.println("HENNLO  ListPartitionManager getPartitionId() "+ catalogPartition.partitionQualifiers.contains(columnValue)+ " "+ catalogPartition.partitionQualifiers + " " + catalogPartition.partitionQualifiers.get(0).toString());

                for(int i = 0; i < catalogPartition.partitionQualifiers.size(); i++ ) {
                    //Could be int
                    if (catalogPartition.partitionQualifiers.get(i).toString().equals(columnValue)) {
                        System.out.println("HENNLO  ListPartitionManager getPartitionId(): Found column value: " + columnValue + " on partitionID " + partitionID + " with qualifiers: " + catalogPartition.partitionQualifiers);
                        selectedPartitionId = catalogPartition.id;
                        break;
                    }
                    else if (catalogPartition.isUnbound) {
                        selectedPartitionId = catalogPartition.id;
                    }
                }

        }
            //If no concrete partition could be identified, report back the unbound/default partition
            if ( selectedPartitionId == -1){
                selectedPartitionId = unboundPartitionId;
            }

        } catch (UnknownPartitionException e) {
            e.printStackTrace();
        }

        return selectedPartitionId;
    }

    @Override
    public boolean validatePartitionDistribution(CatalogTable table) {
        System.out.println("HENNLO  ListPartitionManager validPartitionDistribution()");
        return false;
    }

    //Needed when columnPlacements are being dropped
    @Override
    public boolean probePartitionDistributionChange(CatalogTable catalogTable, int storeId, long columnId){
        //TODO not implemented yet
        return false;

    }

    @Override
    public List<CatalogColumnPlacement> getRelevantPlacements(CatalogTable catalogTable, long partitionId) {
        return null;
    }

    @Override
    public boolean validatePartitionSetup(List partitionQualifiers, long numPartitions) {

        if( partitionQualifiers.size() +1 != numPartitions ){
            throw  new RuntimeException("Number of partitionQualifiers '" + partitionQualifiers + "' +(Unbound partition) is not equal to number of specified partitions '" + numPartitions +"'");
        }

        if( partitionQualifiers.isEmpty() || partitionQualifiers.size() == 0 ){
            throw  new RuntimeException("Partition Qualifiers are empty '" + partitionQualifiers );
        }

        return true;
    }

    @Override
    public boolean allowsUnboundPartition() {
        return true;
    }

}
