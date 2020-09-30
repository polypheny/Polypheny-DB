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
import org.polypheny.db.catalog.entity.CatalogStore;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownPartitionException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.routing.Router;

import java.util.ArrayList;
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

                if (catalogPartition.isUnbound) {
                    unboundPartitionId = catalogPartition.id;
                }
                for(int i = 0; i < catalogPartition.partitionQualifiers.size(); i++ ) {
                    //Could be int
                    if (catalogPartition.partitionQualifiers.get(i).equals(columnValue)) {
                        System.out.println("HENNLO  ListPartitionManager getPartitionId(): Found column value: " + columnValue + " on partitionID " + partitionID + " with qualifiers: " + catalogPartition.partitionQualifiers);
                        selectedPartitionId = catalogPartition.id;
                        break;
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
        Catalog catalog = Catalog.getInstance();
            //Check for every column if there exists at least one placement which contains all partitions
            for (long columnId : table.columnIds){
                boolean skip = false;
                
                int numberOfFullPlacements = getNumberOfPlacementsWithAllPartitions(columnId, table.numPartitions).size();
                if ( numberOfFullPlacements >= 1 ){
                    System.out.println("HENNLO: validatePartitionDistribution() Found ColumnPlacement which contains all partitions for column: "+ columnId);
                    skip = true;
                    break;
                }

                if ( skip ){ continue;}
                else{
                    System.out.println("ERROR Column: '" + Catalog.getInstance().getColumn(columnId).name +"' has no placement containing all partitions");
                    return false;
                }
            }


        return true;
    }

    //Needed when columnPlacements are being dropped
    @Override
    public boolean probePartitionDistributionChange(CatalogTable catalogTable, int storeId, long columnId){

        Catalog catalog = Catalog.getInstance();
        /* try {

            int thresholdCounter = 0;
            boolean validDistribution = false;
            //check for every partition if the column in question has still all partition somewher even when columnId on Store would be removed
            for (long partitionId : catalogTable.partitionIds) {

                //check if a column is dropped from a store if this column has still other placements with all partitions
                List<CatalogColumnPlacement> ccps = catalog.getColumnPlacementsByPartition(catalogTable.id, partitionId, columnId);
                for ( CatalogColumnPlacement columnPlacement : ccps){
                    if (columnPlacement.storeId != storeId){
                        thresholdCounter++;
                        break;
                    }
                }
                if ( thresholdCounter < 1){
                    return false;
                }
            }

            } catch ( UnknownPartitionException e) {
            throw  new RuntimeException(e);
         }*/



            //change is only critical if there is only one column left with the charecteristics
            int numberOfFullPlacements = getNumberOfPlacementsWithAllPartitions(columnId, catalogTable.numPartitions).size();
            if ( numberOfFullPlacements <= 1 ){
                //Check if this one column is the column we are about to delete
                if ( catalog.getPartitionsOnDataPlacement(storeId, catalogTable.id).size() == catalogTable.numPartitions ){
                    return false;
                }
            }



        return true;

    }

    //Relevant for select
    @Override
    public List<CatalogColumnPlacement> getRelevantPlacements(CatalogTable catalogTable, List<Long> partitionIds) {
        Catalog catalog = Catalog.getInstance();
        List<CatalogColumnPlacement> relevantCcps = new ArrayList<>();

        if (partitionIds != null) {
            try {

                for (long partitionId : partitionIds) {

                    //Find stores with fullplacements (partitions)
                    //Pick for each column the columnplacemnt which has full partitioning //SELECT WORSTCASE ergo Fallback
                    for (long columnId : catalogTable.columnIds) {

                        List<CatalogColumnPlacement> ccps = catalog.getColumnPlacementsByPartition(catalogTable.id, partitionId, columnId);
                        if (!ccps.isEmpty()) {
                            //get first columnpalcement which contains parttion
                            relevantCcps.add(ccps.get(0));
                            System.out.println("------------" + ccps.get(0).storeUniqueName + " " + ccps.get(0).getLogicalColumnName() + " with part. " + partitionId);
                        } else {
                            //Worstcase routing
                            //
                        }


                        //Take the first column placement
                        //Worstcase
                        //relevantCcps.add(getNumberOfPlacementsWithAllPartitions(columnId, catalogTable.numPartitions).get(0));
                    }
                }

            } catch (UnknownPartitionException e) {
                e.printStackTrace();
            }
        }else{
            //Take the first column placement
            //Worstcase
            for (long columnId : catalogTable.columnIds) {
                relevantCcps.add(getNumberOfPlacementsWithAllPartitions(columnId, catalogTable.numPartitions).get(0));
            }
        }
        return relevantCcps;
    }

    @Override
    public boolean validatePartitionSetup(List<String> partitionQualifiers, long numPartitions, List<String> partitionNames) {
        super.validatePartitionSetup(partitionQualifiers,numPartitions, partitionNames);
        if( partitionQualifiers.size() +1 != numPartitions ){
            throw  new RuntimeException("Number of partitionQualifiers '" + partitionQualifiers + "' + (mandatory 'Unbound' partition) is not equal to number of specified partitions '" + numPartitions +"'");
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

    /**
     *  Returns number of placements for this column which contain all partitions
     *
     * @param columnId  column to be checked
     * @param numPartitions  numPartitions
     * @return If its correctly distributed or not
     */
    private List<CatalogColumnPlacement> getNumberOfPlacementsWithAllPartitions(long columnId, long numPartitions){

        Catalog catalog = Catalog.getInstance();

        //Return every placement of this column
        List<CatalogColumnPlacement> tempCcps = catalog.getColumnPlacements(columnId);
        List<CatalogColumnPlacement> returnCcps = new ArrayList<>();
        int placementCounter = 0;
        for (CatalogColumnPlacement ccp : tempCcps ){
            //If the DataPlacement has stored all partitions and therefore all partitions for this placement
            if ( catalog.getPartitionsOnDataPlacement(ccp.storeId, ccp.tableId).size() == numPartitions  ){
                returnCcps.add(ccp);
                placementCounter++;
            }
        }
        return returnCcps;

    }

}
