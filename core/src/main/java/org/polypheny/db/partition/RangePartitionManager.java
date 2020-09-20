package org.polypheny.db.partition;

import org.polypheny.db.catalog.entity.CatalogTable;

public class RangePartitionManager extends AbstractPartitionManager{

    @Override
    public long getTargetPartitionId(CatalogTable catalogTable, String columnValue) {
        System.out.println("HENNLO  RangePartitionManager getPartitionId()");
        return 0;
    }

    @Override
    public boolean validatePartitionDistribution(CatalogTable table) {
        System.out.println("HENNLO  RangePartitionManager validPartitionDistribution()");
        return false;
    }
    //Needed when columnPlacements are being dropped
    @Override
    public boolean probePartitionDistributionChange(CatalogTable catalogTable, int storeId, long columnId){
        //TODO nOt implemented yet
        return false;

    }


}
