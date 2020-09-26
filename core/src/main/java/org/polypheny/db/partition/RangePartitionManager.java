package org.polypheny.db.partition;

import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;

import java.util.List;

public class RangePartitionManager extends AbstractPartitionManager{

    boolean hasUnboundPartition = true;

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

    @Override
    public List<CatalogColumnPlacement> getRelevantPlacements(CatalogTable catalogTable, long partitionId) {
        return null;
    }

    @Override
    public boolean validatePartitionSetup(List<String> partitionQualifiers, long numPartitions, List<String> partitionNames) {
        super.validatePartitionSetup(partitionQualifiers,numPartitions, partitionNames);
        return false;
    }

    @Override
    public boolean allowsUnboundPartition() {
        return true;
    }

}
