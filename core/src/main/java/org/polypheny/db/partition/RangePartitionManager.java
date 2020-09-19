package org.polypheny.db.partition;

import org.polypheny.db.catalog.entity.CatalogTable;

public class RangePartitionManager extends AbstractPartitionManager{

    @Override
    public long getTargetPartitionId(CatalogTable catalogTable, String columnValue) {
        System.out.println("HENNLO  RangePartitionManager getPartitionId()");
        return 0;
    }

    @Override
    public boolean validPartitionDistribution() {
        System.out.println("HENNLO  RangePartitionManager validPartitionDistribution()");
        return false;
    }


}
