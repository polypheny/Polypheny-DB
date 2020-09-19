package org.polypheny.db.partition;

import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;

public interface PartitionManager {


    //returns the Index of the partition where to place the object
    public abstract long getTargetPartitionId(CatalogTable catalogTable, String columnValue);

    public abstract boolean validPartitionDistribution();

}
