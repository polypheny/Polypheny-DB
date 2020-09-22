package org.polypheny.db.partition;

import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;

import java.util.List;

public interface PartitionManager {


    //returns the Index of the partition where to place the object
    abstract long getTargetPartitionId(CatalogTable catalogTable, String columnValue);

    abstract boolean validatePartitionDistribution(CatalogTable table);

    abstract boolean probePartitionDistributionChange(CatalogTable catalogTable, int storeId, long columnId);

    abstract List<CatalogColumnPlacement> getRelevantPlacements(CatalogTable catalogTable, long partitionId);

    abstract boolean validatePartitionSetup(List partitionQualifiers, long numPartitions);

    abstract boolean allowsUnboundPartition();
}
