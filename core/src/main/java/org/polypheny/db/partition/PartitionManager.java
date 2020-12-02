package org.polypheny.db.partition;

import java.util.List;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;

public interface PartitionManager {


    // Returns the Index of the partition where to place the object
    long getTargetPartitionId( CatalogTable catalogTable, String columnValue );

    boolean validatePartitionDistribution( CatalogTable table );

    boolean probePartitionDistributionChange( CatalogTable catalogTable, int storeId, long columnId );

    List<CatalogColumnPlacement> getRelevantPlacements( CatalogTable catalogTable, List<Long> partitionIds );

    boolean validatePartitionSetup( List<List<String>> partitionQualifiers, long numPartitions, List<String> partitionNames, CatalogColumn partitionColumn );

    boolean allowsUnboundPartition();
}
