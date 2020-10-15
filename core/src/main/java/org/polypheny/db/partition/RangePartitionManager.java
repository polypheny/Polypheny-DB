package org.polypheny.db.partition;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;

@Slf4j
public class RangePartitionManager extends AbstractPartitionManager {

    boolean hasUnboundPartition = true;


    @Override
    public long getTargetPartitionId( CatalogTable catalogTable, String columnValue ) {
        log.debug( "RangePartitionManager" );
        return 0;
    }


    @Override
    public boolean validatePartitionDistribution( CatalogTable table ) {
        log.debug( "RangePartitionManager validPartitionDistribution()" );
        return false;
    }


    // Needed when columnPlacements are being dropped
    @Override
    public boolean probePartitionDistributionChange( CatalogTable catalogTable, int storeId, long columnId ) {
        // TODO @HENNLO not implemented yet
        return false;
    }


    @Override
    public List<CatalogColumnPlacement> getRelevantPlacements( CatalogTable catalogTable, List<Long> partitionIds ) {
        return null;
    }


    @Override
    public boolean validatePartitionSetup( List<String> partitionQualifiers, long numPartitions, List<String> partitionNames ) {
        super.validatePartitionSetup( partitionQualifiers, numPartitions, partitionNames );
        return false;
    }


    @Override
    public boolean allowsUnboundPartition() {
        return true;
    }

}
