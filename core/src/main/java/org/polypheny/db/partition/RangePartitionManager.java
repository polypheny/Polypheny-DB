package org.polypheny.db.partition;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;


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
    public boolean validatePartitionSetup( List<String> partitionQualifiers, long numPartitions, List<String> partitionNames, CatalogColumn partitionColumn ) {
        super.validatePartitionSetup( partitionQualifiers, numPartitions, partitionNames, partitionColumn );

        System.out.println( "TYPE "+ partitionColumn.type );

        if (partitionColumn.type.getFamily() != PolyTypeFamily.NUMERIC ){
            throw new RuntimeException( "You cannot specify RANGE partitioning for a non-numeric type. Detected Type: "+  partitionColumn.type );
        }


        if ( partitionQualifiers.isEmpty() ) {
            throw new RuntimeException( "RANGE Partitioning doesn't support  empty Partition Qualifiers: '" + partitionQualifiers + "'. USE (PARTITION name1 VALUES(value1)[(,PARTITION name1 VALUES(value1))*])" );
        }

        if ( partitionQualifiers.size() + 1 != numPartitions ) {
            throw new RuntimeException( "Number of partitionQualifiers '" + partitionQualifiers + "' + (mandatory 'Unbound' partition) is not equal to number of specified partitions '" + numPartitions + "'" );
        }

        if ( partitionQualifiers.isEmpty() || partitionQualifiers.size() == 0 ) {

            throw new RuntimeException( "Partition Qualifiers are empty '" + partitionQualifiers );
        }

        //return true;

        throw new RuntimeException("RANGE Partitioning is not implemented yet");
        //return false;
    }


    @Override
    public boolean allowsUnboundPartition() {
        return true;
    }

}
