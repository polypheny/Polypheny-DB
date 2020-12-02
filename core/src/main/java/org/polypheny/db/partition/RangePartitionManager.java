package org.polypheny.db.partition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogPartition;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownPartitionIdRuntimeException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;


@Slf4j
public class RangePartitionManager extends AbstractPartitionManager {

    boolean hasUnboundPartition = true;


    @Override
    public long getTargetPartitionId( CatalogTable catalogTable, String columnValue ) {
        log.debug( "RangePartitionManager" );

        Catalog catalog = Catalog.getInstance();
        long selectedPartitionId = -1;
        long unboundPartitionId = -1;
        try {
            for ( long partitionID : catalogTable.partitionIds ) {

                CatalogPartition catalogPartition = catalog.getPartition( partitionID );

                if ( catalogPartition.isUnbound ) {
                    unboundPartitionId = catalogPartition.id;
                    continue;
                }

                if ( isValueInRange(columnValue, catalogPartition) ){
                    System.out.println("in RANGE");
                    log.debug( "Found column value: {} on partitionID {} in range: [{} - {}]", columnValue, partitionID, catalogPartition.partitionQualifiers.get( 0 ), catalogPartition.partitionQualifiers.get( 1 ) );
                    selectedPartitionId = catalogPartition.id;
                    return selectedPartitionId;
                }
            }
            // If no concrete partition could be identified, report back the unbound/default partition
            if ( selectedPartitionId == -1 ) {
                selectedPartitionId = unboundPartitionId;
            }

        } catch ( UnknownPartitionIdRuntimeException e ) {
            e.printStackTrace();
        }

        return selectedPartitionId;
    }


    @Override
    public boolean validatePartitionDistribution( CatalogTable table ) {
        log.debug( "RangePartitionManager validPartitionDistribution()" );
        Catalog catalog = Catalog.getInstance();
        // Check for every column if there exists at least one placement which contains all partitions
        for ( long columnId : table.columnIds ) {
            boolean skip = false;

            int numberOfFullPlacements = getPlacementsWithAllPartitions( columnId, table.numPartitions ).size();
            if ( numberOfFullPlacements >= 1 ) {
                log.debug( "Found ColumnPlacement which contains all partitions for column: {}", columnId );
                skip = true;
                break;
            }

            if ( skip ) {
                continue;
            } else {
                if ( log.isDebugEnabled() ) {
                    log.debug( "{}' has no placement containing all partitions", Catalog.getInstance().getColumn( columnId ).name );
                }
                return false;
            }
        }

        return true;
    }


    // Needed when columnPlacements are being dropped
    @Override
    public boolean probePartitionDistributionChange( CatalogTable catalogTable, int storeId, long columnId ) {
        Catalog catalog = Catalog.getInstance();

        // change is only critical if there is only one column left with the charecteristics
        int numberOfFullPlacements = getPlacementsWithAllPartitions( columnId, catalogTable.numPartitions ).size();
        if ( numberOfFullPlacements <= 1 ) {
            //Check if this one column is the column we are about to delete
            if ( catalog.getPartitionsOnDataPlacement( storeId, catalogTable.id ).size() == catalogTable.numPartitions ) {
                return false;
            }
        }

        return true;
    }


    @Override
    public List<CatalogColumnPlacement> getRelevantPlacements( CatalogTable catalogTable, List<Long> partitionIds ) {
        Catalog catalog = Catalog.getInstance();
        List<CatalogColumnPlacement> relevantCcps = new ArrayList<>();

        if ( partitionIds != null ) {
            try {

                for ( long partitionId : partitionIds ) {

                    // Find stores with full placements (partitions)
                    // Pick for each column the column placement which has full partitioning //SELECT WORST-CASE ergo Fallback
                    for ( long columnId : catalogTable.columnIds ) {

                        List<CatalogColumnPlacement> ccps = catalog.getColumnPlacementsByPartition( catalogTable.id, partitionId, columnId );
                        if ( !ccps.isEmpty() ) {
                            //get first column placement which contains partition
                            relevantCcps.add( ccps.get( 0 ) );
                            if ( log.isDebugEnabled() ) {
                                log.debug( "{} {} with part. {}", ccps.get( 0 ).storeUniqueName, ccps.get( 0 ).getLogicalColumnName(), partitionId );
                            }
                        }
                    }
                }

            } catch ( UnknownPartitionIdRuntimeException e ) {
                e.printStackTrace();
            }
        } else {
            // Take the first column placement
            // Worst-case
            for ( long columnId : catalogTable.columnIds ) {
                relevantCcps.add( getPlacementsWithAllPartitions( columnId, catalogTable.numPartitions ).get( 0 ) );
            }
        }
        return relevantCcps;
    }


    @Override
    public boolean validatePartitionSetup( List<List<String>> partitionQualifierList, long numPartitions, List<String> partitionNames, CatalogColumn partitionColumn ) {
        super.validatePartitionSetup( partitionQualifierList, numPartitions, partitionNames, partitionColumn );


        if (partitionColumn.type.getFamily() != PolyTypeFamily.NUMERIC ){
            throw new RuntimeException( "You cannot specify RANGE partitioning for a non-numeric type. Detected Type: "+  partitionColumn.type + " for column: '" + partitionColumn.name + "'");
        }


        for ( List <String> partitionQualifiers:partitionQualifierList ) {
            for ( String partitionQualifier:partitionQualifiers ) {

                if(  partitionQualifier.isEmpty() ){
                    throw new RuntimeException( "RANGE Partitioning doesn't support  empty Partition Qualifiers: '" + partitionQualifierList + "'. USE (PARTITION name1 VALUES(value1)[(,PARTITION name1 VALUES(value1))*])" );
                }

                if(  !(partitionQualifier.chars().allMatch( Character::isDigit )) ){
                    throw new RuntimeException( "RANGE Partitioning doesn't support non-integer partition qualifiers: '" + partitionQualifier + "'" );
                }

                if(  partitionQualifiers.size() > 2 || partitionQualifiers.size() <= 1 ){
                    throw new RuntimeException( "RANGE Partitioning doesn't support anything other than two qualifiers per partition. Use (PARTITION name1 VALUES(lowerValue, upperValue)\n Error Token: '" + partitionQualifiers +"' " );
                }
            }
        }

        if ( partitionQualifierList.size() + 1 != numPartitions ) {
            throw new RuntimeException( "Number of partitionQualifiers '" + partitionQualifierList + "' + (mandatory 'Unbound' partition) is not equal to number of specified partitions '" + numPartitions + "'" );
        }

        if ( partitionQualifierList.isEmpty() || partitionQualifierList.size() == 0 ) {

            throw new RuntimeException( "Partition Qualifiers are empty '" + partitionQualifierList );
        }

        //Check if range is overlapping
        for ( int i = 0; i < partitionQualifierList.size() ; i++ ) {

            int lowerBound = Integer.valueOf( partitionQualifierList.get( i ).get( 0 ));
            int upperBound = Integer.valueOf( partitionQualifierList.get( i ).get( 1 ));

            //Check
            if ( upperBound < lowerBound ){
                int temp = upperBound;
                upperBound = lowerBound;
                lowerBound = temp;

                //Rearrange List values lower < upper
                partitionQualifierList.set( i,  Stream.of(partitionQualifierList.get( i ).get( 1 ),partitionQualifierList.get( i ).get( 0 )).collect( Collectors.toList()) );

            }else if ( upperBound == lowerBound ){
                throw new RuntimeException( "No Range specified. Lower and upper bound are equal:" +  lowerBound + " = " + upperBound  );
            }

            for ( int k = i; k < partitionQualifierList.size()-1; k++ ){
                int contestingLowerBound = Integer.valueOf( partitionQualifierList.get( k+1 ).get( 0 ));
                int contestingUpperBound = Integer.valueOf( partitionQualifierList.get( k+1 ).get( 1 ));

                if ( contestingUpperBound < contestingLowerBound ){
                    int temp = contestingUpperBound;
                    contestingUpperBound = contestingUpperBound;
                    contestingLowerBound = temp;

                    partitionQualifierList.set( k+1,  Stream.of(partitionQualifierList.get( k+1 ).get( 1 ),partitionQualifierList.get( k+1 ).get( 0 )).collect( Collectors.toList()) );

                }else if ( contestingUpperBound == contestingLowerBound ){
                    throw new RuntimeException( "No Range specified. Lower and upper bound are equal:" +  contestingLowerBound + " = " + contestingUpperBound  );
                }

                //Check if they are overlapping
                if (lowerBound <= contestingUpperBound && upperBound >= contestingLowerBound) {
                    throw new RuntimeException( "Several ranges are overlapping: [" +  lowerBound + " - " + upperBound  + "] and [" +  contestingLowerBound + " - " + contestingUpperBound  + "] You need to specify distinct ranges.");
                }

            }

        }

        return true;
    }


    @Override
    public boolean allowsUnboundPartition() {
        return true;
    }

    private boolean isValueInRange(String columnValue, CatalogPartition catalogPartition){

        int lowerBound = Integer.valueOf( catalogPartition.partitionQualifiers.get( 0 ));
        int upperBound = Integer.valueOf( catalogPartition.partitionQualifiers.get( 1 ));

        double numericValue = Double.valueOf( columnValue);

        if ( numericValue >= lowerBound && numericValue <= upperBound ){
            return true;
        }

        return false;
    }

}
