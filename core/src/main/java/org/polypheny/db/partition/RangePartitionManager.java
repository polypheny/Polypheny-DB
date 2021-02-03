/*
 * Copyright 2019-2021 The Polypheny Project
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

import java.util.ArrayList;
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
import org.polypheny.db.type.PolyTypeFamily;


@Slf4j
public class RangePartitionManager extends AbstractPartitionManager {

    public static final boolean ALLOWS_UNBOUND_PARTITION = true;


    @Override
    public long getTargetPartitionId( CatalogTable catalogTable, String columnValue ) {
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

                if ( isValueInRange( columnValue, catalogPartition ) ) {
                    if ( log.isDebugEnabled() ) {
                        log.debug( "Found column value: {} on partitionID {} in range: [{} - {}]",
                                columnValue,
                                partitionID,
                                catalogPartition.partitionQualifiers.get( 0 ),
                                catalogPartition.partitionQualifiers.get( 1 ) );
                    }
                    selectedPartitionId = catalogPartition.id;
                    return selectedPartitionId;
                }
            }
            // If no concrete partition could be identified, report back the unbound/default partition
            if ( selectedPartitionId == -1 ) {
                selectedPartitionId = unboundPartitionId;
            }

        } catch ( UnknownPartitionIdRuntimeException e ) {
            // TODO Hennlo: Why catching this runtime exception?
            log.error( "Caught exception", e );
        }

        return selectedPartitionId;
    }


    // Needed when columnPlacements are being dropped
    @Override
    public boolean probePartitionDistributionChange( CatalogTable catalogTable, int storeId, long columnId ) {
        Catalog catalog = Catalog.getInstance();

        // change is only critical if there is only one column left with the characteristics
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
                                log.debug( "{} {} with part. {}", ccps.get( 0 ).adapterUniqueName, ccps.get( 0 ).getLogicalColumnName(), partitionId );
                            }
                        }
                    }
                }

            } catch ( UnknownPartitionIdRuntimeException e ) {
                // TODO Hennlo: Why catching this runtime exception?
                log.error( "Caught exception", e );
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

        if ( partitionColumn.type.getFamily() != PolyTypeFamily.NUMERIC ) {
            throw new RuntimeException( "You cannot specify RANGE partitioning for a non-numeric type. Detected Type: " + partitionColumn.type + " for column: '" + partitionColumn.name + "'" );
        }

        for ( List<String> partitionQualifiers : partitionQualifierList ) {
            for ( String partitionQualifier : partitionQualifiers ) {

                if ( partitionQualifier.isEmpty() ) {
                    throw new RuntimeException( "RANGE Partitioning doesn't support  empty Partition Qualifiers: '" + partitionQualifierList + "'. USE (PARTITION name1 VALUES(value1)[(,PARTITION name1 VALUES(value1))*])" );
                }

                if ( !(partitionQualifier.chars().allMatch( Character::isDigit )) ) {
                    throw new RuntimeException( "RANGE Partitioning doesn't support non-integer partition qualifiers: '" + partitionQualifier + "'" );
                }

                if ( partitionQualifiers.size() > 2 || partitionQualifiers.size() <= 1 ) {
                    throw new RuntimeException( "RANGE Partitioning doesn't support anything other than two qualifiers per partition. Use (PARTITION name1 VALUES(lowerValue, upperValue)\n Error Token: '" + partitionQualifiers + "' " );
                }
            }
        }

        if ( partitionQualifierList.size() + 1 != numPartitions ) {
            throw new RuntimeException( "Number of partitionQualifiers '" + partitionQualifierList + "' + (mandatory 'Unbound' partition) is not equal to number of specified partitions '" + numPartitions + "'" );
        }

        if ( partitionQualifierList.isEmpty() ) {
            throw new RuntimeException( "Partition Qualifiers are empty '" + partitionQualifierList + "'" );
        }

        // Check if range is overlapping
        for ( int i = 0; i < partitionQualifierList.size(); i++ ) {

            int lowerBound = Integer.parseInt( partitionQualifierList.get( i ).get( 0 ) );
            int upperBound = Integer.parseInt( partitionQualifierList.get( i ).get( 1 ) );

            // Check
            if ( upperBound < lowerBound ) {
                int temp = upperBound;
                upperBound = lowerBound;
                lowerBound = temp;

                // Rearrange List values lower < upper
                partitionQualifierList.set( i, Stream.of( partitionQualifierList.get( i ).get( 1 ), partitionQualifierList.get( i ).get( 0 ) ).collect( Collectors.toList() ) );

            } else if ( upperBound == lowerBound ) {
                throw new RuntimeException( "No Range specified. Lower and upper bound are equal:" + lowerBound + " = " + upperBound );
            }

            for ( int k = i; k < partitionQualifierList.size() - 1; k++ ) {
                int contestingLowerBound = Integer.parseInt( partitionQualifierList.get( k + 1 ).get( 0 ) );
                int contestingUpperBound = Integer.parseInt( partitionQualifierList.get( k + 1 ).get( 1 ) );

                if ( contestingUpperBound < contestingLowerBound ) {
                    int temp = contestingUpperBound;
                    contestingUpperBound = contestingUpperBound;
                    contestingLowerBound = temp;

                    List<String> list = Stream.of( partitionQualifierList.get( k + 1 ).get( 1 ), partitionQualifierList.get( k + 1 ).get( 0 ) )
                            .collect( Collectors.toList() );
                    partitionQualifierList.set( k + 1, list );

                } else if ( contestingUpperBound == contestingLowerBound ) {
                    throw new RuntimeException( "No Range specified. Lower and upper bound are equal:" + contestingLowerBound + " = " + contestingUpperBound );
                }

                //Check if they are overlapping
                if ( lowerBound <= contestingUpperBound && upperBound >= contestingLowerBound ) {
                    throw new RuntimeException( "Several ranges are overlapping: [" + lowerBound + " - " + upperBound + "] and [" + contestingLowerBound + " - " + contestingUpperBound + "] You need to specify distinct ranges." );
                }

            }

        }

        return true;
    }


    @Override
    public boolean allowsUnboundPartition() {
        return ALLOWS_UNBOUND_PARTITION;
    }


    private boolean isValueInRange( String columnValue, CatalogPartition catalogPartition ) {
        int lowerBound = Integer.parseInt( catalogPartition.partitionQualifiers.get( 0 ) );
        int upperBound = Integer.parseInt( catalogPartition.partitionQualifiers.get( 1 ) );

        double numericValue = Double.parseDouble( columnValue );

        return numericValue >= lowerBound && numericValue <= upperBound;
    }

}
