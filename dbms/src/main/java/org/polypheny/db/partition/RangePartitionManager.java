/*
 * Copyright 2019-2024 The Polypheny Project
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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.allocation.AllocationPartition;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.partition.PartitionFunctionInfo.PartitionFunctionInfoColumn;
import org.polypheny.db.partition.PartitionFunctionInfo.PartitionFunctionInfoColumnType;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;


@Slf4j
public class RangePartitionManager extends AbstractPartitionManager {

    public static final boolean REQUIRES_UNBOUND_PARTITION_GROUP = true;
    public static final String FUNCTION_TITLE = "RANGE";
    public static final List<PolyType> SUPPORTED_TYPES = ImmutableList.of( PolyType.INTEGER, PolyType.BIGINT, PolyType.SMALLINT, PolyType.TINYINT );


    @Override
    public long getTargetPartitionId( LogicalTable table, PartitionProperty property, String columnValue ) {
        long unboundPartitionId = -1;
        long selectedPartitionId = -1;

        // Process all accumulated CatalogPartitions
        for ( AllocationPartition partition : Catalog.snapshot().alloc().getPartitionsFromLogical( table.id ) ) {
            if ( partition.isUnbound ) {
                unboundPartitionId = partition.id;
                break;
            }

            if ( isValueInRange( columnValue, partition ) ) {
                if ( log.isDebugEnabled() ) {
                    log.debug( "Found column value: {} on partitionID {} in range: [{} - {}]",
                            columnValue,
                            partition.id,
                            partition.qualifiers.get( 0 ),
                            partition.qualifiers.get( 1 ) );
                }
                selectedPartitionId = partition.id;
                break;
            }
        }

        // If no concrete partition could be identified, report back the unbound/default partition
        if ( selectedPartitionId == -1 ) {
            selectedPartitionId = unboundPartitionId;
        }

        return selectedPartitionId;
    }


    @Override
    public List<List<String>> validateAdjustPartitionGroupSetup( List<List<String>> partitionGroupQualifiers, long numPartitionGroups, List<String> partitionGroupNames, LogicalColumn partitionColumn ) {
        partitionGroupQualifiers = new ArrayList<>( super.validateAdjustPartitionGroupSetup( partitionGroupQualifiers, numPartitionGroups, partitionGroupNames, partitionColumn ) );

        if ( partitionColumn.type.getFamily() != PolyTypeFamily.NUMERIC ) {
            throw new GenericRuntimeException( "You cannot specify RANGE partitioning for a non-numeric type. Detected ExpressionType: " + partitionColumn.type + " for column: '" + partitionColumn.name + "'" );
        }

        for ( List<String> partitionQualifiers : partitionGroupQualifiers ) {
            for ( String partitionQualifier : partitionQualifiers ) {
                if ( partitionQualifier.isEmpty() ) {
                    throw new GenericRuntimeException( "RANGE Partitioning doesn't support  empty Partition Qualifiers: '" + partitionGroupQualifiers + "'. USE (PARTITION name1 VALUES(value1)[(,PARTITION name1 VALUES(value1))*])" );
                }

                if ( !(partitionQualifier.chars().allMatch( Character::isDigit )) ) {
                    throw new GenericRuntimeException( "RANGE Partitioning doesn't support non-integer partition qualifiers: '" + partitionQualifier + "'" );
                }

                if ( partitionQualifiers.size() != 2 ) {
                    throw new GenericRuntimeException( "RANGE Partitioning doesn't support anything other than two qualifiers per partition. Use (PARTITION name1 VALUES(lowerValue, upperValue)\n Error Token: '" + partitionQualifiers + "' " );
                }
            }
        }

        if ( partitionGroupQualifiers.size() + 1 != numPartitionGroups ) {
            throw new GenericRuntimeException( "Number of partitionQualifiers '" + partitionGroupQualifiers + "' + (mandatory 'Unbound' partition) is not equal to number of specified partitions '" + numPartitionGroups + "'" );
        }

        if ( partitionGroupQualifiers.isEmpty() ) {
            throw new GenericRuntimeException( "Partition Qualifiers are empty '" + partitionGroupQualifiers + "'" );
        }

        // Check if range is overlapping
        for ( int i = 0; i < partitionGroupQualifiers.size(); i++ ) {

            int lowerBound = Integer.parseInt( partitionGroupQualifiers.get( i ).get( 0 ) );
            int upperBound = Integer.parseInt( partitionGroupQualifiers.get( i ).get( 1 ) );

            // Check
            if ( upperBound < lowerBound ) {
                int temp = upperBound;
                upperBound = lowerBound;
                lowerBound = temp;

                // Rearrange List values lower < upper
                partitionGroupQualifiers.set( i, Stream.of( partitionGroupQualifiers.get( i ).get( 1 ), partitionGroupQualifiers.get( i ).get( 0 ) ).toList() );

            } else if ( upperBound == lowerBound ) {
                throw new GenericRuntimeException( "No Range specified. Lower and upper bound are equal:" + lowerBound + " = " + upperBound );
            }

            for ( int k = i; k < partitionGroupQualifiers.size() - 1; k++ ) {
                int contestingLowerBound = Integer.parseInt( partitionGroupQualifiers.get( k + 1 ).get( 0 ) );
                int contestingUpperBound = Integer.parseInt( partitionGroupQualifiers.get( k + 1 ).get( 1 ) );

                if ( contestingUpperBound < contestingLowerBound ) {
                    int temp = contestingUpperBound;
                    contestingUpperBound = contestingLowerBound;
                    contestingLowerBound = temp;

                    List<String> list = Stream.of( partitionGroupQualifiers.get( k + 1 ).get( 1 ), partitionGroupQualifiers.get( k + 1 ).get( 0 ) )
                            .toList();
                    partitionGroupQualifiers.set( k + 1, list );

                } else if ( contestingUpperBound == contestingLowerBound ) {
                    throw new GenericRuntimeException( "No Range specified. Lower and upper bound are equal:" + contestingLowerBound + " = " + contestingUpperBound );
                }

                // Check if they are overlapping
                if ( lowerBound <= contestingUpperBound && upperBound >= contestingLowerBound ) {
                    throw new GenericRuntimeException( "Several ranges are overlapping: [" + lowerBound + " - " + upperBound + "] and [" + contestingLowerBound + " - " + contestingUpperBound + "] You need to specify distinct ranges." );
                }

            }

        }

        return partitionGroupQualifiers;
    }


    @Override
    public PartitionFunctionInfo getPartitionFunctionInfo() {
        // Dynamic content which will be generated by selected numPartitions
        List<PartitionFunctionInfoColumn> dynamicRows = new ArrayList<>();
        dynamicRows.add( PartitionFunctionInfoColumn.builder()
                .fieldType( PartitionFunctionInfoColumnType.STRING )
                .mandatory( true )
                .modifiable( true )
                .sqlPrefix( "PARTITION" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .defaultValue( "" )
                .build() );

        dynamicRows.add( PartitionFunctionInfoColumn.builder()
                .fieldType( PartitionFunctionInfoColumnType.INTEGER )
                .mandatory( true )
                .modifiable( true )
                .sqlPrefix( "VALUES(" )
                .sqlSuffix( "" )
                .valueSeparation( "," )
                .defaultValue( "" )
                .build() );

        dynamicRows.add( PartitionFunctionInfoColumn.builder()
                .fieldType( PartitionFunctionInfoColumnType.INTEGER )
                .mandatory( true )
                .modifiable( true )
                .sqlPrefix( "," )
                .sqlSuffix( ")" )
                .valueSeparation( "," )
                .defaultValue( "" )
                .build() );

        // Fixed rows to display after dynamically generated ones
        List<List<PartitionFunctionInfoColumn>> rowsAfter = new ArrayList<>();
        List<PartitionFunctionInfoColumn> unboundRow = new ArrayList<>();
        unboundRow.add( PartitionFunctionInfoColumn.builder()
                .fieldType( PartitionFunctionInfoColumnType.LABEL )
                .mandatory( false )
                .modifiable( false )
                .sqlPrefix( "" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .defaultValue( "UNBOUND" )
                .build() );

        unboundRow.add( PartitionFunctionInfoColumn.builder()
                .fieldType( PartitionFunctionInfoColumnType.STRING )
                .mandatory( false )
                .modifiable( false )
                .sqlPrefix( "" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .defaultValue( "auto fill" )
                .build() );

        unboundRow.add( PartitionFunctionInfoColumn.builder()
                .fieldType( PartitionFunctionInfoColumnType.STRING )
                .mandatory( false )
                .modifiable( false )
                .sqlPrefix( "" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .defaultValue( "auto fill" )
                .build() );

        rowsAfter.add( unboundRow );

        return PartitionFunctionInfo.builder()
                .functionTitle( FUNCTION_TITLE )
                .description( "Partitions data based on a defined numeric range. A partition is therefore responsible for all values residing in that range. "
                        + "INFO: Note that this partition function provides an 'UNBOUND' partition capturing all values that are not covered by one of the specified ranges." )
                .sqlPrefix( "(" )
                .sqlSuffix( ")" )
                .rowSeparation( "," )
                .dynamicRows( dynamicRows )
                .rowsAfter( rowsAfter )
                .headings( new ArrayList<>( Arrays.asList( "Partition Name", "MIN", "MAX" ) ) )
                .build();
    }


    @Override
    public boolean requiresUnboundPartitionGroup() {
        return REQUIRES_UNBOUND_PARTITION_GROUP;
    }


    @Override
    public boolean supportsColumnOfType( PolyType type ) {
        return SUPPORTED_TYPES.contains( type );
    }


    private boolean isValueInRange( String columnValue, AllocationPartition partition ) {
        int lowerBound = Integer.parseInt( partition.qualifiers.get( 0 ) );
        int upperBound = Integer.parseInt( partition.qualifiers.get( 1 ) );

        double numericValue = Double.parseDouble( columnValue );

        return numericValue >= lowerBound && numericValue <= upperBound;
    }

}
