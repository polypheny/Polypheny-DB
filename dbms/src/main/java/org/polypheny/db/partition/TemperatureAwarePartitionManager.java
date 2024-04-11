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
import java.util.Map;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.partition.PartitionFunctionInfo.PartitionFunctionInfoColumn;
import org.polypheny.db.partition.PartitionFunctionInfo.PartitionFunctionInfoColumnType;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.partition.properties.TemperaturePartitionProperty;
import org.polypheny.db.type.PolyType;


public class TemperatureAwarePartitionManager extends AbstractPartitionManager {

    public static final boolean REQUIRES_UNBOUND_PARTITION_GROUP = false;
    public static final String FUNCTION_TITLE = "TEMPERATURE";
    public static final List<PolyType> SUPPORTED_TYPES = ImmutableList.of( PolyType.INTEGER, PolyType.BIGINT, PolyType.SMALLINT, PolyType.TINYINT, PolyType.VARCHAR );


    @Override
    public long getTargetPartitionId( LogicalTable table, PartitionProperty property, String columnValue ) {
        // Get partition manager
        PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();

        PartitionManager partitionManager = partitionManagerFactory.getPartitionManager( ((TemperaturePartitionProperty) property).getInternalPartitionFunction() );

        return partitionManager.getTargetPartitionId( table, property, columnValue );
    }


    @Override
    public Map<Long, List<AllocationColumn>> getRelevantPlacements( LogicalTable table, List<AllocationEntity> allocs, List<Long> excludedAdapters ) {
        // Get partition manager
        PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
        PartitionProperty property = Catalog.getInstance().getSnapshot().alloc().getPartitionProperty( table.id ).orElseThrow();
        PartitionManager partitionManager = partitionManagerFactory.getPartitionManager(
                ((TemperaturePartitionProperty) property).getInternalPartitionFunction()
        );

        return partitionManager.getRelevantPlacements( table, allocs, excludedAdapters );
    }


    @Override
    public Map<Long, Map<Long, List<AllocationColumn>>> getAllPlacements( LogicalTable table, List<Long> partitionIds ) {
        // Get partition manager
        PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
        PartitionProperty property = Catalog.getInstance().getSnapshot().alloc().getPartitionProperty( table.id ).orElseThrow();
        PartitionManager partitionManager = partitionManagerFactory.getPartitionManager(
                ((TemperaturePartitionProperty) property).getInternalPartitionFunction()
        );

        return partitionManager.getAllPlacements( table, partitionIds );
    }


    @Override
    public boolean requiresUnboundPartitionGroup() {
        return REQUIRES_UNBOUND_PARTITION_GROUP;
    }


    @Override
    public boolean supportsColumnOfType( PolyType type ) {
        return SUPPORTED_TYPES.contains( type );
    }


    @Override
    public List<List<String>> validateAdjustPartitionGroupSetup( List<List<String>> partitionGroupQualifiers, long numPartitionGroups, List<String> partitionGroupNames, LogicalColumn partitionColumn ) {
        return super.validateAdjustPartitionGroupSetup( partitionGroupQualifiers, numPartitionGroups, partitionGroupNames, partitionColumn );
    }


    @Override
    public PartitionFunctionInfo getPartitionFunctionInfo() {
        List<List<PartitionFunctionInfoColumn>> rowsBefore = new ArrayList<>();

        //ROW for HOT partition infos about custom name & hot-label,
        List<PartitionFunctionInfoColumn> hotRow = new ArrayList<>();
        hotRow.add( PartitionFunctionInfoColumn.builder()
                .fieldType( PartitionFunctionInfoColumnType.STRING )
                .mandatory( true )
                .modifiable( true )
                .sqlPrefix( "(PARTITION" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .defaultValue( "HOT" )
                .build() );
        hotRow.add( PartitionFunctionInfoColumn.builder()
                .fieldType( PartitionFunctionInfoColumnType.LABEL )
                .mandatory( false )
                .modifiable( false )
                .sqlPrefix( "" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .defaultValue( "HOT" )
                .build() );

        //ROW for COLD partition infos about custom name & cold-label,
        List<PartitionFunctionInfoColumn> coldRow = new ArrayList<>();
        coldRow.add( PartitionFunctionInfoColumn.builder()
                .fieldType( PartitionFunctionInfoColumnType.STRING )
                .mandatory( true )
                .modifiable( true )
                .sqlPrefix( "PARTITION" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .defaultValue( "COLD" )
                .build() );
        coldRow.add( PartitionFunctionInfoColumn.builder()
                .fieldType( PartitionFunctionInfoColumnType.LABEL )
                .mandatory( false )
                .modifiable( false )
                .sqlPrefix( "" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .defaultValue( "COLD" )
                .build() );

        List<PartitionFunctionInfoColumn> rowInHot = new ArrayList<>();
        rowInHot.add( PartitionFunctionInfoColumn.builder()
                .fieldType( PartitionFunctionInfoColumnType.LABEL )
                .mandatory( false )
                .modifiable( false )
                .sqlPrefix( "" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .defaultValue( "% Threshold into HOT" )
                .build() );

        rowInHot.add( PartitionFunctionInfoColumn.builder()
                .fieldType( PartitionFunctionInfoColumnType.STRING )
                .mandatory( false )
                .modifiable( true )
                .sqlPrefix( "VALUES(" )
                .sqlSuffix( "%)," )
                .valueSeparation( "" )
                .defaultValue( "10" )
                .build() );

        List<PartitionFunctionInfoColumn> rowOutHot = new ArrayList<>();
        rowOutHot.add( PartitionFunctionInfoColumn.builder()
                .fieldType( PartitionFunctionInfoColumnType.LABEL )
                .mandatory( false )
                .modifiable( false )
                .sqlPrefix( "" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .defaultValue( "% Threshold out of HOT" )
                .build() );

        rowOutHot.add( PartitionFunctionInfoColumn.builder()
                .fieldType( PartitionFunctionInfoColumnType.STRING )
                .mandatory( false )
                .modifiable( true )
                .sqlPrefix( "VALUES(" )
                .sqlSuffix( "%))" )
                .valueSeparation( "" )
                .defaultValue( "15" )
                .build() );

        rowsBefore.add( hotRow );
        rowsBefore.add( rowInHot );
        rowsBefore.add( coldRow );
        rowsBefore.add( rowOutHot );

        // COST MODEL
        // Fixed rows to display after dynamically generated ones
        List<List<PartitionFunctionInfoColumn>> rowsAfter = new ArrayList<>();

        List<PartitionFunctionInfoColumn> costRow = new ArrayList<>();
        costRow.add( PartitionFunctionInfoColumn.builder()
                .fieldType( PartitionFunctionInfoColumnType.LABEL )
                .mandatory( false )
                .modifiable( false )
                .sqlPrefix( "" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .defaultValue( "Cost Model" )
                .build() );

        costRow.add( PartitionFunctionInfoColumn.builder()
                .fieldType( PartitionFunctionInfoColumnType.LIST )
                .mandatory( false )
                .modifiable( true )
                .sqlPrefix( "USING FREQUENCY" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .options( new ArrayList<>( Arrays.asList( "ALL", "WRITE", "READ" ) ) )
                .build() );

        List<PartitionFunctionInfoColumn> extendedCostRow = new ArrayList<>();

        extendedCostRow.add( PartitionFunctionInfoColumn.builder()
                .fieldType( PartitionFunctionInfoColumnType.LABEL )
                .mandatory( false )
                .modifiable( false )
                .sqlPrefix( "" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .defaultValue( "Time Window" )
                .build() );

        extendedCostRow.add( PartitionFunctionInfoColumn.builder()
                .fieldType( PartitionFunctionInfoColumnType.STRING )
                .mandatory( false )
                .modifiable( true )
                .sqlPrefix( "INTERVAL" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .defaultValue( "2" )
                .build() );

        extendedCostRow.add( PartitionFunctionInfoColumn.builder()
                .fieldType( PartitionFunctionInfoColumnType.LIST )
                .mandatory( false )
                .modifiable( true )
                .sqlPrefix( "" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .options( new ArrayList<>( Arrays.asList( "Minutes", "Hours", "Days" ) ) )
                .build() );

        List<PartitionFunctionInfoColumn> chunkRow = new ArrayList<>();
        chunkRow.add( PartitionFunctionInfoColumn.builder()
                .fieldType( PartitionFunctionInfoColumnType.LABEL )
                .mandatory( false )
                .modifiable( false )
                .sqlPrefix( "" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .defaultValue( "Number of internal  data chunks" )
                .build() );

        chunkRow.add( PartitionFunctionInfoColumn.builder()
                .fieldType( PartitionFunctionInfoColumnType.STRING )
                .mandatory( false )
                .modifiable( true )
                .sqlPrefix( "WITH" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .defaultValue( "-04071993" )
                .build() );

        List<PartitionFunctionInfoColumn> unboundRow = new ArrayList<>();
        unboundRow.add( PartitionFunctionInfoColumn.builder()
                .fieldType( PartitionFunctionInfoColumnType.LABEL )
                .mandatory( false )
                .modifiable( false )
                .sqlPrefix( "" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .defaultValue( "Internal Partitioning" )
                .build() );

        unboundRow.add( PartitionFunctionInfoColumn.builder()
                .fieldType( PartitionFunctionInfoColumnType.LIST )
                .mandatory( false )
                .modifiable( true )
                .sqlPrefix( "" )
                .sqlSuffix( "PARTITIONS" )
                .valueSeparation( "" )
                .options( new ArrayList<>( List.of( "HASH" ) ) )
                .build() );

        rowsAfter.add( costRow );
        rowsAfter.add( extendedCostRow );
        rowsAfter.add( chunkRow );
        rowsAfter.add( unboundRow );

        // Bring all rows and columns together

        return PartitionFunctionInfo.builder()
                .functionTitle( FUNCTION_TITLE )
                .description( "Automatically partitions data into HOT and COLD based on a selected cost model which is automatically applied to "
                        + "the values of the partition column. "
                        + "Further the data inside the table will be internally partitioned into chunks to apply the cost model on. "
                        + "Therefore a secondary partitioning can be used" )
                .sqlPrefix( "" )
                .sqlSuffix( "" )
                .rowSeparation( "" )
                .rowsBefore( rowsBefore )
                .rowsAfter( rowsAfter )
                .headings( new ArrayList<>( Arrays.asList( "Partition Name", "Classification" ) ) )
                .build();
    }

}
