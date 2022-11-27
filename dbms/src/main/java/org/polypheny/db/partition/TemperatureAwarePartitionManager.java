/*
 * Copyright 2019-2022 The Polypheny Project
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
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.partition.PartitionFunctionInfo.PartitionFunctionInfoColumn;
import org.polypheny.db.partition.PartitionFunctionInfo.PartitionFunctionInfoColumnType;
import org.polypheny.db.partition.properties.TemperaturePartitionProperty;
import org.polypheny.db.type.PolyType;


public class TemperatureAwarePartitionManager extends AbstractPartitionManager {

    public static final boolean REQUIRES_UNBOUND_PARTITION_GROUP = false;
    public static final String FUNCTION_TITLE = "TEMPERATURE";
    public static final List<PolyType> SUPPORTED_TYPES = ImmutableList.of( PolyType.INTEGER, PolyType.BIGINT, PolyType.SMALLINT, PolyType.TINYINT, PolyType.VARCHAR );


    @Override
    public long getTargetPartitionId( CatalogTable catalogTable, String columnValue ) {
        // Get partition manager
        PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
        PartitionManager partitionManager = partitionManagerFactory.getPartitionManager(
                ((TemperaturePartitionProperty) catalogTable.partitionProperty).getInternalPartitionFunction()
        );

        return partitionManager.getTargetPartitionId( catalogTable, columnValue );
    }


    @Override
    public Map<Long, List<CatalogColumnPlacement>> getRelevantPlacements( CatalogTable catalogTable, List<Long> partitionIds, List<Integer> excludedAdapters ) {
        // Get partition manager
        PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
        PartitionManager partitionManager = partitionManagerFactory.getPartitionManager(
                ((TemperaturePartitionProperty) catalogTable.partitionProperty).getInternalPartitionFunction()
        );

        return partitionManager.getRelevantPlacements( catalogTable, partitionIds, excludedAdapters );
    }


    @Override
    public Map<Integer, Map<Long, List<CatalogColumnPlacement>>> getAllPlacements( CatalogTable catalogTable, List<Long> partitionIds ) {
        // Get partition manager
        PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
        PartitionManager partitionManager = partitionManagerFactory.getPartitionManager(
                ((TemperaturePartitionProperty) catalogTable.partitionProperty).getInternalPartitionFunction()
        );

        return partitionManager.getAllPlacements( catalogTable, partitionIds );
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
    public int getNumberOfPartitionsPerGroup( int numberOfPartitions ) {
        return 1;
    }


    @Override
    public boolean validatePartitionGroupSetup( List<List<String>> partitionGroupQualifiers, long numPartitionGroups, List<String> partitionGroupNames, CatalogColumn partitionColumn ) {
        super.validatePartitionGroupSetup( partitionGroupQualifiers, numPartitionGroups, partitionGroupNames, partitionColumn );

        return true;
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
                .options( new ArrayList<>( Arrays.asList( "HASH" ) ) )
                .build() );

        rowsAfter.add( costRow );
        rowsAfter.add( extendedCostRow );
        rowsAfter.add( chunkRow );
        rowsAfter.add( unboundRow );

        // Bring all rows and columns together
        PartitionFunctionInfo uiObject = PartitionFunctionInfo.builder()
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

        return uiObject;
    }

}
