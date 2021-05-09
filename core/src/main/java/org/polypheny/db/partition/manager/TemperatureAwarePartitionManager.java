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

package org.polypheny.db.partition.manager;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.partition.PartitionFunctionInfo;
import org.polypheny.db.partition.PartitionFunctionInfo.PartitionFunctionInfoColumn;
import org.polypheny.db.partition.PartitionFunctionInfo.PartitionFunctionInfoColumnType;
import org.polypheny.db.type.PolyType;


public class TemperatureAwarePartitionManager extends AbstractPartitionManager{

    public static final boolean REQUIRES_UNBOUND_PARTITION = false;
    public static final String FUNCTION_TITLE = "TEMPERATURE";

    //TODO HENNLO central config to define the thresholds when data is considered hot and when cold (15% and 20%)

    //TODO also define default Settings
    //E.g. HASH partitioning if nothing else is specified, or  cost model = access frequency


    @Override
    public long getTargetPartitionId( CatalogTable catalogTable, String columnValue ) {
        return 0;
    }


    @Override
    public boolean probePartitionDistributionChange( CatalogTable catalogTable, int storeId, long columnId ) {
        return false;
    }


    @Override
    public List<CatalogColumnPlacement> getRelevantPlacements( CatalogTable catalogTable, List<Long> partitionIds ) {
        return null;
    }


    @Override
    public boolean requiresUnboundPartition() {
        return false;
    }


    @Override
    public boolean supportsColumnOfType( PolyType type ) {
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
                .sqlPrefix( "" )
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
                .sqlPrefix( "" )
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


        rowsBefore.add( hotRow );
        rowsBefore.add( coldRow );



        //COST MODEL
        //Fixed rows to display after dynamically generated ones
        List<List<PartitionFunctionInfoColumn>> rowsAfter = new ArrayList<>();

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
                .fieldType( PartitionFunctionInfoColumnType.STRING )
                .mandatory( false )
                .modifiable( false )
                .sqlPrefix( "" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .defaultValue( "HASH" )
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
                .modifiable( false )
                .sqlPrefix( "" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .defaultValue( "20" )
                .build() );


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
                .sqlPrefix( "" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .options(new ArrayList<>( Arrays.asList( "Total Access Frequency", "Write Frequency", "Read Frequency" )  ))
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
                .sqlPrefix( "" )
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
                .options(new ArrayList<>( Arrays.asList( "Minutes", "Hours", "Days" )  ))
                .build() );




        rowsAfter.add( unboundRow );
        rowsAfter.add( chunkRow );
        rowsAfter.add( costRow );
        rowsAfter.add( extendedCostRow );




        //Bring all rows and columns together
        PartitionFunctionInfo uiObject = PartitionFunctionInfo.builder()
                .functionTitle( FUNCTION_TITLE )
                .description( "Automatically partitions data into HOT and COLD based on a selected cost model which is automatically applied to "
                        + "the values of the partition column. "
                        + "Further the data inside the table will be internally partitioned into chunks to apply the cost model on. "
                        + "Therefore a secondary partitioning can be used" )
                .sqlPrefix( "WITH (" )
                .sqlSuffix( ")" )
                .rowSeparation( "," )
                .rowsBefore( rowsBefore )
                .rowsAfter( rowsAfter )
                .headings( new ArrayList<>( Arrays.asList( "Partition Name", "Classification" ) ) )
                .build();





        return uiObject;
    }
}
