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

import com.fasterxml.jackson.databind.util.JSONPObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.partition.PartitionFunctionInfo.Column;


@Slf4j
public class HashPartitionManager extends AbstractPartitionManager {

    public static final boolean ALLOWS_UNBOUND_PARTITION = false;
    public static final String function_title = "HASH";


    @Override
    public long getTargetPartitionId( CatalogTable catalogTable, String columnValue ) {
        long partitionID = columnValue.hashCode() * -1;

        // Don't want any neg. value for now
        if ( partitionID <= 0 ) {
            partitionID *= -1;
        }

        // Finally decide on which partition to put it
        return catalogTable.partitionIds.get( (int) (partitionID % catalogTable.numPartitions) );
    }


    // Needed when columnPlacements are being dropped
    // HASH Partitioning needs at least one column placement which contains all partitions as a fallback
    @Override
    public boolean probePartitionDistributionChange( CatalogTable catalogTable, int storeId, long columnId ) {

        // Change is only critical if there is only one column left with the characteristics
        int numberOfFullPlacements = getPlacementsWithAllPartitions( columnId, catalogTable.numPartitions ).size();
        if ( numberOfFullPlacements <= 1 ) {
            Catalog catalog = Catalog.getInstance();
            //Check if this one column is the column we are about to delete
            if ( catalog.getPartitionsOnDataPlacement( storeId, catalogTable.id ).size() == catalogTable.numPartitions ) {
                return false;
            }
        }

        return true;
    }


    @Override
    public List<CatalogColumnPlacement> getRelevantPlacements( CatalogTable catalogTable, List<Long> partitionIds ) {

        List<CatalogColumnPlacement> relevantCcps = new ArrayList<>();
        // Find stores with full placements (partitions)
        // Pick for each column the column placement which has full partitioning //SELECT WORST-CASE ergo Fallback
        for ( long columnId : catalogTable.columnIds ) {
            // Take the first column placement
            relevantCcps.add( getPlacementsWithAllPartitions( columnId, catalogTable.numPartitions ).get( 0 ) );
        }

        return relevantCcps;
    }


    @Override
    public boolean validatePartitionSetup( List<List<String>> partitionQualifiers, long numPartitions, List<String> partitionNames, CatalogColumn partitionColumn ) {
        super.validatePartitionSetup( partitionQualifiers, numPartitions, partitionNames, partitionColumn );

        //ToDO @HENNLO Debug
        System.out.println(getRequiredUiInputs());

        if ( !partitionQualifiers.isEmpty() ) {
            throw new RuntimeException( "PartitionType HASH does not support the assignment of values to partitions" );
        }
        if ( numPartitions < 2 ) {
            throw new RuntimeException( "You can't partition a table with less than 2 partitions. You only specified: '" + numPartitions + "'" );
        }

        return true;
    }


    @Override
    public PartitionFunctionInfo getRequiredUiInputs() {

        //Could be considered in value generation

        /* Example JSON. Use for templating

            {
                "function_title" : "HASH",
                "info_tooltip": "STRING with information for example what this partition does and if it has special behaviours (unbound partition)"
                "columns" : <2..n>  - The first column is always the index (1..n) where n is numPartitions specified in first modal at `add data partitioning` can be used to iterate with for to know how many params you are looking for
                maybe just make columns and sub documents

                "columns" : [
                                {
                                        "title" : "Partition Names"
                                        "field_type" : "text/dropdown/label"
                                        "options": [1,2,3] (only relevant if `"field_type" : "dropdown"` selected)
                                        "maxValue" : ...
                                        "mandatory" : "FALSE"
                                        "info_tooltip" : "Example Text when hovering over info tooltip to help the user"
                                        "default_value" : "Part_<0..numPartitions>"
                                        "value_separation" : ",/;",
                                        "sql_construct" : "VALUES()"
                                        "fixed_size": "NONE/1,2,3,4,..."
                                },


                            ]

                "numPartitions" :
                                    {
                                        "type" : "INTEGER"
                                        "maxValue" : ...
                                        "mandatory" : "TRUE/FALSE"
                                        "info_tooltip" : "Example Text when hovering over info tooltip to help the user"
                                        "fixed_size": "NONE/1,2,3,4,..."
                                    },
                "partitionNames" :
                                    {
                                        "type" : "String"
                                        "maxValue" : ...
                                        "mandatory" : "FALSE"
                                        "info_tooltip" : "Example Text when hovering over info tooltip to help the user"
                                        "default_value" : "Part_<0..numPartitions>"
                                        "fixed_size": "NONE/1,2,3,4,..."
                                    },
                "exampleField" :
                                    {
                                        "inputType" : "INTEGER"
                                        "fieldType" : "TEXTBOX,DROPDOWN,BUTTON"
                                        "maxValue" : ...
                                        "mandatory" : "TRUE/FALSE"
                                        "info_tooltip" : "Example Text when hovering over info tooltip to help the user"
                                        "fixed_size": NONE/1,2,3,4,...,
                                        "SQL_Encapsulation",
                                        "options": [option]
                                    }

            }

            Should at the end create table: ALTER TABLE depo
                                            PARTITION BY LIST (num)
                                            (PARTITION A892_233 VALUES(892, 233),
                                            PARTITION a1001_1002 VALUES(1001, 1002),
                                            PARTITION a8000_4003 VALUES(8000, 4003),
                                            PARTITION a900_999 VALUES(900, 999)
                                            )

                                            especially the part in parenthesis





                EXAMPLE HASH to generate SQL alter table we partition by hash (msg) with (partitionNames)

                {
                "function_title" : "HASH",
                "info_tooltip" : "Partitions data based on a hash function which is applied to the values of the partition column."
                "sql_prefix" : "WITH (",
                sql_suffix" : ")",
                "dynamic_rows" : "TRUE",
                "row_separation" : ",",
                "columns" : [
                                {
                                        "title" : "Partition Names"
                                        "field_type" : "text"
                                        "mandatory" : true
                                        "info_tooltip" : "Example Text when hovering over info tooltip to help the user"
                                        "default_value" : ""
                                },


         */



        //Dynamic content which will be generated by selected numPartitions
        List<Column> dynamicRows = new ArrayList<>();
        dynamicRows.add( Column.builder()
                .title( "Partition Names" )
                .fieldType( "text" )
                .mandatory( true )
                .modifiable( true )
                .sqlPrefix( "" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .defaultValue( "partition_name" )
                .build() );



        //Fixed rows to display before
        List<List<Column>> rowsBefore = new ArrayList<>();
        List<Column> firstTestRow = new ArrayList<>();
        firstTestRow.add( Column.builder()
                .title( "Partition Names" )
                .fieldType( "text" )
                .mandatory( true )
                .modifiable( true )
                .sqlPrefix( "" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .defaultValue( "partition_name" )
                .build() );

        firstTestRow.add( Column.builder()
                .title( "Another Value" )
                .fieldType( "text" )
                .mandatory( true )
                .modifiable( true )
                .sqlPrefix( "" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .defaultValue( "partition_name" )
                .build() );

        List<Column> secondTestRow = new ArrayList<>();
        secondTestRow.add( Column.builder()
                .title( "Second Row" )
                .fieldType( "text" )
                .mandatory( true )
                .modifiable( true )
                .sqlPrefix( "" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .defaultValue( "partition_name" )
                .build() );

        rowsBefore.add( firstTestRow );
        rowsBefore.add( secondTestRow );



        PartitionFunctionInfo uiObject = PartitionFunctionInfo.builder()
                .functionTitle( function_title )
                .uiTooltip( "Partitions data based on a hash function which is applied to the values of the partition column." )
                .sqlPrefix( "WITH (" )
                .sqlSuffix( ")" )
                .rowSeparation( "," )
                .rowsBefore( rowsBefore )
                .dynamicRows( dynamicRows )
                .build();





        String returnValue = "";

        //returnValue = getRequiredUiInputsDEBUGHASH();
        //returnValue = getRequiredUiInputsDEBUGSample();
        //returnValue = getRequiredUiInputsDEBUGTemperature();
        //returnValue = getRequiredUiInputsDEBUGList();
        //returnValue = getRequiredUiInputsDEBUGRange();



        return uiObject;

    }


    public String getRequiredUiInputsDEBUGHASH() {

        //Outer Layer - Top Level
        JsonObject jsonDocument = new JsonObject();

        jsonDocument.addProperty( "function_title", "HASH" );
        jsonDocument.addProperty( "info_tooltip", "Partitions data based on a hash function which is applied to the values of the partition column." );
        jsonDocument.addProperty( "sql_prefix", "WITH (" );
        jsonDocument.addProperty( "sql_suffix", ")" );
        jsonDocument.addProperty( "dynamic_rows", true );
        jsonDocument.addProperty( "row_separation", "," );


        //First nested JSON Doc
        JsonObject nestedJsonColumns = new JsonObject();
        JsonArray columns = new JsonArray();

        //First column
        JsonObject nestedColumnPartitionNames = new JsonObject();
        nestedColumnPartitionNames.addProperty( "title", "Partition Names" );
        nestedColumnPartitionNames.addProperty( "field_type", "text" );
        nestedColumnPartitionNames.addProperty( "mandatory", true );
        nestedColumnPartitionNames.addProperty( "sql_prefix", "" );
        nestedColumnPartitionNames.addProperty( "sql_suffix", "" );
        nestedColumnPartitionNames.addProperty( "default_value", "" );


        // add all repetetive columns to top-level columns element
        columns.add( nestedColumnPartitionNames );


        jsonDocument.add( "columns", columns );

        generateSampleSQL(jsonDocument);

        // ToDO @HENNLO return JsonObject not String
        return jsonDocument.toString();
    }

    public String getRequiredUiInputsDEBUGSample() {

        //Outer Layer - Top Level
        JsonObject jsonDocument = new JsonObject();

        jsonDocument.addProperty( "function_title", "SAMPLE" );
        jsonDocument.addProperty( "info_tooltip", "Partitions data based on a hash function which is applied to the values of the partition column." );
        jsonDocument.addProperty( "sql_prefix", "WITH (" );
        jsonDocument.addProperty( "sql_suffix", ")" );
        jsonDocument.addProperty( "dynamic_rows", true );
        jsonDocument.addProperty( "row_separation", "," );


        //First nested JSON Doc
        JsonObject nestedJsonColumns = new JsonObject();
        JsonArray columns = new JsonArray();

        //First column
        JsonObject nestedColumnPartitionNames = new JsonObject();
        nestedColumnPartitionNames.addProperty( "title", "Partition Names" );
        nestedColumnPartitionNames.addProperty( "field_type", "text" );
        nestedColumnPartitionNames.addProperty( "mandatory", true );
        nestedColumnPartitionNames.addProperty( "sql_prefix", "" );
        nestedColumnPartitionNames.addProperty( "sql_suffix", "" );
        nestedColumnPartitionNames.addProperty( "default_value", "" );

        //Second column
        JsonObject nestedColumnLabel = new JsonObject();
        nestedColumnLabel.addProperty( "title", "Classification" );
        nestedColumnLabel.addProperty( "field_type", "label" );
        nestedColumnLabel.addProperty( "mandatory", true );
        nestedColumnLabel.addProperty( "sql_prefix", "" );
        nestedColumnLabel.addProperty( "sql_suffix", "" );
        nestedColumnLabel.addProperty( "default_value", "newLabel" );


        //Third column
        JsonObject nestedColumnDropdown = new JsonObject();
        nestedColumnDropdown.addProperty( "title", "Options" );
        nestedColumnDropdown.addProperty( "field_type", "dropdown" );
        nestedColumnDropdown.addProperty( "mandatory", true );
        nestedColumnDropdown.addProperty( "sql_prefix", "" );
        nestedColumnDropdown.addProperty( "sql_suffix", "" );
        nestedColumnDropdown.addProperty( "default_value", "" );

        // add all repetetive columns to top-level columns element
        columns.add( nestedColumnPartitionNames );
        columns.add( nestedColumnLabel );
        columns.add( nestedColumnDropdown );

        jsonDocument.add( "columns", columns );

        generateSampleSQL(jsonDocument);

        // ToDO @HENNLO return JsonObject not String
        return jsonDocument.toString();
    }

    public String getRequiredUiInputsDEBUGTemperature() {

        //Outer Layer - Top Level
        JsonObject jsonDocument = new JsonObject();

        jsonDocument.addProperty( "function_title", "TEMPERATURE" );
        jsonDocument.addProperty( "info_tooltip", "Partitions data based on a temperature base function usind a prepared metric which is applied to the values of the partition column." );
        jsonDocument.addProperty( "sql_prefix", "(" );
        jsonDocument.addProperty( "sql_suffix", ")" );
        jsonDocument.addProperty( "dynamic_rows", false );
        jsonDocument.addProperty( "row_separation", "," );


        //First nested JSON Doc
        JsonArray columns = new JsonArray();
        JsonArray firstRow = new JsonArray();
        JsonArray secondRow = new JsonArray();
        JsonArray thirdRow = new JsonArray();

        //First ROW
            //First column
            JsonObject nestedColumnPartitionNamesFirst = new JsonObject();
            nestedColumnPartitionNamesFirst.addProperty( "title", "Partition Names" );
            nestedColumnPartitionNamesFirst.addProperty( "field_type", "text" );
            nestedColumnPartitionNamesFirst.addProperty( "mandatory", false );
            nestedColumnPartitionNamesFirst.addProperty( "sql_prefix", "PARTITION" );
            nestedColumnPartitionNamesFirst.addProperty( "sql_suffix", "" );
            nestedColumnPartitionNamesFirst.addProperty( "default_value", "" );

            //Second column
            JsonObject nestedColumnClassificationFirst = new JsonObject();
            nestedColumnClassificationFirst.addProperty( "title", "Classification" );
            nestedColumnClassificationFirst.addProperty( "field_type", "label" );
            nestedColumnClassificationFirst.addProperty( "mandatory", true );
            nestedColumnClassificationFirst.addProperty( "sql_prefix", "as" );
            nestedColumnClassificationFirst.addProperty( "sql_suffix", "" );
            nestedColumnClassificationFirst.addProperty( "default_value", "HOT" );

            // add all repetetive columns to top-level columns element
            firstRow.add( nestedColumnPartitionNamesFirst );
            firstRow.add( nestedColumnClassificationFirst );


        //Second ROW
            //First column
             JsonObject nestedColumnPartitionNamesSecond = new JsonObject();
             nestedColumnPartitionNamesSecond.addProperty( "title", "Partition Names" );
             nestedColumnPartitionNamesSecond.addProperty( "field_type", "text" );
             nestedColumnPartitionNamesSecond.addProperty( "mandatory", false );
             nestedColumnPartitionNamesSecond.addProperty( "sql_prefix", "PARTITION" );
             nestedColumnPartitionNamesSecond.addProperty( "sql_suffix", "" );
             nestedColumnPartitionNamesSecond.addProperty( "default_value", "" );

            //Second column
            JsonObject nestedColumnClassificationSecond = new JsonObject();
            nestedColumnClassificationSecond.addProperty( "title", "Classification" );
            nestedColumnClassificationSecond.addProperty( "field_type", "label" );
            nestedColumnClassificationSecond.addProperty( "mandatory", true );
            nestedColumnClassificationSecond.addProperty( "sql_prefix", "as" );
            nestedColumnClassificationSecond.addProperty( "sql_suffix", "" );
            nestedColumnClassificationSecond.addProperty( "default_value", "COLD" );

            secondRow.add( nestedColumnPartitionNamesSecond );
            secondRow.add( nestedColumnClassificationSecond );


            //Third column

            JsonObject nestedColumnLabelThird = new JsonObject();
            nestedColumnLabelThird.addProperty( "title", "Label" );
            nestedColumnLabelThird.addProperty( "field_type", "label" );
            nestedColumnLabelThird.addProperty( "default_value", "Using Metric: " );
            nestedColumnLabelThird.addProperty( "mandatory", false );
            nestedColumnLabelThird.addProperty( "sql_prefix", "" );
            nestedColumnLabelThird.addProperty( "sql_suffix", "" );


            JsonObject nestedColumnClassificationThird = new JsonObject();
            nestedColumnClassificationThird.addProperty( "title", "Classification" );
            nestedColumnClassificationThird.addProperty( "field_type", "dropdown" );


            JsonArray options = new JsonArray();
            options.add( "access_frequency" );
            options.add( "write_frequency" );
            options.add( "read_frequency" );

            nestedColumnClassificationThird.add( "options", options );

            nestedColumnClassificationThird.addProperty( "mandatory", false );
            nestedColumnClassificationThird.addProperty( "sql_prefix", "USING METRIC" );
            nestedColumnClassificationThird.addProperty( "sql_suffix", "" );
            nestedColumnClassificationThird.addProperty( "default_value", "" );

            thirdRow.add( nestedColumnLabelThird );
            thirdRow.add( nestedColumnClassificationThird );



            columns.add( firstRow );
            columns.add( secondRow );
            columns.add( thirdRow );

        jsonDocument.add( "columns", columns );

        generateSampleSQL(jsonDocument);

        // ToDO @HENNLO return JsonObject not String
        return jsonDocument.toString();
    }


    public String getRequiredUiInputsDEBUGList() {

        //Outer Layer - Top Level
        JsonObject jsonDocument = new JsonObject();

        jsonDocument.addProperty( "function_title", "LIST" );
        jsonDocument.addProperty( "info_tooltip", "Partitions data based on a list of values which is assigned to a specific partition." );
        jsonDocument.addProperty( "sql_prefix", "(" );
        jsonDocument.addProperty( "sql_suffix", ")" );
        jsonDocument.addProperty( "dynamic_rows", true );
        jsonDocument.addProperty( "row_separation", "," );


        //First nested JSON Doc
        JsonArray columns = new JsonArray();

        //First column
        JsonObject nestedColumnPartitionNames = new JsonObject();
        nestedColumnPartitionNames.addProperty( "title", "Partition Names" );
        nestedColumnPartitionNames.addProperty( "field_type", "text" );
        nestedColumnPartitionNames.addProperty( "mandatory", true );
        nestedColumnPartitionNames.addProperty( "sql_prefix", "PARTITION" );
        nestedColumnPartitionNames.addProperty( "sql_suffix", "" );
        nestedColumnPartitionNames.addProperty( "default_value", "" );

        //Second column
        JsonObject nestedColumnLabel = new JsonObject();
        nestedColumnLabel.addProperty( "title", "Values" );
        nestedColumnLabel.addProperty( "field_type", "text" );
        nestedColumnLabel.addProperty( "mandatory", true );
        nestedColumnLabel.addProperty( "default_value", "newLabel" );
        nestedColumnLabel.addProperty( "value_separation", "," );
        nestedColumnLabel.addProperty( "sql_prefix", "VALUES(" );
        nestedColumnLabel.addProperty( "sql_suffix", ")" );


        // add all repetetive columns to top-level columns element
        columns.add( nestedColumnPartitionNames );
        columns.add( nestedColumnLabel );


        jsonDocument.add( "columns", columns );



        //Display Only columns
        JsonArray display_only_after = new JsonArray();

        //First Row
        JsonArray displayFirstRow = new JsonArray();

        JsonObject nestedDisplayUnboundName = new JsonObject();
        nestedDisplayUnboundName.addProperty( "title", "Unbound" );
        nestedDisplayUnboundName.addProperty( "field_type", "label" );
        nestedDisplayUnboundName.addProperty( "default_value", "UNBOUND" );
        nestedDisplayUnboundName.addProperty( "modifiable", false );
        nestedDisplayUnboundName.addProperty( "sql_prefix", "" );
        nestedDisplayUnboundName.addProperty( "sql_suffix", "" );


        JsonObject nestedDisplayUnboundValue = new JsonObject();
        nestedDisplayUnboundValue.addProperty( "title", "Unbound" );
        nestedDisplayUnboundValue.addProperty( "field_type", "text" );
        nestedDisplayUnboundValue.addProperty( "default_value", "UNBOUND" );
        nestedDisplayUnboundValue.addProperty( "modifiable", false );
        nestedDisplayUnboundValue.addProperty( "sql_prefix", "" );
        nestedDisplayUnboundValue.addProperty( "sql_suffix", "" );


        displayFirstRow.add( nestedDisplayUnboundName );
        displayFirstRow.add( nestedDisplayUnboundValue );

        display_only_after.add( displayFirstRow );

        jsonDocument.add( "display_only_after", display_only_after );

        generateSampleSQL(jsonDocument);

        // ToDO @HENNLO return JsonObject not String
        return jsonDocument.toString();
    }


    public String getRequiredUiInputsDEBUGRange() {

        //Outer Layer - Top Level
        JsonObject jsonDocument = new JsonObject();

        jsonDocument.addProperty( "function_title", "RANGE" );
        jsonDocument.addProperty( "info_tooltip", "Partitions data based on a RANGE using min and max values per range. The rest of the values goes to defualt UNBOUND partition as a fallback." );
        jsonDocument.addProperty( "sql_prefix", "(" );
        jsonDocument.addProperty( "sql_suffix", ")" );
        jsonDocument.addProperty( "dynamic_rows", true );
        jsonDocument.addProperty( "row_separation", "," );


        //First nested JSON Doc
        JsonArray columns = new JsonArray();

        //First column
        JsonObject nestedColumnPartitionNames = new JsonObject();
        nestedColumnPartitionNames.addProperty( "title", "Partition Names" );
        nestedColumnPartitionNames.addProperty( "field_type", "text" );
        nestedColumnPartitionNames.addProperty( "mandatory", true );
        nestedColumnPartitionNames.addProperty( "sql_prefix", "PARTITION" );
        nestedColumnPartitionNames.addProperty( "sql_suffix", "" );
        nestedColumnPartitionNames.addProperty( "default_value", "" );

        //Second column
        JsonObject nestedColumnValueMIN = new JsonObject();
        nestedColumnValueMIN.addProperty( "title", "MIN" );
        nestedColumnValueMIN.addProperty( "field_type", "text" );
        nestedColumnValueMIN.addProperty( "mandatory", true );
        nestedColumnValueMIN.addProperty( "default_value", "newLabel" );
        nestedColumnValueMIN.addProperty( "value_separation", "," );
        nestedColumnValueMIN.addProperty( "sql_prefix", "VALUES(" );
        nestedColumnValueMIN.addProperty( "sql_suffix", "" );

        //Third column
        JsonObject nestedColumnValueMAX = new JsonObject();
        nestedColumnValueMAX.addProperty( "title", "MAX" );
        nestedColumnValueMAX.addProperty( "field_type", "text" );
        nestedColumnValueMAX.addProperty( "mandatory", true );
        nestedColumnValueMAX.addProperty( "default_value", "newLabel" );
        nestedColumnValueMAX.addProperty( "value_separation", "," );
        nestedColumnValueMAX.addProperty( "sql_prefix", ", " );
        nestedColumnValueMAX.addProperty( "sql_suffix", ")" );


        // add all repetetive columns to top-level columns element
        columns.add( nestedColumnPartitionNames );
        columns.add( nestedColumnValueMIN );
        columns.add( nestedColumnValueMAX );


        jsonDocument.add( "columns", columns );



        //Display Only columns
        JsonArray display_only_after = new JsonArray();

        //First Row
        JsonArray displayFirstRow = new JsonArray();

        JsonObject nestedDisplayUnboundName = new JsonObject();
        nestedDisplayUnboundName.addProperty( "title", "Unbound" );
        nestedDisplayUnboundName.addProperty( "field_type", "label" );
        nestedDisplayUnboundName.addProperty( "default_value", "UNBOUND" );
        nestedDisplayUnboundName.addProperty( "modifiable", false );
        nestedDisplayUnboundName.addProperty( "sql_prefix", "" );
        nestedDisplayUnboundName.addProperty( "sql_suffix", "" );


        JsonObject nestedDisplayUnboundValueMin = new JsonObject();
        nestedDisplayUnboundValueMin.addProperty( "title", "Unbound Min" );
        nestedDisplayUnboundValueMin.addProperty( "field_type", "text" );
        nestedDisplayUnboundValueMin.addProperty( "default_value", "UNBOUND" );
        nestedDisplayUnboundValueMin.addProperty( "modifiable", false );
        nestedDisplayUnboundValueMin.addProperty( "sql_prefix", "" );
        nestedDisplayUnboundValueMin.addProperty( "sql_suffix", "" );

        JsonObject nestedDisplayUnboundValueMax = new JsonObject();
        nestedDisplayUnboundValueMax.addProperty( "title", "Unbound Max" );
        nestedDisplayUnboundValueMax.addProperty( "field_type", "text" );
        nestedDisplayUnboundValueMax.addProperty( "default_value", "UNBOUND" );
        nestedDisplayUnboundValueMax.addProperty( "modifiable", false );
        nestedDisplayUnboundValueMax.addProperty( "sql_prefix", "" );
        nestedDisplayUnboundValueMax.addProperty( "sql_suffix", "" );


        displayFirstRow.add( nestedDisplayUnboundName );
        displayFirstRow.add( nestedDisplayUnboundValueMin );
        displayFirstRow.add( nestedDisplayUnboundValueMax );

        display_only_after.add( displayFirstRow );

        jsonDocument.add( "display_only_after", display_only_after );

        generateSampleSQL(jsonDocument);

        // ToDO @HENNLO return JsonObject not String
        return jsonDocument.toString();
    }

    @Override
    public boolean allowsUnboundPartition() {
        return ALLOWS_UNBOUND_PARTITION;
    }

}
