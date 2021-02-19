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


@Slf4j
public class HashPartitionManager extends AbstractPartitionManager {

    public static final boolean ALLOWS_UNBOUND_PARTITION = false;


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
    public String getRequiredUiInputs() {

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


        String returnValue = "";

        //returnValue = getRequiredUiInputsDEBUGHASH();
        returnValue = getRequiredUiInputsDEBUGSample();
        //returnValue = getRequiredUiInputsDEBUGTemperature();



        return returnValue;

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
        nestedColumnPartitionNames.addProperty( "default_value", "" );

        //Second column
        JsonObject nestedColumnLabel = new JsonObject();
        nestedColumnLabel.addProperty( "title", "Classification" );
        nestedColumnLabel.addProperty( "field_type", "label" );
        nestedColumnLabel.addProperty( "mandatory", true );
        nestedColumnLabel.addProperty( "default_value", "newLabel" );


        //Third column
        JsonObject nestedColumnDropdown = new JsonObject();
        nestedColumnDropdown.addProperty( "title", "Options" );
        nestedColumnDropdown.addProperty( "field_type", "dropdown" );
        nestedColumnDropdown.addProperty( "mandatory", true );
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
        jsonDocument.addProperty( "info_tooltip", "Partitions data based on a hash function which is applied to the values of the partition column." );
        jsonDocument.addProperty( "sql_prefix", "WITH (" );
        jsonDocument.addProperty( "sql_suffix", ")" );
        jsonDocument.addProperty( "dynamic_rows", false );
        jsonDocument.addProperty( "row_separation", "," );


        //First nested JSON Doc
        JsonArray columns = new JsonArray();
        JsonArray firstRow = new JsonArray();
        JsonArray secondRow = new JsonArray();

        //First ROW
            //First column
            JsonObject nestedColumnPartitionNamesFirst = new JsonObject();
            nestedColumnPartitionNamesFirst.addProperty( "title", "Partition Names" );
            nestedColumnPartitionNamesFirst.addProperty( "field_type", "text" );
            nestedColumnPartitionNamesFirst.addProperty( "mandatory", false );
            nestedColumnPartitionNamesFirst.addProperty( "default_value", "" );

            //Second column
            JsonObject nestedColumnClassificationFirst = new JsonObject();
            nestedColumnClassificationFirst.addProperty( "title", "Classification" );
            nestedColumnClassificationFirst.addProperty( "field_type", "label" );
            nestedColumnClassificationFirst.addProperty( "mandatory", true );
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
             nestedColumnPartitionNamesSecond.addProperty( "default_value", "" );

            //Second column
            JsonObject nestedColumnClassificationSecond = new JsonObject();
            nestedColumnClassificationSecond.addProperty( "title", "Classification" );
            nestedColumnClassificationSecond.addProperty( "field_type", "label" );
            nestedColumnClassificationSecond.addProperty( "mandatory", true );
            nestedColumnClassificationSecond.addProperty( "default_value", "COLD" );




            secondRow.add( nestedColumnPartitionNamesSecond );
            secondRow.add( nestedColumnClassificationSecond );

            columns.add( firstRow );
            columns.add( secondRow );

        jsonDocument.add( "columns", columns );

        generateSampleSQL(jsonDocument);

        // ToDO @HENNLO return JsonObject not String
        return jsonDocument.toString();
    }


    @Override
    public boolean allowsUnboundPartition() {
        return ALLOWS_UNBOUND_PARTITION;
    }

}
