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
public abstract class AbstractPartitionManager implements PartitionManager {


    // returns the Index of the partition where to place the object
    @Override
    public abstract long getTargetPartitionId( CatalogTable catalogTable, String columnValue );


    /**
     * Validates the table if the partitions are sufficiently distributed.
     * There has to be at least on columnPlacement which contains all partitions
     *
     * @param table Table to be checked
     * @return If its correctly distributed or not
     */
    @Override
    public boolean validatePartitionDistribution( CatalogTable table ) {
        // Check for every column if there exists at least one placement which contains all partitions
        for ( long columnId : table.columnIds ) {
            int numberOfFullPlacements = getPlacementsWithAllPartitions( columnId, table.numPartitions ).size();
            if ( numberOfFullPlacements >= 1 ) {
                log.debug( "Found ColumnPlacement which contains all partitions for column: {}", columnId );
                break;
            }

            if ( log.isDebugEnabled() ) {
                log.debug( "ERROR Column: '{}' has no placement containing all partitions", Catalog.getInstance().getColumn( columnId ).name );
            }
            return false;
        }

        return true;
    }


    @Override
    public abstract boolean probePartitionDistributionChange( CatalogTable catalogTable, int storeId, long columnId );

    @Override
    public abstract List<CatalogColumnPlacement> getRelevantPlacements( CatalogTable catalogTable, List<Long> partitionIds );


    @Override
    public boolean validatePartitionSetup(
            List<List<String>> partitionQualifiers,
            long numPartitions,
            List<String> partitionNames,
            CatalogColumn partitionColumn ) {
        if ( numPartitions == 0 && partitionNames.size() < 2 ) {
            throw new RuntimeException( "Partitioning of table failed! Can't partition table with less than 2 partitions/names" );
        }
        return true;
    }


    /**
     * Returns number of placements for this column which contain all partitions
     *
     * @param columnId column to be checked
     * @param numPartitions numPartitions
     * @return If its correctly distributed or not
     */
    protected List<CatalogColumnPlacement> getPlacementsWithAllPartitions( long columnId, long numPartitions ) {
        Catalog catalog = Catalog.getInstance();

        // Return every placement of this column
        List<CatalogColumnPlacement> tempCcps = catalog.getColumnPlacements( columnId );
        List<CatalogColumnPlacement> returnCcps = new ArrayList<>();
        int placementCounter = 0;
        for ( CatalogColumnPlacement ccp : tempCcps ) {
            // If the DataPlacement has stored all partitions and therefore all partitions for this placement
            if ( catalog.getPartitionsOnDataPlacement( ccp.adapterId, ccp.tableId ).size() == numPartitions ) {
                returnCcps.add( ccp );
                placementCounter++;
            }
        }
        return returnCcps;
    }

    @Override
    public abstract String getRequiredUiInputs();
    // ToDO: @HENNLO Maybe enforce and fillout mandatory keys on top level here and only fill out body in specialized classes


    protected void generateSampleSQL( JsonObject json ){

        System.out.println("Number of Top Level Keys: "+ json.size());

        String function_title = json.get( "function_title" ).getAsString();
        String info_tooltip = json.get( "info_tooltip" ).getAsString();
        String sql_prefix = json.get( "sql_prefix" ).getAsString();
        String sql_suffix = json.get( "sql_suffix" ).getAsString();
        boolean dynamic_rows = json.get( "dynamic_rows" ).getAsBoolean();
        String row_separation = json.get( "row_separation" ).getAsString();

        System.out.println("Function Title: "+ function_title);
        System.out.println("Info Tooltip: "+ info_tooltip + "\n");
        System.out.println("SQL Construct: "+ sql_prefix + " " + sql_suffix);

        System.out.println("Use Dynamic Rows: "+ dynamic_rows);
        System.out.println("Row Separation Character: "+ row_separation);


        //Will be selected in UI
        int numPartitions = 3;

        String content = "";

        System.out.println("\n___________________________________________________");
        if ( dynamic_rows ){
            if ( !json.has( "columns" ) ){
                throw new RuntimeException("UI-based Partition Definition for function: " + function_title + " is ill-defined");
            }

            JsonArray columns = json.get( "columns" ).getAsJsonArray();

            for ( int i = 0; i < numPartitions; i++ ){

                String header = "";
                String fieldUI = "";
                for ( int j = 0; j <columns.size() ; j++ ) {

                    JsonObject currentJson = columns.get( j ).getAsJsonObject();
                    //First round - print headers

                    header = header + "\t\t\t" + currentJson.get( "title" );

                    String field_type = currentJson.get( "field_type" ).getAsString();


                    switch ( field_type ) {
                        case "text":
                            fieldUI = fieldUI + "\t\t\t[______]";
                            break;

                        case "dropdown":
                            fieldUI = fieldUI + "\t\t\t[    |v]";
                            //If dropdown was selected than pick options value which is also array
                            break;

                        case "label":
                            fieldUI = fieldUI + "\t\t\t" + currentJson.get( "default_value" ).getAsString();
                            break;

                        default:
                            System.out.println( "Unknown: field_type: '" + field_type + "' " );
                            break;
                    }


                }

                if ( i == 0 ) {
                    System.out.println(header);
                }
                System.out.println( (i + 1) + ".  \t\t" + fieldUI );
            }


        }else{
            System.out.println("Static-Rows: Not implemented yet -- Go line by line");


            if ( !json.has( "columns" ) ){
                throw new RuntimeException("UI-based Partition Definition for function: " + function_title + " is ill-defined");
            }

            JsonArray columns = json.get( "columns" ).getAsJsonArray();
            numPartitions = columns.size();
            System.out.println(numPartitions);
            for ( int i = 0; i < numPartitions; i++ ){

                //HERE
                JsonArray currentRow = columns.get( i ).getAsJsonArray();

                String header = "";
                String fieldUI = "";
                for ( int j = 0; j <currentRow.size() ; j++ ) {

                    //HERE
                    JsonObject currentJson= columns.get( j ).getAsJsonObject();


                    header = header + "\t\t\t" + currentJson.get( "title" );

                    String field_type = currentJson.get( "field_type" ).getAsString();


                    switch ( field_type ) {
                        case "text":
                            fieldUI = fieldUI + "\t\t\t[______]";
                            break;

                        case "dropdown":
                            fieldUI = fieldUI + "\t\t\t[    |v]";
                            //If dropdown was selected than pick options value which is also array
                            break;

                        case "label":
                            fieldUI = fieldUI + "\t\t\t" + currentJson.get( "default_value" ).getAsString();
                            break;

                        default:
                            System.out.println( "Unknown: field_type: '" + field_type + "' " );
                            break;
                    }


                }

                if ( i == 0 ) {
                    System.out.println(header);
                }
                System.out.println( (i + 1) + ".  \t\t" + fieldUI );
            }


        }
        System.out.println("\n___________________________________________________");

        System.out.println("\nSQL: ALTER TABLE dummy PARTITION BY "+ function_title + " (column) " + sql_prefix + " " + content + " " +  sql_suffix + "\n");

    }
}
