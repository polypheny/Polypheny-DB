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

package org.polypheny.db.adapter.googlesheet;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.GridProperties;
import com.google.api.services.sheets.v4.model.Sheet;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.ValueRange;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.util.Pair;


/**
 * Class that scans the Google Sheet directly using the GoogleSheetApi.
 */
public class GoogleSheetReader {

    private final URL url;
    private final int querySize;
    private final Pair<String, String> oAuthIdKey;
    private final HashMap<String, List<Object>> tableSurfaceData = new HashMap<>();
    private final HashMap<String, List<List<Object>>> tableData = new HashMap<>();
    private final HashMap<String, Integer> tableStart = new HashMap<>();
    private final HashMap<String, Integer> tableLeftOffset = new HashMap<>();
    private final HashMap<String, Integer> enumPointer = new HashMap<>();
    private final GoogleSheetSource googleSheetSource;


    /**
     * @param url URL of the Google Sheet to source.
     * @param querySize Size of the query (in case of large files)
     */
    public GoogleSheetReader( URL url, int querySize, GoogleSheetSource googleSheetSource ) {
        this.url = url;
        this.querySize = querySize;
        this.googleSheetSource = googleSheetSource;
        this.oAuthIdKey = Pair.of( googleSheetSource.clientId, googleSheetSource.clientKey );
    }


    /**
     * Finds first row for field names
     */
    private void readTableSurfaceData() {
        if ( !tableSurfaceData.isEmpty() ) {
            return;
        }
        try {
            final String spreadsheetId = parseUrlToString( url );
            Sheets service = GoogleSheetSource.getSheets( oAuthIdKey, googleSheetSource );

            // Get the properties of all the sheets
            Spreadsheet s = service.spreadsheets().get( spreadsheetId ).execute();

            for ( Sheet sheet : s.getSheets() ) {
                String sheetName = sheet.getProperties().getTitle();
                GridProperties gp = sheet.getProperties().getGridProperties();
                int queryStartRow = 1;
                while ( true ) {
                    if ( queryStartRow > gp.getRowCount() ) { // nothing in the sheet
                        break;
                    }
                    int queryEndRow = queryStartRow + querySize - 1;
                    String sheetRange = sheetName + "!" + queryStartRow + ":" + queryEndRow;
                    ValueRange response = service.spreadsheets().values()
                            .get( spreadsheetId, sheetRange )
                            .execute();

                    List<List<Object>> values = response.getValues();
                    if ( values == null ) { // no rows had values
                        queryStartRow += querySize;
                        continue;
                    }
                    for ( List<Object> row : values ) { // found at least 1 row
                        if ( row.size() != 0 ) {
                            for ( int i = 0; i < row.size(); i++ ) {
                                if ( !Objects.equals( row.get( i ).toString(), "" ) ) {
                                    row = row.subList( i, row.size() );
                                    break;
                                }
                            }
                            tableSurfaceData.put( sheetName, row );
                            break;
                        }
                    }
                    break;
                }
            }
        } catch ( IOException e ) {
            throw new GenericRuntimeException( e );
        }
    }


    public HashMap<String, List<Object>> getTableSurfaceData() {
        if ( tableSurfaceData.isEmpty() ) {
            readTableSurfaceData();
        }
        return tableSurfaceData;
    }


    public void deleteToken() {
        File file = new File( GoogleSheetSource.TOKENS_PATH, "/StoredCredential" );
        if ( file.exists() ) {
            file.delete();
        }
    }


    private String parseUrlToString( URL url ) {
        String content = url.getPath();
        String[] contentArr = content.split( "/" );
        return contentArr[3];
    }


    /**
     * Reads sheet in Google Sheet URL block by block
     *
     * @param tableName - name of the sheet to read in the URL.
     */
    private void readTable( String tableName ) {
        try {
            final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
            final String spreadsheetId = parseUrlToString( url );

            Sheets service = new Sheets.Builder( HTTP_TRANSPORT, GoogleSheetSource.JSON_FACTORY, GoogleSheetSource.getCredentials( oAuthIdKey, HTTP_TRANSPORT, googleSheetSource ) )
                    .setApplicationName( GoogleSheetSource.APPLICATION_NAME )
                    .build();

            // First query! let's start searching till we find first row.
            if ( !tableStart.containsKey( tableName ) ) {
                Spreadsheet s = service.spreadsheets().get( spreadsheetId ).execute(); // get all the sheets
                Sheet chosen_sheet = new Sheet();
                for ( Sheet sheet : s.getSheets() ) {
                    if ( tableName.equalsIgnoreCase( sheet.getProperties().getTitle() ) ) {
                        chosen_sheet = sheet;
                        break;
                    }
                }
                if ( chosen_sheet.isEmpty() ) {
                    chosen_sheet = s.getSheets().get( 0 );
                }

                int queryStartRow = 1;
                while ( true ) {
                    // Nothing in sheet
                    if ( queryStartRow > chosen_sheet.getProperties().getGridProperties().getRowCount() ) {
                        tableStart.put( tableName, -1 );
                        return;
                    }

                    int queryEndRow = queryStartRow + querySize - 1;

                    String sheetRange = tableName + "!" + queryStartRow + ":" + queryEndRow;
                    ValueRange response = service.spreadsheets().values()
                            .get( spreadsheetId, sheetRange )
                            .execute();
                    List<List<Object>> values = response.getValues();

                    if ( values == null ) {
                        queryStartRow += querySize;
                        continue;
                    }

                    int firstRowIndex = -1;
                    int indexCounter = 0;
                    for ( List<Object> row : values ) { // found at least 1 row, set the left shift
                        if ( row.size() != 0 ) {
                            for ( int i = 0; i < row.size(); i++ ) {
                                if ( !Objects.equals( row.get( i ).toString(), "" ) ) {
                                    tableLeftOffset.put( tableName, i );
                                    break;
                                }
                            }
                            firstRowIndex = indexCounter + 1;
                            break;
                        }
                        indexCounter++;
                    }

                    // TODO: optimize this
                    // Add the remaining data to tableData
                    for ( int i = firstRowIndex; i < values.size(); i++ ) {
                        List<Object> fullRow = values.get( i );
                        List<Object> trueRow = fullRow.subList( tableLeftOffset.get( tableName ), fullRow.size() );
                        values.set( i, trueRow );
                    }
                    tableData.put( tableName, values.subList( firstRowIndex, values.size() ) );

                    if ( values.size() < querySize ) { // we already queried everything
                        tableStart.put( tableName, -1 );
                    } else { // we'll start at that value later?
                        tableStart.put( tableName, firstRowIndex + 1 );
                    }

                    break;
                }
            } else if ( tableStart.get( tableName ) == -1 ) { // end of document already
                return;
            } else { // begin getting from
                Spreadsheet s = service.spreadsheets().get( spreadsheetId ).execute(); // get all the sheets
                Sheet chosen_sheet = new Sheet();
                for ( Sheet sheet : s.getSheets() ) {
                    if ( Objects.equals( sheet.getProperties().getTitle(), tableName ) ) {
                        chosen_sheet = sheet;
                        break;
                    }
                }

                int queryStartRow = tableStart.get( tableName );

                int queryEndRow = queryStartRow + querySize - 1;

                if ( queryStartRow > chosen_sheet.getProperties().getGridProperties().getRowCount() ) {
                    tableStart.put( tableName, -1 );
                    return;
                }

                String sheetRange = tableName + "!" + queryStartRow + ":" + queryEndRow;
                ValueRange response = service.spreadsheets().values()
                        .get( spreadsheetId, sheetRange )
                        .execute();
                List<List<Object>> values = response.getValues();

                if ( values == null ) { // we've reached an empty part of the document, so all rows have been queried
                    tableStart.put( tableName, -1 );
                    return;
                }

                List<List<Object>> currData = tableData.getOrDefault( tableName, new ArrayList<>() );
                for ( List<Object> row : values ) {
                    List<Object> trueRow = row.subList( tableLeftOffset.get( tableName ), row.size() );
                    currData.add( trueRow );
                }
                tableData.put( tableName, currData );

                // Final check: if the current data that we have is smaller than our query, we reached
                // the end, so we set our start to -1.
                if ( values.size() < querySize ) {
                    tableStart.put( tableName, -1 );
                } else {
                    tableStart.put( tableName, queryEndRow + 1 );
                }
            }
        } catch ( IOException | GeneralSecurityException e ) {
            throw new GenericRuntimeException( e );
        }
    }


    public String[] readNext( String tableName ) {
        if ( !enumPointer.containsKey( tableName ) ) {
            enumPointer.put( tableName, 0 );
        }

        if ( !tableData.containsKey( tableName ) || tableData.get( tableName ).size() <= enumPointer.get( tableName ) ) {
            readTable( tableName );
        }

        // Still true, so we've reached the end of the google sheet
        if ( tableData.get( tableName ).size() <= enumPointer.get( tableName ) ) {
            return null;
        }

        List<Object> results = tableData.get( tableName ).get( enumPointer.get( tableName ) );
        List<String> resultsStr = new ArrayList<>();
        enumPointer.put( tableName, enumPointer.get( tableName ) + 1 );

        for ( Object val : results ) {
            resultsStr.add( val.toString() );
        }

        return resultsStr.toArray( new String[0] );
    }


    public HashMap<String, List<List<Object>>> getTableData() {
        return tableData;
    }

}
