/*
 * Copyright 2019-2023 The Polypheny Project
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

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.GridProperties;
import com.google.api.services.sheets.v4.model.Sheet;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.ValueRange;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;


/**
 * Class that scans the Google Sheet directly using the GoogleSheetApi.
 */
public class GoogleSheetReader {

    private final String APPLICATION_NAME = "POLYPHENY GOOGLE SHEET READER";
    private final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
    private final String TOKENS_DIRECTORY_PATH = "google-sheet-adapter/src/main/resources/tokens";
    private final List<String> SCOPES = Collections.singletonList( SheetsScopes.SPREADSHEETS_READONLY );
    private final String CREDENTIALS_FILE_PATH = "/credentials.json";


    private final URL url;
    private final int querySize;
    private HashMap<String, List<Object>> tableSurfaceData = new HashMap<>();
    private HashMap<String, List<List<Object>>> tableData = new HashMap<>();
    private HashMap<String, Integer> tableStart = new HashMap<>();
    private HashMap<String, Integer> tableLeftOffset = new HashMap<>();
    private HashMap<String, Integer> enumPointer = new HashMap<>();


    /**
     * @param url - url of the Google Sheet to source.
     * @param querySize - size of the query (in case of large files)
     */
    public GoogleSheetReader( URL url, int querySize ) {
        this.url = url;
        this.querySize = querySize;
    }


    /**
     * Finds first row for field names
     */
    private void readTableSurfaceData() {
        if ( !tableSurfaceData.isEmpty() ) {
            return;
        }
        try {
            final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
            final String spreadsheetId = parseUrlToString( url );

            Sheets service = new Sheets.Builder( HTTP_TRANSPORT, JSON_FACTORY, getCredentials( HTTP_TRANSPORT ) )
                    .setApplicationName( APPLICATION_NAME )
                    .build();

            // get the properties of all the sheets
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
        } catch ( IOException | GeneralSecurityException e ) {
            throw new RuntimeException( e );
        }
    }


    public HashMap<String, List<Object>> getTableSurfaceData() {
        if ( tableSurfaceData.isEmpty() ) {
            readTableSurfaceData();
        }
        return tableSurfaceData;
    }


    private Credential getCredentials( final NetHttpTransport HTTP_TRANSPORT ) throws IOException {
        // Load client secrets.
        InputStream in = GoogleSheetReader.class.getResourceAsStream( CREDENTIALS_FILE_PATH );
        if ( in == null ) {
            throw new FileNotFoundException( "Resource not found: " + CREDENTIALS_FILE_PATH );
        }
        GoogleClientSecrets clientSecrets = GoogleClientSecrets.load( JSON_FACTORY, new InputStreamReader( in ) );

        // Build flow and trigger user authorization request.
        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
                HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES )
                .setDataStoreFactory( new FileDataStoreFactory( new java.io.File( TOKENS_DIRECTORY_PATH ) ) )
                .setAccessType( "offline" )
                .build();
        LocalServerReceiver receiver = new LocalServerReceiver.Builder().setPort( 8888 ).build();
        return new AuthorizationCodeInstalledApp( flow, receiver ).authorize( "user" );
    }


    public void deleteToken() {
        File file = new File( TOKENS_DIRECTORY_PATH + "/StoredCredential" );
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

            Sheets service = new Sheets.Builder( HTTP_TRANSPORT, JSON_FACTORY, getCredentials( HTTP_TRANSPORT ) )
                    .setApplicationName( APPLICATION_NAME )
                    .build();

            // first query! let's start searching till we find first row.
            if ( !tableStart.containsKey( tableName ) ) {
                Spreadsheet s = service.spreadsheets().get( spreadsheetId ).execute(); // get all the sheets
                Sheet chosen_sheet = new Sheet();
                for ( Sheet sheet : s.getSheets() ) {
                    if ( Objects.equals( sheet.getProperties().getTitle(), tableName ) ) {
                        chosen_sheet = sheet;
                        break;
                    }
                }

                int queryStartRow = 1;
                while ( true ) {
                    // nothing in sheet
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
                    // add the remaining data to tableData
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

                // final check: if the current data that we have is smaller than our query, we reached
                // the end, so we set our start to -1.
                if ( values.size() < querySize ) {
                    tableStart.put( tableName, -1 );
                } else {
                    tableStart.put( tableName, queryEndRow + 1 );
                }
            }
        } catch ( IOException | GeneralSecurityException e ) {
            throw new RuntimeException( e );
        }
    }


    public String[] readNext( String tableName ) {
        if ( !enumPointer.containsKey( tableName ) ) {
            enumPointer.put( tableName, 0 );
        }

        if ( !tableData.containsKey( tableName ) || tableData.get( tableName ).size() <= enumPointer.get( tableName ) ) {
            readTable( tableName );
        }

        // still true, so we've reached the end of the google sheet
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