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
import com.google.api.services.sheets.v4.model.Sheet;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.ValueRange;

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


/**
 * SOURCE: sheets quick start by google.
 */

/**
 * How to optimize when we've already read into the table?
 */

public class GoogleSheetReader {
    private final String APPLICATION_NAME = "POLYPHENY GOOGLE SHEET READER";
    private final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
    private final String TOKENS_DIRECTORY_PATH = "google-sheet-adapter/src/main/resources/tokens";
    private final List<String> SCOPES = Collections.singletonList(SheetsScopes.SPREADSHEETS_READONLY);
    private final String CREDENTIALS_FILE_PATH = "/credentials.json";



    private final URL url;
    private HashMap<String, List<List<Object>>> tableData = new HashMap<>();
    private String targetTableName;
    private List<List<Object>> targetTableData;
    private int currBlock;  // ptr to the current row to read from in the first table.

    public GoogleSheetReader(URL url) {
        this.url = url;
        readAllTables();

    }

    public GoogleSheetReader(URL url, String tableName) {
        this.url = url;
        readAllTables();
        setTargetTable(tableName);
        currBlock = 0;
        System.out.println(targetTableName);
        System.out.println(targetTableData.size());
    }


    private Credential getCredentials(final NetHttpTransport HTTP_TRANSPORT) throws IOException {
        // Load client secrets.
        InputStream in = GoogleSheetReader.class.getResourceAsStream(CREDENTIALS_FILE_PATH);
        if (in == null) {
            throw new FileNotFoundException("Resource not found: " + CREDENTIALS_FILE_PATH);
        }
        GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));

        // Build flow and trigger user authorization request.
        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
                HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
                .setDataStoreFactory(new FileDataStoreFactory(new java.io.File(TOKENS_DIRECTORY_PATH)))
                .setAccessType("offline")
                .build();
        LocalServerReceiver receiver = new LocalServerReceiver.Builder().setPort(8888).build();
        return new AuthorizationCodeInstalledApp(flow, receiver).authorize("user");
    }

    private String parseUrlToString(URL url) {
        String content = url.getPath();
        String[] contentArr = content.split("/");
        return contentArr[3];
    }


    private void readAllTables() {
        if (!tableData.isEmpty()){
            return;
        }
        try {
            final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
            final String spreadsheetId = parseUrlToString(url);

            Sheets service = new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY, getCredentials(HTTP_TRANSPORT))
                    .setApplicationName(APPLICATION_NAME)
                    .build();

            // get the properties of all the sheets
            Spreadsheet s = service.spreadsheets().get(spreadsheetId).execute();

            for (Sheet sheet: s.getSheets()) {
                String sheetName = sheet.getProperties().getTitle();
                ValueRange response = service.spreadsheets().values()
                        .get(spreadsheetId, sheetName)
                        .execute();

                List<List<Object>> values = response.getValues();
                tableData.put(sheetName, values);
            }
        } catch (IOException | GeneralSecurityException e ) {
            throw new RuntimeException(e);
        }
    }

    private void setTargetTable(String tableName) {
        targetTableName = tableName;
        targetTableData = tableData.get(tableName);
    }


    public String[] readNext() {
        if (currBlock >= targetTableData.size()) {
            return null;
        }

        List<Object> results = targetTableData.get(currBlock);
        List<String> resultsStr = new ArrayList<>();
        currBlock += 1;

        for (Object val: results) {
            resultsStr.add(val.toString());
        }

        return resultsStr.toArray(new String[0]);
    }


    public HashMap<String, List<List<Object>>> getTableData() {
        return tableData;
    }
}