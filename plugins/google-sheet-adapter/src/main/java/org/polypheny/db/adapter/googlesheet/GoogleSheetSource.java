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

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets.Details;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.RelationalScanDelegate;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.annotations.AdapterSettingBoolean;
import org.polypheny.db.adapter.annotations.AdapterSettingInteger;
import org.polypheny.db.adapter.annotations.AdapterSettingString;
import org.polypheny.db.adapter.googlesheet.util.PolyphenyTokenStoreFactory;
import org.polypheny.db.catalog.catalogs.RelAdapterCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.PolyphenyHomeDirManager;


@Slf4j
@AdapterProperties(
        name = "GoogleSheets",
        description = "An adapter for querying online Google Sheets, using the Google Sheets Java API. Currently, this adapter only supports read operations.",
        usedModes = DeployMode.REMOTE,
        defaultMode = DeployMode.REMOTE)
@AdapterSettingString(name = "sheetsURL", description = "The URL of the Google Sheet to query.", defaultValue = "", position = 1)
@AdapterSettingInteger(name = "maxStringLength", defaultValue = 255, position = 2,
        description = "Which length (number of characters including whitespace) should be used for the varchar columns. Make sure this is equal or larger than the longest string in any of the columns.")
@AdapterSettingInteger(name = "querySize", defaultValue = 1000, position = 3,
        description = "How many rows should be queried per network call. Can be larger than number of rows.")
@AdapterSettingBoolean(name = "resetRefreshToken", defaultValue = false, position = 4, description = "If you want to change the current email used to access the Google Sheet, input \"YES\".")
@AdapterSettingString(name = "oAuth-Client-ID", description = "Authentication credentials used for GoogleSheets API. Not the account credentials.", defaultValue = "", position = 5)
@AdapterSettingString(name = "oAuth-Client-Key", description = "Authentication credentials used for GoogleSheets API. Not the account credentials.", defaultValue = "")
@AdapterSettingString(name = "sheetName", description = "Name of sheet to use.", defaultValue = "")
public class GoogleSheetSource extends DataSource<RelAdapterCatalog> {

    @Delegate(excludes = Excludes.class)
    private final RelationalScanDelegate delegate;

    static final String APPLICATION_NAME = "POLYPHENY GOOGLE SHEET";
    static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
    static final List<String> SCOPES = Collections.singletonList( SheetsScopes.SPREADSHEETS_READONLY );
    public final String clientId;
    public final String clientKey;

    public static final File TOKENS_PATH = PolyphenyHomeDirManager.getInstance().registerNewFolder( "tokens" );
    public final String sheet;
    private URL sheetsUrl;
    private final int querySize;

    @Getter
    private GoogleSheetNamespace currentNamespace;
    private final int maxStringLength;

    @Getter
    @Setter
    Credential credentials;


    public GoogleSheetSource( final long storeId, final String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, true, new RelAdapterCatalog( storeId ) );

        this.clientId = getSettingOrFail( "oAuth-Client-ID", settings );
        this.clientKey = getSettingOrFail( "oAuth-Client-Key", settings );
        this.sheet = getSettingOrFail( "sheetName", settings );
        maxStringLength = Integer.parseInt( settings.get( "maxStringLength" ) );
        if ( maxStringLength < 1 ) {
            throw new GenericRuntimeException( "Invalid value for maxStringLength: " + maxStringLength );
        }
        querySize = Integer.parseInt( settings.get( "querySize" ) );
        if ( querySize < 1 ) {
            throw new GenericRuntimeException( "Invalid value for querySize: " + querySize );
        }
        setSheetsUrl( settings );

        createInformationPage();
        enableInformationPage();
        if ( settings.get( "resetRefreshToken" ).equalsIgnoreCase( "yes" ) ) {
            GoogleSheetReader r = new GoogleSheetReader( sheetsUrl, querySize, this );
            r.deleteToken();
        }
        this.delegate = new RelationalScanDelegate( this, adapterCatalog );
    }


    static Credential getCredentials( Pair<String, String> oAuthIdKey, final NetHttpTransport HTTP_TRANSPORT, GoogleSheetSource googleSheetSource ) throws IOException {
        if ( googleSheetSource.getCredentials() != null ) {
            return googleSheetSource.getCredentials();
        }
        // Load client secrets.
        GoogleClientSecrets clientSecrets = new GoogleClientSecrets();

        Details details = new Details();
        details.set( "client_id", oAuthIdKey.getKey() );
        details.set( "client_secret", oAuthIdKey.getValue() );
        details.setRedirectUris( List.of(
                "http://localhost:" + RuntimeConfig.WEBUI_SERVER_PORT,
                "http://localhost:" + RuntimeConfig.WEBUI_SERVER_PORT + "/Callback",
                "http://localhost:8888/Callback",
                "http://localhost:8888" ) );
        details.set( "auth_provider_x509_cert_url", "https://www.googleapis.com/oauth2/v1/certs" );
        clientSecrets.setInstalled( details );

        // Build flow and trigger user authorization request.
        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
                HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES )
                .setDataStoreFactory( new PolyphenyTokenStoreFactory( TOKENS_PATH ) )// own store, due to plugin system
                .setAccessType( "offline" )
                .build();
        LocalServerReceiver receiver = new LocalServerReceiver.Builder().setPort( 8888 ).build();
        AuthorizationCodeInstalledApp auth = new AuthorizationCodeInstalledApp( flow, receiver );
        googleSheetSource.setCredentials( auth.authorize( "user" ) );
        return googleSheetSource.getCredentials();
    }


    static Sheets getSheets( Pair<String, String> oAuthIdKey, GoogleSheetSource googleSheetSource ) {
        final NetHttpTransport HTTP_TRANSPORT;
        try {
            HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
            return new Sheets.Builder( HTTP_TRANSPORT, JSON_FACTORY, getCredentials( oAuthIdKey, HTTP_TRANSPORT, googleSheetSource ) )
                    .setApplicationName( APPLICATION_NAME )
                    .build();
        } catch ( GeneralSecurityException | IOException e ) {
            throw new GenericRuntimeException( e );
        }
    }


    private String getSettingOrFail( String key, Map<String, String> settings ) {
        if ( !settings.containsKey( key ) ) {
            throw new GenericRuntimeException( "Settings do not contain required key: " + key );
        }
        return settings.get( key );
    }


    private void setSheetsUrl( Map<String, String> settings ) {
        try {
            sheetsUrl = new URL( settings.get( "sheetsURL" ) );
        } catch ( MalformedURLException e ) {
            throw new GenericRuntimeException( e );
        }
    }


    protected void createInformationPage() {
        for ( Map.Entry<String, List<ExportedColumn>> entry : getExportedColumns().entrySet() ) {
            InformationGroup group = new InformationGroup(
                    informationPage,
                    entry.getValue().get( 0 ).physicalSchemaName + "." + entry.getValue().get( 0 ).physicalTableName );

            InformationTable table = new InformationTable(
                    group,
                    Arrays.asList( "Position", "Column Name", "Type", "Nullable", "Filename", "Primary" ) );
            for ( ExportedColumn exportedColumn : entry.getValue() ) {
                table.addRow(
                        exportedColumn.physicalPosition,
                        exportedColumn.name,
                        exportedColumn.getDisplayType(),
                        exportedColumn.nullable ? "✔" : "",
                        exportedColumn.physicalSchemaName,
                        exportedColumn.primary ? "✔" : ""
                );
            }
            informationElements.add( table );
            informationGroups.add( group );
        }
    }


    @Override
    public Map<String, List<ExportedColumn>> getExportedColumns() {

        Map<String, List<ExportedColumn>> exportedColumnCache = new HashMap<>();
        GoogleSheetReader reader = new GoogleSheetReader( sheetsUrl, querySize, this );
        HashMap<String, List<Object>> tablesData = reader.getTableSurfaceData();

        for ( Map.Entry<String, List<Object>> entry : tablesData.entrySet() ) {
            String tableName = entry.getKey();
            List<Object> tableColumns = entry.getValue();
            List<ExportedColumn> exportedCols = new ArrayList<>();
            int position = 0;
            for ( Object col : tableColumns ) {
                String colStr = col.toString();
                int colon_index = colStr.indexOf( ':' );

                String colName = colStr;
                PolyType type = PolyType.VARCHAR;
                Integer length = maxStringLength;

                if ( colon_index != -1 ) {
                    colName = colStr.substring( 0, colon_index ).trim();
                    String colType = colStr.substring( colon_index + 1 ).toLowerCase( Locale.ROOT ).trim();
                    switch ( colType ) {
                        case "int":
                            type = PolyType.INTEGER;
                            length = null;
                            break;
                        case "string":
                            break;
                        case "boolean":
                            type = PolyType.BOOLEAN;
                            length = null;
                            break;
                        case "long":
                            type = PolyType.BIGINT;
                            length = null;
                            break;
                        case "float":
                            type = PolyType.REAL;
                            length = null;
                            break;
                        case "double":
                            type = PolyType.DOUBLE;
                            length = null;
                            break;
                        case "date":
                            type = PolyType.DATE;
                            length = null;
                            break;
                        case "time":
                            type = PolyType.TIME;
                            length = 0;
                            break;
                        case "timestamp":
                            type = PolyType.TIMESTAMP;
                            length = 0;
                            break;
                        default:
                            throw new GenericRuntimeException( "Unknown type: " + colType.toLowerCase() );
                    }
                }

                exportedCols.add( new ExportedColumn(
                        colName,
                        type,
                        null,
                        length,
                        null,
                        null,
                        null,
                        false,
                        "public",
                        tableName,
                        col.toString(),
                        position,
                        position == 0
                ) );
                position++;
            }

            exportedColumnCache.put( tableName, exportedCols );
        }

        return exportedColumnCache;
    }


    @Override
    public void shutdown() {
        removeInformationPage();
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        if ( updatedSettings.contains( "sheetsURL" ) ) {
            setSheetsUrl( settings );
        }
    }


    @Override
    public void updateNamespace( String name, long id ) {
        currentNamespace = new GoogleSheetNamespace( id, adapterId, this.sheetsUrl, this.querySize, this );
    }


    @Override
    public void truncate( Context context, long allocId ) {
        throw new GenericRuntimeException( "Google Sheet adapter does not support truncate" );
    }


    protected void updateNativePhysical( long allocId ) {
        PhysicalTable table = this.adapterCatalog.fromAllocation( allocId );
        adapterCatalog.replacePhysical( this.currentNamespace.createGoogleSheetTable( table, this ) );
    }


    @Override
    public void renameLogicalColumn( long id, String newColumnName ) {
        adapterCatalog.renameLogicalColumn( id, newColumnName );
        adapterCatalog.fields.values().stream().filter( c -> c.id == id ).forEach( c -> updateNativePhysical( c.allocId ) );
    }


    @Override
    public List<PhysicalEntity> createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation ) {
        PhysicalTable table = adapterCatalog.createTable(
                logical.table.getNamespaceName(),
                logical.table.name,
                logical.columns.stream().collect( Collectors.toMap( c -> c.id, c -> c.name ) ),
                logical.table,
                logical.columns.stream().collect( Collectors.toMap( t -> t.id, t -> t ) ),
                logical.pkIds, allocation );
        adapterCatalog.replacePhysical( currentNamespace.createGoogleSheetTable( table, this ) );
        return List.of( table );
    }


    @Override
    public boolean prepare( PolyXid xid ) {
        log.debug( "Google Sheet adapter does not support prepare()." );
        return true;
    }


    @Override
    public void commit( PolyXid xid ) {
        log.debug( "Google Sheet adapter does not support commit()." );
    }


    @Override
    public void rollback( PolyXid xid ) {
        log.debug( "Google Sheet adapter does not support rollback()." );
    }


    @SuppressWarnings("unused")
    private interface Excludes {

        void renameLogicalColumn( long id, String newColumnName );

        void refreshTable( long allocId );

        void createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation );

    }

}
