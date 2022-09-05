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

package org.polypheny.db.adapter.googlesheet;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Table;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;


@Slf4j
@Adapter.AdapterProperties(
        name = "GoogleSheets",
        description = "An adapter for querying online Google Sheets, using the Google Sheets Java API. Currently, this adapter only supports read operations.",
        usedModes = DeployMode.REMOTE)
@Adapter.AdapterSettingString(name = "sheetsURL", description = "The URL of the Google Sheets to query", defaultValue = "https://docs.google.com/spreadsheets/d/1-int7xwx0UyyigB4FLGMOxaCiuHXSNhi09fYSuAIX2Q/edit#gid=0", position = 1)
@Adapter.AdapterSettingInteger(name = "maxStringLength", defaultValue = 255, position = 2,
        description = "Which length (number of characters including whitespace) should be used for the varchar columns. Make sure this is equal or larger than the longest string in any of the columns.")
@Adapter.AdapterSettingInteger(name = "querySize", defaultValue = 1000, position = 3,
        description = "How many rows should be queried per network call. Can be larger than number of rows.")
@Adapter.AdapterSettingString(name = "resetRefreshToken", defaultValue = "No", position = 4, description = "If you want to change the current email used to access the Google Sheet, input \"YES\".")
public class GoogleSheetSource extends DataSource {

    private URL sheetsUrl;
    private int querySize;
    private GoogleSheetSchema currentSchema;
    private final int maxStringLength;
    private Map<String, List<ExportedColumn>> exportedColumnCache;


    public GoogleSheetSource( final int storeId, final String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, true );

        maxStringLength = Integer.parseInt( settings.get( "maxStringLength" ) );
        if ( maxStringLength < 1 ) {
            throw new RuntimeException( "Invalid value for maxStringLength: " + maxStringLength );
        }
        querySize = Integer.parseInt( settings.get( "querySize" ) );
        if ( querySize < 1 ) {
            throw new RuntimeException( "Invalid value for querySize: " + querySize );
        }
        setSheetsUrl( settings );

        createInformationPage();
        enableInformationPage();
        if ( settings.get( "resetRefreshToken" ).equalsIgnoreCase( "yes" ) ) {
            GoogleSheetReader r = new GoogleSheetReader( sheetsUrl, querySize );
            r.deleteToken();
        }
    }


    private void setSheetsUrl( Map<String, String> settings ) {
        try {
            sheetsUrl = new URL( settings.get( "sheetsURL" ) );
        } catch ( MalformedURLException e ) {
            throw new RuntimeException( e );
        }
    }


    protected void createInformationPage() {
        for ( Map.Entry<String, List<ExportedColumn>> entry : getExportedColumns().entrySet() ) {
            InformationGroup group = new InformationGroup(
                    informationPage,
                    entry.getValue().get( 0 ).physicalSchemaName + "." + entry.getValue().get( 0 ).physicalTableName );

            InformationTable table = new InformationTable(
                    group,
                    // Arrays.asList( "Position", "Column Name", "Type", "Primary" ) );
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
        if ( exportedColumnCache != null ) {
            return exportedColumnCache;
        }

        Map<String, List<ExportedColumn>> exportedColumnCache = new HashMap<>();
        GoogleSheetReader reader = new GoogleSheetReader( sheetsUrl, querySize );
        HashMap<String, List<Object>> tablesData = reader.getTableSurfaceData();

        for ( Map.Entry<String, List<Object>> entry : tablesData.entrySet() ) {
            String tableName = entry.getKey();
            List<Object> tableColumns = entry.getValue();
            List<ExportedColumn> exportedCols = new ArrayList<>();
            int position = 0;
            for ( Object col : tableColumns ) {
                exportedCols.add( new ExportedColumn(
                        col.toString(),
                        PolyType.VARCHAR, // defaulting to VARCHAR currently
                        null,
                        null,
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
    public void createNewSchema( SchemaPlus rootSchema, String name ) {
        currentSchema = new GoogleSheetSchema( this.sheetsUrl, this.querySize );
    }


    @Override
    public Table createTableSchema( CatalogTable combinedTable, List<CatalogColumnPlacement> columnPlacementsOnStore, CatalogPartitionPlacement partitionPlacement ) {
        return currentSchema.createGoogleSheetTable( combinedTable, columnPlacementsOnStore, this );
    }


    @Override
    public Schema getCurrentSchema() {
        return currentSchema;
    }


    @Override
    public void truncate( Context context, CatalogTable table ) {
        throw new RuntimeException( "Google Sheet adapter does not support truncate" );
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

}
