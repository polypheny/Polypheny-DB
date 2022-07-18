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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;


/**
 * A LIST of QUESTIONS:
 * - Should this be Embedded or Remote deploy?
 * - What is length - scale - dimension - cardinality in the ExportedColumn?  - Physical name vs name in Exported Column
 * - Should I implement truncate? prepare? If so, what is Context? CatalogTable? PolyXID?
 */


/**
 * Functions that need to be implemented (based on other data sources)
 * - Main: sets the URL, and other settings if needed, create information page, enable information page.
 *
 * - set URL: (ETH)
 *
 * - create information page:
 *
 *
 * - getExportedColumns:
 * this is where you return a map of {table_name: existing columns}.
 * existing columns: each existing column is an ExportedColumn data type, with: column name, PolyType, collectionsType (can be null), length - scale - dimension - cardinality can all be null, schema_name, table_name,
 *  physical_name (name again), position (index), when to be primary key (== 0 is good enough).
 *
 * - EnableInformationPage: call from Adapter package, do nothing.
 *
 * - shutdown(): call from adapter package to shutdown.
 *
 * - reloadSettings: just update current settings (sheetsURL, maybe extra modes in the future).
 *
 * - createNewSchema: refer to GoogleSheetSchema file
 *
 * - createTableSchema: calls createTable in GoogleSheetSchema
 *
 * - getCurrentSchema: return currentSchema
 *
 * - truncate: (removes all rows from table) throw exception. As
 *
 */

@Slf4j
@Adapter.AdapterProperties(
        name = "Google Sheets",
        description = "An adapter for querying online Google Sheets, using the Google Sheets Java API. Currently, this adapter only supports read operations.",
        usedModes = DeployMode.EMBEDDED)
@Adapter.AdapterSettingString(name = "SheetsURL", description = "The URL of the Google Sheets to query", defaultValue = "https://docs.google.com/spreadsheets/d/1-int7xwx0UyyigB4FLGMOxaCiuHXSNhi09fYSuAIX2Q/edit#gid=0", position = 1)
@Adapter.AdapterSettingInteger(name = "maxStringLength", defaultValue = 255, position = 2,
        description = "Which length (number of characters including whitespace) should be used for the varchar columns. Make sure this is equal or larger than the longest string in any of the columns.")
public class GoogleSheetSource extends DataSource {
    private URL sheetsUrl;
    private GoogleSheetSchema currentSchema;
    private final int maxStringLength;
    private Map<String, List<ExportedColumn>> exportedColumnCache;

    public GoogleSheetSource(final int storeId, final String uniqueName, final Map<String, String> settings){
        super(storeId, uniqueName, settings, true);

        // Validate maxStringLength setting
        maxStringLength = Integer.parseInt( settings.get( "maxStringLength" ) );
        if ( maxStringLength < 1 ) {
            throw new RuntimeException( "Invalid value for maxStringLength: " + maxStringLength );
        }
        setSheetsUrl(settings);

        createInformationPage();
        enableInformationPage();
    }

    private void setSheetsUrl (Map<String, String> settings) {
        try {
            sheetsUrl = new URL(settings.get("SheetsURL"));
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * manipulates getExportedColumns to add information to 2 (black box) objects: InformationGroup, InformationTable.
     *  *   informationGroup is about the tables
     *  *   InformationTable is about the columns.
     *  *   Currently using CSV's version (more information), could use ETH for simplicity
     */
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

    /**
     * Do once you have reader
     * @return a map of {table_name: existing columns}.
     *  * existing columns: each existing column is an ExportedColumn data type, with: column name, PolyType, collectionsType (can be null), length - scale - dimension - cardinality can all be null, schema_name, table_name,
     *  *  physical_name (name again), position (index), when to be primary key (== 0 is good enough).
     */
    @Override
    public Map<String, List<ExportedColumn>> getExportedColumns(){
        if (exportedColumnCache != null) {
            return exportedColumnCache;
        }

        Map<String, List<ExportedColumn>> exportedColumnCache = new HashMap<>();
        GoogleSheetReader reader = new GoogleSheetReader(sheetsUrl);
        HashMap<String, List<List<Object>>> tablesData = reader.getTableData();

        for (Map.Entry<String, List<List<Object>>> entry: tablesData.entrySet()) {
            String tableName = entry.getKey();
            List<Object> tableColumns = entry.getValue().get(0);
            List<ExportedColumn> exportedCols = new ArrayList<>();
            int position = 0;
            for (Object col: tableColumns) {
                exportedCols.add(new ExportedColumn(
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
                ));
                position++;
            }

            exportedColumnCache.put(tableName, exportedCols);
        }

        return exportedColumnCache;
    }

    @Override
    public void shutdown() {
        removeInformationPage();
    }

    @Override
    protected void reloadSettings(List<String> updatedSettings) {
        if (updatedSettings.contains("SheetsURL")) {
            setSheetsUrl(settings);
        }
    }

    @Override
    public void createNewSchema(SchemaPlus rootSchema, String name) {
        currentSchema = new GoogleSheetSchema(this.sheetsUrl);
    }

    @Override
    public Table createTableSchema(CatalogTable combinedTable, List<CatalogColumnPlacement> columnPlacementsOnStore, CatalogPartitionPlacement partitionPlacement) {
        return currentSchema.createGoogleSheetTable( combinedTable, columnPlacementsOnStore, this );
    }

    @Override
    public Schema getCurrentSchema() {
        return currentSchema;
    }

    @Override
    public void truncate(Context context, CatalogTable table ) {
        throw new RuntimeException( "CSV adapter does not support truncate" );
    }

    @Override
    public boolean prepare( PolyXid xid ) {
        log.debug( "CSV Store does not support prepare()." );
        return true;
    }

    @Override
    public void commit( PolyXid xid ) {
        log.debug( "CSV Store does not support commit()." );
    }


    @Override
    public void rollback( PolyXid xid ) {
        log.debug( "CSV Store does not support rollback()." );
    }
}
