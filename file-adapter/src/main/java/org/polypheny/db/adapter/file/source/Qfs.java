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

package org.polypheny.db.adapter.file.source;


import com.google.common.collect.ImmutableList;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Table;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;


/**
 * A data source that can Query a File System
 */
@Slf4j
public class Qfs extends DataSource {

    @SuppressWarnings("WeakerAccess")
    public static final String ADAPTER_NAME = "QFS";
    @SuppressWarnings("WeakerAccess")
    public static final String DESCRIPTION = "A data source that can query a file system";
    @SuppressWarnings("WeakerAccess")
    public static final List<AdapterSetting> AVAILABLE_SETTINGS = ImmutableList.of(
            new AdapterSettingString( "rootDir", false, true, true, "" )
    );

    @Getter
    private File rootDir;
    private QfsSchema currentSchema;

    public Qfs( int adapterId, String uniqueName, Map<String, String> settings ) {
        super( adapterId, uniqueName, settings, true );
        init( settings );
    }

    private void init( final Map<String, String> settings ) {
        rootDir = new File( settings.get( "rootDir" ) );
        if ( !rootDir.exists() ) {
            throw new RuntimeException( "The specified root dir does not exist!" );
        }
    }

    @Override
    public String getAdapterName() {
        return ADAPTER_NAME;
    }

    @Override
    public void createNewSchema( SchemaPlus rootSchema, String name ) {
        currentSchema = new QfsSchema( rootSchema, name, this );
    }

    @Override
    public Table createTableSchema( CatalogTable combinedTable, List<CatalogColumnPlacement> columnPlacementsOnStore ) {
        return currentSchema.createFileTable( combinedTable, columnPlacementsOnStore );
    }

    @Override
    public Schema getCurrentSchema() {
        return currentSchema;
    }

    @Override
    public void truncate( Context context, CatalogTable table ) {
        throw new RuntimeException( "QFS does not support truncate" );
    }

    @Override
    public boolean prepare( PolyXid xid ) {
        log.debug( "QFS does not support truncate" );
        return true;
    }

    @Override
    public void commit( PolyXid xid ) {
        log.debug( "QFS does not support commit" );
    }

    @Override
    public void rollback( PolyXid xid ) {
        log.debug( "QFS does not support rollback" );
    }

    @Override
    public List<AdapterSetting> getAvailableSettings() {
        return AVAILABLE_SETTINGS;
    }

    @Override
    public void shutdown() {

    }

    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        init( settings );
    }

    @Override
    public Map<String, List<ExportedColumn>> getExportedColumns() {
        //name, extension, path, mime, canExecute, canRead, canWrite, size, lastModified
        String physSchemaName = getUniqueName();
        String physTableName = getUniqueName();
        List<ExportedColumn> columns = new ArrayList<>();

        columns.add( new ExportedColumn(
                "path",
                PolyType.VARCHAR,
                null,
                1000,
                null,
                null,
                null,
                false,
                physSchemaName,
                physTableName,
                "path",
                1,
                true
        ) );

        columns.add( new ExportedColumn(
                "name",
                PolyType.VARCHAR,
                null,
                500,
                null,
                null,
                null,
                false,
                physSchemaName,
                physTableName,
                "name",
                2,
                false
        ) );

        columns.add( new ExportedColumn(
                "size",
                PolyType.BIGINT,
                null,
                null,
                null,
                null,
                null,
                false,
                physSchemaName,
                physTableName,
                "size",
                3,
                false
        ) );

        columns.add( new ExportedColumn(
                "file",
                PolyType.FILE,
                null,
                null,
                null,
                null,
                null,
                false,
                physSchemaName,
                physTableName,
                "file",
                4,
                false
        ) );

        Map<String, List<ExportedColumn>> out = new HashMap<>();
        out.put( getUniqueName(), columns );
        return out;
    }
}
