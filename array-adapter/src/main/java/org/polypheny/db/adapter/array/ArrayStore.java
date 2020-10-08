/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.adapter.array;


import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.Store;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnPlacementException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Table;
import org.polypheny.db.transaction.PolyXid;


@Slf4j
public class ArrayStore extends Store {

    @SuppressWarnings("WeakerAccess")
    public static final String ADAPTER_NAME = "Array";
    @SuppressWarnings("WeakerAccess")
    public static final String DESCRIPTION = "An simple in-memory adapter storing the data as lists of arrays.";
    @SuppressWarnings("WeakerAccess")
    public static final List<AdapterSetting> AVAILABLE_SETTINGS = ImmutableList.of(
    );

    private ArraySchema currentSchema;

    private final Map<String, Collection<Object[]>> data = new HashMap<>();


    public ArrayStore( final int storeId, final String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, false, false, false );
    }


    @Override
    public void createNewSchema( SchemaPlus rootSchema, String name ) {
        currentSchema = new ArraySchema();
    }


    @Override
    public Table createTableSchema( CatalogTable catalogTable, List<CatalogColumnPlacement> columnPlacementsOnStore ) {
        return currentSchema.createTable( catalogTable, columnPlacementsOnStore, this );
    }


    @Override
    public Schema getCurrentSchema() {
        return currentSchema;
    }


    Collection<Object[]> getRow( String physicalTableName ) {
        if ( data.containsKey( physicalTableName ) ) {
            return data.get( physicalTableName );
        } else {
            throw new RuntimeException( "Unknown table: " + physicalTableName );
        }
    }


    @Override
    public void createTable( Context context, CatalogTable catalogTable ) {
        List<String> qualifiedNames = new LinkedList<>();
        qualifiedNames.add( catalogTable.getSchemaName() );
        qualifiedNames.add( catalogTable.name );
        String physicalTableName = getPhysicalTableName( catalogTable.id );
        if ( log.isDebugEnabled() ) {
            log.debug( "[{}] createTable: Qualified names: {}, physicalTableName: {}", getUniqueName(), qualifiedNames, physicalTableName );
        }
        if ( data.containsKey( physicalTableName ) ) {
            throw new RuntimeException( "Table already exists! This should not happen..." );
        }
        data.put( physicalTableName, new LinkedHashSet<>() );
        // Add physical names to placements
        for ( CatalogColumnPlacement placement : catalog.getColumnPlacementsOnStore( getStoreId(), catalogTable.id ) ) {
            try {
                catalog.updateColumnPlacementPhysicalNames(
                        getStoreId(),
                        placement.columnId,
                        "public",
                        physicalTableName,
                        getPhysicalColumnName( placement.columnId ) );
            } catch ( GenericCatalogException | UnknownColumnPlacementException e ) {
                throw new RuntimeException( e );
            }
        }
    }


    @Override
    public void dropTable( Context context, CatalogTable catalogTable ) {
        String physicalTableName = getPhysicalTableName( catalogTable.id );
        if ( !data.containsKey( physicalTableName ) ) {
            throw new RuntimeException( "Table does not exist! This should not happen..." );
        }
        data.remove( physicalTableName );
    }


    @Override
    public void addColumn( Context context, CatalogTable catalogTable, CatalogColumn catalogColumn ) {
        log.warn( "Array adapter does not support adding columns!" );
    }


    @Override
    public void dropColumn( Context context, CatalogColumnPlacement columnPlacement ) {
        log.warn( "Array adapter does not support dropping columns!" );
    }


    @Override
    public boolean prepare( PolyXid xid ) {
        log.debug( "Array Store does not support prepare()." );
        return true;
    }


    @Override
    public void commit( PolyXid xid ) {
        log.debug( "Array Store does not support commit()." );
    }


    @Override
    public void rollback( PolyXid xid ) {
        log.debug( "Array Store does not support rollback()." );
    }


    @Override
    public void truncate( Context context, CatalogTable catalogTable ) {
        String physicalTableName = getPhysicalTableName( catalogTable.id );
        if ( !data.containsKey( physicalTableName ) ) {
            throw new RuntimeException( "Table does not exist! This should not happen..." );
        }
        data.get( physicalTableName ).clear();
    }


    @Override
    public void updateColumnType( Context context, CatalogColumnPlacement placement, CatalogColumn catalogColumn ) {
        throw new RuntimeException( "Array adapter does not support updating column types!" );
    }


    @Override
    public String getAdapterName() {
        return ADAPTER_NAME;
    }


    @Override
    public List<AdapterSetting> getAvailableSettings() {
        return AVAILABLE_SETTINGS;
    }


    @Override
    public void shutdown() {
        // Nothing to do
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        // Nothing to do
    }


    protected String getPhysicalTableName( long tableId ) {
        return "tab" + tableId;
    }


    protected String getPhysicalColumnName( long columnId ) {
        return "col" + columnId;
    }

}
