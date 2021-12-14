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
import org.polypheny.db.adapter.Adapter.AdapterProperties;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Table;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;


@Slf4j
@AdapterProperties(
        name = "Array",
        description = "An simple in-memory adapter storing the data as lists of arrays.",
        usedModes = { DeployMode.EMBEDDED })
public class ArrayStore extends DataStore {

    private ArraySchema currentSchema;

    private final Map<String, Collection<Object[]>> data = new HashMap<>();


    public ArrayStore( final int storeId, final String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, false );
    }


    @Override
    public void createNewSchema( SchemaPlus rootSchema, String name ) {
        currentSchema = new ArraySchema();
    }


    @Override
    public Table createTableSchema( CatalogTable combinedTable, List<CatalogColumnPlacement> columnPlacementsOnStore, CatalogPartitionPlacement partitionPlacement ) {
        return currentSchema.createTable( combinedTable, columnPlacementsOnStore, partitionPlacement, this );
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
    public void createTable( Context context, CatalogTable combinedTable, List<Long> partitionIds ) {
        for ( long partitionId : partitionIds ) {
            List<String> qualifiedNames = new LinkedList<>();
            qualifiedNames.add( combinedTable.getSchemaName() );
            qualifiedNames.add( combinedTable.name );
            String physicalTableName = getPhysicalTableName( combinedTable.id, partitionId );
            if ( log.isDebugEnabled() ) {
                log.debug( "[{}] createTable: Qualified names: {}, physicalTableName: {}", getUniqueName(), qualifiedNames, physicalTableName );
            }
            if ( data.containsKey( physicalTableName ) ) {
                throw new RuntimeException( "Table already exists! This should not happen..." );
            }
            data.put( physicalTableName, new LinkedHashSet<>() );
        }
        // Add physical names to placements
        for ( CatalogColumnPlacement placement : catalog.getColumnPlacementsOnAdapterPerTable( getAdapterId(), combinedTable.id ) ) {
            catalog.updateColumnPlacementPhysicalNames(
                    getAdapterId(),
                    placement.columnId,
                    "public",
                    getPhysicalColumnName( placement.columnId ),
                    true );
        }
    }


    @Override
    public void dropTable( Context context, CatalogTable combinedTable, List<Long> partitionIds ) {
        for ( long partitionId : partitionIds ) {
            String physicalTableName = getPhysicalTableName( combinedTable.id, partitionId );
            if ( !data.containsKey( physicalTableName ) ) {
                throw new RuntimeException( "Table does not exist! This should not happen..." );
            }
            data.remove( physicalTableName );
        }
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
    public void addIndex( Context context, CatalogIndex catalogIndex, List<Long> partitionIds ) {
        throw new RuntimeException( "Array adapter does not support adding indexes" );
    }


    @Override
    public void dropIndex( Context context, CatalogIndex catalogIndex, List<Long> partitionIds ) {
        throw new RuntimeException( "Array adapter does not support dropping indexes" );
    }


    @Override
    public void updateColumnType( Context context, CatalogColumnPlacement columnPlacement, CatalogColumn catalogColumn, PolyType oldType ) {
        throw new RuntimeException( "Array adapter does not support updating column types!" );
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
        for ( CatalogPartitionPlacement partitionPlacement : catalog.getPartitionPlacementByTable( getAdapterId(), catalogTable.id ) ) {
            String physicalTableName = getPhysicalTableName( catalogTable.id, partitionPlacement.partitionId );
            if ( !data.containsKey( physicalTableName ) ) {
                throw new RuntimeException( "Table does not exist! This should not happen..." );
            }
            data.get( physicalTableName ).clear();
        }
    }


    @Override
    public List<AvailableIndexMethod> getAvailableIndexMethods() {
        return ImmutableList.of();
    }


    @Override
    public AvailableIndexMethod getDefaultIndexMethod() {
        throw new RuntimeException( "Array adapter does not support adding indexes" );
    }


    @Override
    public List<FunctionalIndexInfo> getFunctionalIndexes( CatalogTable catalogTable ) {
        return ImmutableList.of();
    }


    @Override
    public void shutdown() {
        // Nothing to do
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        // Nothing to do
    }


    protected String getPhysicalTableName( long tableId, long partitionId ) {
        String physicalTableName = "tab" + tableId;
        if ( partitionId >= 0 ) {
            physicalTableName += "_part" + partitionId;
        }
        return physicalTableName;
    }


    protected String getPhysicalColumnName( long columnId ) {
        return "col" + columnId;
    }

}
