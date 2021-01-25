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

package org.polypheny.db.adapter;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.jdbc.Context;


public abstract class DataStore extends Adapter {

    protected final Map<String, String> settings;

    @Getter
    private final boolean persistent;

    protected final Catalog catalog = Catalog.getInstance();


    public DataStore( final int adapterId, final String uniqueName, final Map<String, String> settings, final boolean persistent ) {
        super( adapterId, uniqueName );
        // Make sure the settings are actually valid
        this.validateSettings( settings, true );
        this.settings = settings;
        this.persistent = persistent;
    }


    public abstract void createTable( Context context, CatalogTable combinedTable );

    public abstract void dropTable( Context context, CatalogTable combinedTable );

    public abstract void addColumn( Context context, CatalogTable catalogTable, CatalogColumn catalogColumn );

    public abstract void dropColumn( Context context, CatalogColumnPlacement columnPlacement );

    public abstract void addIndex( Context context, CatalogIndex catalogIndex );

    public abstract void dropIndex( Context context, CatalogIndex catalogIndex );

    public abstract void truncate( Context context, CatalogTable table );

    public abstract void updateColumnType( Context context, CatalogColumnPlacement columnPlacement, CatalogColumn catalogColumn );

    public abstract List<AvailableIndexMethod> getAvailableIndexMethods();

    public abstract AvailableIndexMethod getDefaultIndexMethod();

    public abstract List<FunctionalIndexInfo> getFunctionalIndexes( CatalogTable catalogTable );

    public abstract void shutdown();

    /**
     * Informs a store that its settings have changed.
     *
     * @param updatedSettings List of setting names that have changed.
     */
    protected abstract void reloadSettings( List<String> updatedSettings );


    protected List<String> applySettings( Map<String, String> newSettings ) {
        List<String> updatedSettings = new ArrayList<>();
        for ( Entry<String, String> newSetting : newSettings.entrySet() ) {
            if ( !Objects.equals( this.settings.get( newSetting.getKey() ), newSetting.getValue() ) ) {
                this.settings.put( newSetting.getKey(), newSetting.getValue() );
                updatedSettings.add( newSetting.getKey() );
            }
        }

        return updatedSettings;
    }


    public void updateSettings( Map<String, String> newSettings ) {
        this.validateSettings( newSettings, false );
        List<String> updatedSettings = this.applySettings( newSettings );
        this.reloadSettings( updatedSettings );
    }


    public Map<String, String> getCurrentSettings() {
        return settings;
    }


    @AllArgsConstructor
    public static class AvailableIndexMethod {

        public final String name;
        public final String displayName;

    }


    @AllArgsConstructor
    public static class FunctionalIndexInfo {

        public final List<Long> columnIds;
        public final String methodDisplayName;


        public List<String> getColumnNames() {
            List<String> columnNames = new ArrayList<>( columnIds.size() );
            for ( long columnId : columnIds ) {
                columnNames.add( Catalog.getInstance().getColumn( columnId ).name );
            }
            return columnNames;
        }

    }

}
