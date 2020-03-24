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
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedTable;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Table;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.transaction.Transaction;


public abstract class Store {

    @Getter
    private final int storeId;
    @Getter
    private final String uniqueName;

    @Getter
    private final boolean dataReadOnly;
    @Getter
    private final boolean schemaReadOnly;

    protected final Map<String, String> settings;

    @Getter
    private final boolean persistent;


    public Store(
            final int storeId,
            final String uniqueName,
            final Map<String, String> settings,
            final boolean dataReadOnly,
            final boolean schemaReadOnly ) {
        this.storeId = storeId;
        this.uniqueName = uniqueName;
        // Make sure the settings are actually valid
        this.validateSettings( settings, true );
        this.settings = settings;
        this.dataReadOnly = dataReadOnly;
        this.schemaReadOnly = schemaReadOnly;

        if ( settings.containsKey( "persistent" ) ) {
            persistent = Boolean.parseBoolean( settings.get( "persistent" ) );
        } else {
            persistent = false;
        }
    }


    public abstract void createNewSchema( Transaction transaction, SchemaPlus rootSchema, String name );

    public abstract Table createTableSchema( CatalogTable combinedTable );

    public abstract Schema getCurrentSchema();

    public abstract void createTable( Context context, CatalogTable combinedTable );

    public abstract void dropTable( Context context, CatalogTable combinedTable );

    public abstract void addColumn( Context context, CatalogTable catalogTable, CatalogColumn catalogColumn );

    public abstract void dropColumn( Context context, CatalogColumnPlacement columnPlacement );

    public abstract boolean prepare( PolyXid xid );

    public abstract void commit( PolyXid xid );

    public abstract void rollback( PolyXid xid );

    public abstract void truncate( Context context, CatalogTable table );

    public abstract void updateColumnType( Context context, CatalogColumnPlacement columnPlacement, CatalogColumn catalogColumn );

    public abstract String getAdapterName();

    public abstract List<AdapterSetting> getAvailableSettings();

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


    protected void validateSettings( Map<String, String> newSettings, boolean initialSetup ) {
        for ( AdapterSetting s : getAvailableSettings() ) {
            if ( newSettings.containsKey( s.name ) ) {
                if ( s.modifiable || initialSetup ) {
                    String newValue = newSettings.get( s.name );
                    if ( !s.canBeNull && newValue == null ) {
                        throw new RuntimeException( "Setting \"" + s.name + "\" cannot be null." );
                    }
                } else {
                    throw new RuntimeException( "Setting \"" + s.name + "\" cannot be modified." );
                }
            } else if ( s.required ) {
                throw new RuntimeException( "Setting \"" + s.name + "\" must be present." );
            }
        }
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
    public static abstract class AdapterSetting {

        public final String name;
        public final boolean canBeNull;
        public final boolean required;
        public final boolean modifiable;
    }


    public static class AdapterSettingInteger extends AdapterSetting {

        public final Integer defaultValue;


        public AdapterSettingInteger( String name, boolean canBeNull, boolean required, boolean modifiable, Integer defaultValue ) {
            super( name, canBeNull, required, modifiable );
            this.defaultValue = defaultValue;
        }
    }


    public static class AdapterSettingString extends AdapterSetting {

        public final String defaultValue;


        public AdapterSettingString( String name, boolean canBeNull, boolean required, boolean modifiable, String defaultValue ) {
            super( name, canBeNull, required, modifiable );
            this.defaultValue = defaultValue;
        }
    }


    public static class AdapterSettingBoolean extends AdapterSetting {

        public final boolean defaultValue;


        public AdapterSettingBoolean( String name, boolean canBeNull, boolean required, boolean modifiable, boolean defaultValue ) {
            super( name, canBeNull, required, modifiable );
            this.defaultValue = defaultValue;
        }
    }


    public static class AdapterSettingList extends AdapterSetting {

        public final List<String> options;


        public AdapterSettingList( String name, boolean canBeNull, boolean required, boolean modifiable, List<String> options ) {
            super( name, canBeNull, required, modifiable );
            this.options = options;
        }
    }

}
