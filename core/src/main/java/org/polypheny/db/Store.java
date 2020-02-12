package org.polypheny.db;


import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedTable;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Table;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;


public abstract class Store {

    @Getter
    private final int storeId;
    @Getter
    private final String uniqueName;

    protected final Map<String, String> settings;


    public Store( final int storeId, final String uniqueName, final Map<String, String> settings ) {
        this.storeId = storeId;
        this.uniqueName = uniqueName;
        // Make sure the settings are actually valid
        this.validateSettings( settings, true );
        this.settings = settings;
    }


    public abstract void createNewSchema( Transaction transaction, SchemaPlus rootSchema, String name );

    public abstract Table createTableSchema( CatalogCombinedTable combinedTable );

    public abstract Schema getCurrentSchema();

    public abstract void createTable( Context context, CatalogCombinedTable combinedTable );

    public abstract void dropTable( Context context, CatalogCombinedTable combinedTable );

    public abstract void addColumn( Context context, CatalogCombinedTable catalogTable, CatalogColumn catalogColumn );

    public abstract void dropColumn( Context context, CatalogCombinedTable catalogTable, CatalogColumn catalogColumn );

    public abstract boolean prepare( PolyXid xid );

    public abstract void commit( PolyXid xid );

    public abstract void rollback( PolyXid xid );

    public abstract void truncate( Context context, CatalogCombinedTable table );

    public abstract void updateColumnType( Context context, CatalogColumn catalogColumn );

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
                    if ( ! s.canBeNull && newValue == null ) {
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
