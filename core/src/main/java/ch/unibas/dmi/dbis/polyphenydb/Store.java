package ch.unibas.dmi.dbis.polyphenydb;


import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;


public abstract class Store {

    @Getter
    private final int storeId;
    @Getter
    private final String uniqueName;

    private final Map<String, String> config;


    public Store( final int storeId, final String uniqueName, final Map<String, String> config ) {
        this.storeId = storeId;
        this.uniqueName = uniqueName;
        this.config = config;
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

    public abstract void truncate( Context context, CatalogCombinedTable table );

    public abstract void updateColumnType( Context context, CatalogColumn catalogColumn );

    public abstract String getAdapterName();

    public abstract List<AdapterSetting> getAdapterSettings();


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
