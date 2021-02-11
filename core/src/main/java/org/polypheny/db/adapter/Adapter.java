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

package org.polypheny.db.adapter;


import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Table;
import org.polypheny.db.transaction.PolyXid;

public abstract class Adapter {

    @Getter
    private final int adapterId;
    @Getter
    private final String uniqueName;

    protected final Map<String, String> settings;


    public Adapter( int adapterId, String uniqueName, Map<String, String> settings ) {
        this.adapterId = adapterId;
        this.uniqueName = uniqueName;
        // Make sure the settings are actually valid
        this.validateSettings( settings, true );
        this.settings = settings;
    }


    public abstract String getAdapterName();

    public abstract void createNewSchema( SchemaPlus rootSchema, String name );

    public abstract Table createTableSchema( CatalogTable combinedTable, List<CatalogColumnPlacement> columnPlacementsOnStore );

    public abstract Schema getCurrentSchema();

    public abstract void truncate( Context context, CatalogTable table );

    public abstract boolean prepare( PolyXid xid );

    public abstract void commit( PolyXid xid );

    public abstract void rollback( PolyXid xid );

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


    public void updateSettings( Map<String, String> newSettings ) {
        this.validateSettings( newSettings, false );
        List<String> updatedSettings = this.applySettings( newSettings );
        this.reloadSettings( updatedSettings );
        Catalog.getInstance().updateAdapterSettings( getAdapterId(), newSettings );
    }


    public Map<String, String> getCurrentSettings() {
        return settings;
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
            } else if ( s.required && initialSetup ) {
                throw new RuntimeException( "Setting \"" + s.name + "\" must be present." );
            }
        }
    }


    @Accessors(chain = true)
    public static abstract class AdapterSetting {

        public final String name;
        public final boolean canBeNull;
        public final boolean required;
        public final boolean modifiable;
        @Setter
        public String description;

        public AdapterSetting( final String name, final boolean canBeNull, final boolean required, final boolean modifiable ) {
            this.name = name;
            this.canBeNull = canBeNull;
            this.required = required;
            this.modifiable = modifiable;
        }

        /**
         * In most subclasses, this method returns the defaultValue, because the UI overrides the defaultValue when a new value is set.
         */
        public abstract String getValue();

    }


    public static class AdapterSettingInteger extends AdapterSetting {

        private final String type = "Integer";
        public final Integer defaultValue;


        public AdapterSettingInteger( String name, boolean canBeNull, boolean required, boolean modifiable, Integer defaultValue ) {
            super( name, canBeNull, required, modifiable );
            this.defaultValue = defaultValue;
        }

        public String getValue() {
            return defaultValue.toString();
        }

    }


    public static class AdapterSettingString extends AdapterSetting {

        private final String type = "String";
        public final String defaultValue;


        public AdapterSettingString( String name, boolean canBeNull, boolean required, boolean modifiable, String defaultValue ) {
            super( name, canBeNull, required, modifiable );
            this.defaultValue = defaultValue;
        }

        public String getValue() {
            return defaultValue;
        }

    }


    public static class AdapterSettingBoolean extends AdapterSetting {

        private final String type = "Boolean";
        public final boolean defaultValue;


        public AdapterSettingBoolean( String name, boolean canBeNull, boolean required, boolean modifiable, boolean defaultValue ) {
            super( name, canBeNull, required, modifiable );
            this.defaultValue = defaultValue;
        }

        public String getValue() {
            return Boolean.toString( defaultValue );
        }

    }


    @Accessors(chain = true)
    public static class AdapterSettingList extends AdapterSetting {

        private final String type = "List";
        public final List<String> options;
        @Setter
        public String defaultValue;


        public AdapterSettingList( String name, boolean canBeNull, boolean required, boolean modifiable, List<String> options ) {
            super( name, canBeNull, required, modifiable );
            this.options = options;
            if ( options.size() > 0 ) {
                this.defaultValue = options.get( 0 );
            }
        }

        public String getValue() {
            return defaultValue;
        }

    }


    @Accessors(chain = true)
    public static class AdapterSettingDirectory extends AdapterSetting {

        private final String type = "Directory";
        @Setter
        public String directory;
        //This field is necessary for the the UI and needs to be initialized to be serialized to JSON.
        @Setter
        public String[] fileNames = new String[]{ "" };
        public transient final Map<String, InputStream> inputStreams;

        public AdapterSettingDirectory( String name, boolean canBeNull, boolean required, boolean modifiable ) {
            super( name, canBeNull, required, modifiable );
            //so it will be serialized
            this.directory = "";
            this.inputStreams = new HashMap<>();
        }

        public String getValue() {
            return directory;
        }

    }


    //see https://stackoverflow.com/questions/19588020/gson-serialize-a-list-of-polymorphic-objects/22081826#22081826
    public static class AdapterSettingDeserializer implements JsonDeserializer<AdapterSetting> {

        public AdapterSetting deserialize( JsonElement json, Type typeOfT, JsonDeserializationContext context ) throws JsonParseException {
            JsonObject jsonObject = json.getAsJsonObject();
            String type = jsonObject.get( "type" ).getAsString();
            String name = jsonObject.get( "name" ).getAsString();
            boolean canBeNull = jsonObject.get( "canBeNull" ).getAsBoolean();
            boolean required = jsonObject.get( "required" ).getAsBoolean();
            boolean modifiable = jsonObject.get( "modifiable" ).getAsBoolean();
            String description = null;
            if ( jsonObject.get( "description" ) != null ) {
                description = jsonObject.get( "description" ).getAsString();
            }

            AdapterSetting out;
            switch ( type ) {
                case "Integer":
                    Integer integer = jsonObject.get( "defaultValue" ).getAsInt();
                    out = new AdapterSettingInteger( name, canBeNull, required, modifiable, integer );
                    break;
                case "String":
                    String string = jsonObject.get( "defaultValue" ).getAsString();
                    out = new AdapterSettingString( name, canBeNull, required, modifiable, string );
                    break;
                case "Boolean":
                    boolean bool = jsonObject.get( "defaultValue" ).getAsBoolean();
                    out = new AdapterSettingBoolean( name, canBeNull, required, modifiable, bool );
                    break;
                case "List":
                    List<String> options = context.deserialize( jsonObject.get( "options" ), List.class );
                    String defaultValue = context.deserialize( jsonObject.get( "defaultValue" ), String.class );
                    out = new AdapterSettingList( name, canBeNull, required, modifiable, options ).setDefaultValue( defaultValue );
                    break;
                case "Directory":
                    String directory = context.deserialize( jsonObject.get( "directory" ), String.class );
                    String[] fileNames = context.deserialize( jsonObject.get( "fileNames" ), String[].class );
                    out = new AdapterSettingDirectory( name, canBeNull, required, modifiable ).setDirectory( directory ).setFileNames( fileNames );
                    break;
                default:
                    throw new RuntimeException( "Could not deserialize AdapterSetting of type " + type );
            }
            out.setDescription( description );
            return out;
        }
    }

}
