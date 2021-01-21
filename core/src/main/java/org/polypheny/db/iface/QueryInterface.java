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

package org.polypheny.db.iface;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.polypheny.db.transaction.TransactionManager;


public abstract class QueryInterface implements Runnable {

    protected final transient TransactionManager transactionManager;
    protected final transient Authenticator authenticator;

    @Getter
    private final int queryInterfaceId;
    @Getter
    private final String uniqueName;

    @Getter
    private final boolean supportsDml;
    @Getter
    private final boolean supportsDdl;

    protected final Map<String, String> settings;


    public QueryInterface(
            final TransactionManager transactionManager,
            final Authenticator authenticator,
            final int queryInterfaceId,
            final String uniqueName,
            final Map<String, String> settings,
            final boolean supportsDml,
            final boolean supportsDdl ) {
        this.transactionManager = transactionManager;
        this.authenticator = authenticator;
        this.queryInterfaceId = queryInterfaceId;
        this.uniqueName = uniqueName;
        this.settings = settings;
        this.supportsDml = supportsDml;
        this.supportsDdl = supportsDdl;
    }


    public abstract List<QueryInterfaceSetting> getAvailableSettings();


    public abstract void shutdown();


    public abstract String getInterfaceType();

    /**
     * Informs a query interface that its settings have changed.
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
        for ( QueryInterfaceSetting s : getAvailableSettings() ) {
            if ( newSettings.containsKey( s.name ) ) {
                if ( s.modifiable || initialSetup ) {
                    String newValue = newSettings.get( s.name );
                    if ( !s.canBeNull && newValue == null ) {
                        throw new RuntimeException( "Setting \"" + s.name + "\" cannot be null." );
                    }
                } else {
                    throw new RuntimeException( "Setting \"" + s.name + "\" cannot be modified." );
                }
            } else if ( s.required && s.modifiable ) {
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
    public static abstract class QueryInterfaceSetting {

        public final String name;
        public final boolean canBeNull;
        public final boolean required;
        public final boolean modifiable;

    }


    public static class QueryInterfaceSettingInteger extends QueryInterfaceSetting {

        public final Integer defaultValue;


        public QueryInterfaceSettingInteger( String name, boolean canBeNull, boolean required, boolean modifiable, Integer defaultValue ) {
            super( name, canBeNull, required, modifiable );
            this.defaultValue = defaultValue;
        }

    }


    public static class QueryInterfaceSettingString extends QueryInterfaceSetting {

        public final String defaultValue;


        public QueryInterfaceSettingString( String name, boolean canBeNull, boolean required, boolean modifiable, String defaultValue ) {
            super( name, canBeNull, required, modifiable );
            this.defaultValue = defaultValue;
        }

    }


    public static class QueryInterfaceSettingBoolean extends QueryInterfaceSetting {

        public final boolean defaultValue;


        public QueryInterfaceSettingBoolean( String name, boolean canBeNull, boolean required, boolean modifiable, boolean defaultValue ) {
            super( name, canBeNull, required, modifiable );
            this.defaultValue = defaultValue;
        }

    }


    public static class QueryInterfaceSettingList extends QueryInterfaceSetting {

        public final List<String> options;


        public QueryInterfaceSettingList( String name, boolean canBeNull, boolean required, boolean modifiable, List<String> options ) {
            super( name, canBeNull, required, modifiable );
            this.options = options;
        }

    }


}
