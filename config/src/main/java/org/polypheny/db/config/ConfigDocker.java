/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.config;


import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.polypheny.db.config.exception.ConfigRuntimeException;


@Accessors(chain = true)
public class ConfigDocker extends ConfigObject {

    public static final int COMMUNICATION_PORT = 7001;
    public static final int HANDSHAKE_PORT = 7002;
    public static final int PROXY_PORT = 7003;

    @Getter
    @Setter
    private String host;
    @Getter
    @Setter
    private String alias;
    @Getter
    @Setter
    private String registry;
    @Getter
    @Setter
    private int communicationPort;
    @Getter
    @Setter
    private int handshakePort;
    @Getter
    @Setter
    private int proxyPort;


    public ConfigDocker( String host, String alias ) {
        this( idBuilder.getAndIncrement(), host, alias, "", COMMUNICATION_PORT, HANDSHAKE_PORT, PROXY_PORT );
    }


    public ConfigDocker( String host, String alias, String registry, int communicationPort, int handshakePort, int proxyPort ) {
        this( idBuilder.getAndIncrement(), host, alias, registry, communicationPort, handshakePort, proxyPort );
    }


    public ConfigDocker( int id, String host, String alias, String registry, int communicationPort, int handshakePort, int proxyPort ) {
        super( "dockerConfig" + id );
        this.id = id;
        if ( idBuilder.get() <= id ) {
            idBuilder.set( id + 1 );
        }
        this.host = host;
        this.alias = alias;
        this.registry = registry;
        this.communicationPort = communicationPort;
        this.handshakePort = handshakePort;
        this.proxyPort = proxyPort;
        this.webUiFormType = WebUiFormType.DOCKER_INSTANCE;
    }


    public static ConfigDocker fromMap( Map<String, Object> value ) {
        Double newId = (Double) value.getOrDefault( "id", null );
        if ( newId == null ) {
            newId = (double) idBuilder.getAndIncrement();
        }

        return new ConfigDocker(
                newId.intValue(),
                (String) value.get( "host" ),
                (String) value.get( "alias" ),
                (String) value.getOrDefault( "registry", "" ),
                ((Double) value.getOrDefault( "communicationPort", (double) COMMUNICATION_PORT )).intValue(),
                ((Double) value.getOrDefault( "handshakePort", (double) HANDSHAKE_PORT )).intValue(),
                ((Double) value.getOrDefault( "proxyPort", (double) PROXY_PORT )).intValue()
        );
    }


    public Map<String, Object> toMap() {
        Map<String, Object> m = new HashMap<>();
        m.put( "id", (double) id );
        m.put( "host", host );
        m.put( "alias", alias );
        m.put( "registry", registry );
        m.put( "communicationPort", (double) communicationPort );
        m.put( "handshakePort", (double) handshakePort );
        m.put( "proxyPort", (double) proxyPort );

        return m;
    }


    public Map<String, String> getSettings() {
        Map<String, String> settings = new HashMap<>();

        settings.put( "id", String.valueOf( id ) );
        settings.put( "host", host );
        settings.put( "alias", alias );
        settings.put( "registry", registry );
        settings.put( "communicationPort", String.valueOf( communicationPort ) );
        settings.put( "handshakePort", String.valueOf( handshakePort ) );
        settings.put( "proxyPort", String.valueOf( proxyPort ) );

        return settings;
    }


    @Override
    public Object getPlainValueObject() {
        throw new ConfigRuntimeException( "Not supported for Docker Configs" );
    }


    @Override
    public Object getDefaultValue() {
        throw new ConfigRuntimeException( "Not supported for Docker Configs" );
    }


    /**
     * Checks if the currently set config value, is equal to the system configured default.
     * If you want to reset it to the configured defaultValue use {@link #resetToDefault()}
     * To change the systems default value you can use: {@link #changeDefaultValue(Object)}
     *
     * @return true if it is set to default, false if it deviates
     */
    @Override
    public boolean isDefault() {
        throw new ConfigRuntimeException( "Not supported for Docker Configs" );
    }


    /**
     * Restores the current value to the system configured default value.
     *
     * To obtain the system configured defaultValue use {@link #getDefaultValue()}.
     * If you want to check if the current value deviates from default use: {@link #isDefault()}.
     */
    @Override
    public void resetToDefault() {
        throw new ConfigRuntimeException( "Not supported for Docker Configs" );
    }


    @Override
    void setValueFromFile( com.typesafe.config.Config conf ) {
        fromMap( parseConfigToMap( conf ) );
    }


    /**
     * Build map of settings from config file
     *
     * @param conf config file
     * @return parsed map representation of config file
     */
    public static Map<String, Object> parseConfigToMap( com.typesafe.config.Config conf ) {
        Map<String, Object> confMap = new HashMap<>();

        confMap.put( "id", conf.getDouble( "id" ) );
        confMap.put( "host", conf.getString( "host" ) );
        confMap.put( "alias", conf.getString( "alias" ) );
        confMap.put( "registry", conf.getString( "registry" ) );
        if ( conf.hasPath( "communicationPort" ) ) {
            confMap.put( "communicationPort", conf.getDouble( "communicationPort" ) );
        }
        if ( conf.hasPath( "handshakePort" ) ) {
            confMap.put( "handshakePort", conf.getDouble( "handshakePort" ) );
        }
        if ( conf.hasPath( "proxyPort" ) ) {
            confMap.put( "proxyPort", conf.getDouble( "proxyPort" ) );
        }

        return confMap;
    }


    @Override
    public boolean parseStringAndSetValue( String value ) {
        return false;
    }


    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( o == null || getClass() != o.getClass() ) {
            return false;
        }
        ConfigDocker that = (ConfigDocker) o;
        return host.equals( that.host ) &&
                alias.equals( that.alias ) &&
                registry.equals( that.registry ) &&
                communicationPort == that.communicationPort &&
                handshakePort == that.handshakePort &&
                proxyPort == that.proxyPort;
    }

}
