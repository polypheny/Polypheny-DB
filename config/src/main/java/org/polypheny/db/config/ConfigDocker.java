/*
 * Copyright 2019-2022 The Polypheny Project
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


import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.polypheny.db.config.exception.ConfigRuntimeException;


@Accessors(chain = true)
public class ConfigDocker extends ConfigObject {

    public static final String DEFAULT_PROTOCOL = "tcp";
    public static final int DEFAULT_PORT = 2376;

    // SSH was introduced as a possible transport protocol for connecting to remote Docker hosts recently, it is not yet
    // supported in java-docker but can be enabled as soon as this happens
    public final List<String> protocols = Collections.singletonList( "tcp" );

    @Getter
    @Setter
    private String alias;
    @Getter
    @Setter
    private String host;
    @Getter
    @Setter
    private String protocol = DEFAULT_PROTOCOL;
    @Getter
    @Setter
    private int port = DEFAULT_PORT;
    @Getter
    private String username;
    @Getter
    private String password;
    @Getter
    @Setter
    private boolean dockerRunning;
    @Getter
    @Setter
    private boolean usingInsecure;


    public ConfigDocker( String host, String username, String password, String alias ) {
        this( idBuilder.getAndIncrement(), host, username, password, alias );
    }


    public ConfigDocker( String host, String username, String password ) {
        this( idBuilder.getAndIncrement(), host, username, password, host );
    }


    public ConfigDocker( int id, String host, String username, String password, String alias ) {
        super( "dockerConfig" + id );
        this.id = id;
        if ( idBuilder.get() <= id ) {
            idBuilder.set( id + 1 );
        }
        this.host = host;
        this.alias = alias;
        this.username = username;
        this.password = password;

        this.webUiFormType = WebUiFormType.DOCKER_INSTANCE;
    }


    public static ConfigDocker fromMap( Map<String, Object> value ) {
        ConfigDocker config = new ConfigDocker(
                ((Double) value.get( "id" )).intValue(),
                (String) value.get( "host" ),
                (String) value.getOrDefault( "username", "" ),
                (String) value.getOrDefault( "password", null ),
                (String) value.get( "alias" ) );
        config.setDockerRunning( (Boolean) value.get( "dockerRunning" ) );
        config.setPort( ((Double) value.getOrDefault( "port", DEFAULT_PORT )).intValue() );
        config.setProtocol( (String) value.getOrDefault( "protocol", DEFAULT_PROTOCOL ) );
        config.setUsingInsecure( (Boolean) value.get( "usingInsecure" ) );

        return config;
    }


    public Map<String, String> getSettings() {
        Map<String, String> settings = new HashMap<>();
        settings.put( "host", host );
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
        throw new UnsupportedOperationException( "" );
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
        return port == that.port &&
                dockerRunning == that.dockerRunning &&
                host.equals( that.host ) &&
                alias.equals( that.alias ) &&
                protocol.equals( that.protocol ) &&
                usingInsecure == that.usingInsecure &&
                Objects.equals( username, that.username ) &&
                Objects.equals( password, that.password );
    }

}
