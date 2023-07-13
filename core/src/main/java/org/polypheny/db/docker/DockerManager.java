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

package org.polypheny.db.docker;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.ConfigDocker;
import org.polypheny.db.config.ConfigManager;
import org.polypheny.db.config.RuntimeConfig;

public final class DockerManager {

    @Getter
    private final Map<Integer, DockerInstance> dockerInstances = new ConcurrentHashMap<>();
    private final Map<String, DockerInstance> containerToInstance = new ConcurrentHashMap<>();
    private static final DockerManager INSTANCE = new DockerManager();
    private final AtomicBoolean initialized = new AtomicBoolean( false );


    private DockerManager() {
    }


    public static DockerManager getInstance() {
        if ( !INSTANCE.initialized.getAndSet( true ) ) {
            // The following was previously inside the DockerManager constructor,
            // but this lead to an issue because getInstance() is called recursively
            // inside DockerInstance->checkConnection.
            ConfigListener listener = new ConfigListener() {
                @Override
                public void onConfigChange( Config c ) {
                    INSTANCE.resetClients();
                }


                @Override
                public void restart( Config c ) {
                    INSTANCE.resetClients();
                }
            };
            INSTANCE.resetClients();
            RuntimeConfig.DOCKER_INSTANCES.addObserver( listener );
        }
        return INSTANCE;
    }


    public Optional<DockerInstance> getInstanceById( Integer instanceId ) {
        return Optional.ofNullable( dockerInstances.getOrDefault( instanceId, null ) );
    }


    void takeOwnership( String uuid, DockerInstance dockerInstance ) {
        containerToInstance.put( uuid, dockerInstance );
    }


    DockerInstance getInstanceForContainer( String uuid ) {
        return containerToInstance.get( uuid );
    }


    void removeContainer( String uuid ) {
        containerToInstance.remove( uuid );
    }


    /**
     * Resets the used clients by pulling the {@link RuntimeConfig} for Docker
     * and deleting/adding new clients according to it
     */
    void resetClients() {
        synchronized ( this ) {
            List<Integer> dockerInstanceIds = RuntimeConfig.DOCKER_INSTANCES
                    .getList( ConfigDocker.class )
                    .stream()
                    .map( config -> config.id )
                    .collect( Collectors.toList() );
            // remove unused clients
            List<Integer> removeIds = dockerInstances
                    .keySet()
                    .stream()
                    .filter( id -> !dockerInstanceIds.contains( id ) )
                    .collect( Collectors.toList() );

            removeIds.forEach( dockerInstances::remove );
            // update internal values
            dockerInstances.values().forEach( DockerInstance::updateConfigs );

            // add new clients
            dockerInstanceIds.forEach( id -> {
                if ( !dockerInstances.containsKey( id ) ) {
                    dockerInstances.put( id, new DockerInstance( id ) );
                }
            } );
        }
    }


    /**
     * Returns the id of the new DockerInstance for host, or if it already exists the id for that.
     */
    static int addDockerInstance( String host, String alias, int port ) {
        // TODO: racy, someone else could modify the setting elsewhere
        List<ConfigDocker> configList = RuntimeConfig.DOCKER_INSTANCES.getList( ConfigDocker.class );
        for ( ConfigDocker c : configList ) {
            if ( c.getHost().equals( host ) && c.getPort() == port ) {
                return c.getId();
            }
        }
        // Add a new entry
        ConfigDocker configDocker = new ConfigDocker( host, alias, port );
        configList.add( configDocker );
        ConfigManager.getInstance().getConfig( "runtime/dockerInstances" ).setConfigObjectList( configList.stream().map( ConfigDocker::toMap ).collect( Collectors.toList() ), ConfigDocker.class );
        return configDocker.getId();
    }


    static void removeDockerInstance( int id ) {
        // TODO: racy, someone else could modify the setting elsewhere
        List<ConfigDocker> newList = RuntimeConfig.DOCKER_INSTANCES.getList( ConfigDocker.class ).stream().filter( c -> c.getId() != id ).collect( Collectors.toList() );
        ConfigManager.getInstance().getConfig( "runtime/dockerInstances" ).setConfigObjectList( newList.stream().map( ConfigDocker::toMap ).collect( Collectors.toList() ), ConfigDocker.class );
    }

}
