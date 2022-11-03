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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.ConfigDocker;
import org.polypheny.db.config.RuntimeConfig;

public class DockerManagerImpl extends DockerManager {

    @Getter
    private final Map<Integer, DockerInstance> dockerInstances = new HashMap<>();


    public DockerManagerImpl() {

        ConfigListener listener = new ConfigListener() {
            @Override
            public void onConfigChange( Config c ) {
                resetClients();
            }


            @Override
            public void restart( Config c ) {
                resetClients();
            }
        };
        resetClients();
        RuntimeConfig.DOCKER_INSTANCES.addObserver( listener );
    }


    /**
     * Resets the used clients by pulling the {@link RuntimeConfig} for Docker
     * and deleting/adding new clients according to it
     */
    public void resetClients() {
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
        updateConfigs();

        // add new clients
        dockerInstanceIds.forEach( id -> {
            if ( !dockerInstances.containsKey( id ) ) {
                dockerInstances.put( id, new DockerInstance( id ) );
            }
        } );
    }


    @Override
    public Container initialize( Container container ) {
        dockerInstances.get( container.getDockerInstanceId() ).initialize( container );

        return container;
    }


    @Override
    public void start( Container container ) {
        dockerInstances.get( container.getDockerInstanceId() ).start( container );
    }


    @Override
    public void updateIpAddress( Container container ) {
        dockerInstances.get( container.getDockerInstanceId() ).updateIpAddress( container );
    }


    @Override
    public void stop( Container container ) {
        dockerInstances.get( container.getDockerInstanceId() ).stop( container );
    }


    @Override
    public void destroy( Container container ) {
        dockerInstances.get( container.getDockerInstanceId() ).destroy( container );
    }


    @Override
    public void stopAll( int adapterId ) {
        dockerInstances.values().forEach( dockerInstance -> dockerInstance.stopAll( adapterId ) );
    }


    @Override
    public void destroyAll( int adapterId ) {
        dockerInstances.values().forEach( dockerInstance -> dockerInstance.destroyAll( adapterId ) );
    }


    @Override
    public List<String> getUsedNames() {
        return dockerInstances.values().stream()
                .flatMap( client -> client.getUsedNames().stream() )
                .collect( Collectors.toList() );
    }


    @Override
    public List<Integer> getUsedPorts() {
        return dockerInstances.values().stream()
                .flatMap( client -> client.getUsedPorts().stream() )
                .collect( Collectors.toList() );
    }


    @Override
    public Map<Integer, List<Integer>> getUsedPortsSorted() {
        return dockerInstances.entrySet().stream()
                .filter( e -> e.getValue().isDockerRunning() )
                .collect( Collectors.toMap( Entry::getKey, e -> e.getValue().getUsedPorts() ) );
    }


    @Override
    protected void updateConfigs() {
        dockerInstances.values().forEach( DockerManager::updateConfigs );
    }


    @Override
    public DockerStatus probeDockerStatus( int dockerId ) {
        if ( dockerInstances.containsKey( dockerId ) ) {
            return dockerInstances.get( dockerId ).probeDockerStatus( dockerId );
        }
        throw new RuntimeException( "There was a problem retrieving the correct Docker instance." );
    }

}
