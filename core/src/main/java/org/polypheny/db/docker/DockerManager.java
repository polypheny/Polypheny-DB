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
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.ConfigDocker;
import org.polypheny.db.config.RuntimeConfig;

public final class DockerManager {

    @Getter
    private final Map<Integer, DockerInstance> dockerInstances = new HashMap<>();
    private final Map<String, DockerInstance> containersToInstance = new HashMap<>();
    private static DockerManager INSTANCE;


    private DockerManager() {
    }


    public static DockerManager getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new DockerManager();

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


    public DockerInstance getInstanceForContainer( String uuid ) {
        return containersToInstance.get( uuid );
    }


    public void register( Integer instanceId, DockerInstance instance ) {
        dockerInstances.put( instanceId, instance );
    }


    public DockerInstance getInstanceById( Integer instanceId ) {
        return dockerInstances.get( instanceId );
    }


    public void takeOwnership( String uuid, DockerInstance dockerInstance ) {
        containersToInstance.put( uuid, dockerInstance );
    }


    public void removeContainer( String uuid ) {
        containersToInstance.remove( uuid );
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


    private void updateConfigs() {
        dockerInstances.values().forEach( DockerInstance::updateConfigs );
    }


    public DockerStatus probeDockerStatus( int dockerId ) {
        if ( dockerInstances.containsKey( dockerId ) ) {
            return dockerInstances.get( dockerId ).probeDockerStatus();
        }
        throw new RuntimeException( "There was a problem retrieving the correct Docker instance." );
    }

}
