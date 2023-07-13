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
            RuntimeConfig.DOCKER_INSTANCES.getList( ConfigDocker.class ).forEach( c -> INSTANCE.addDockerInstance( c.getHost(), c.getAlias(), c.getPort() ) );
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
     * Returns the id of the new DockerInstance for host, or if it already exists the id for that.
     */
    int addDockerInstance( String host, String alias, int port ) {
        synchronized ( this ) {
            if ( dockerInstances.values().stream().anyMatch( d -> d.getHost().equals( host ) ) ) {
                throw new RuntimeException( "There is already a docker instance connected to " + host );
            }
            if ( dockerInstances.values().stream().anyMatch( d -> d.getAlias().equals( alias ) ) ) {
                throw new RuntimeException( "There is already a docker instance with alias " + alias );
            }
            // TODO: racy, someone else could modify runtime/dockerInstances elsewhere
            List<ConfigDocker> configList = RuntimeConfig.DOCKER_INSTANCES.getList( ConfigDocker.class );
            ConfigDocker configDocker = new ConfigDocker( host, alias, port );
            configList.add( configDocker );
            ConfigManager.getInstance().getConfig( "runtime/dockerInstances" ).setConfigObjectList( configList.stream().map( ConfigDocker::toMap ).collect( Collectors.toList() ), ConfigDocker.class );
            dockerInstances.put( configDocker.getId(), new DockerInstance( configDocker.getId(), host, alias, port ) );
            return configDocker.getId();
        }
    }


    void updateDockerInstance( int id, String host, String alias ) {
        synchronized ( this ) {
            DockerInstance dockerInstance = getInstanceById( id ).orElseThrow( () -> new RuntimeException( "No docker instance with id " + id ) );
            if ( !dockerInstance.getHost().equals( host ) && dockerInstances.values().stream().anyMatch( d -> d.getHost().equals( host ) ) ) {
                throw new RuntimeException( "There is already a docker instance connected to " + host );
            }
            if ( !dockerInstance.getAlias().equals( alias ) && dockerInstances.values().stream().anyMatch( d -> d.getAlias().equals( alias ) ) ) {
                throw new RuntimeException( "There is already a docker instance with alias " + alias );
            }

            dockerInstance.updateConfig( host, alias );

            // TODO: racy, someone else could modify runtime/dockerInstances elsewhere
            List<ConfigDocker> configs = RuntimeConfig.DOCKER_INSTANCES.getList( ConfigDocker.class );
            configs.forEach( c -> {
                if ( c.id == id ) {
                    c.setHost( host );
                    c.setAlias( alias );
                }
            } );
            ConfigManager.getInstance().getConfig( "runtime/dockerInstances" ).setConfigObjectList( configs.stream().map( ConfigDocker::toMap ).collect( Collectors.toList() ), ConfigDocker.class );
        }
    }


    void removeDockerInstance( int id ) {
        synchronized ( this ) {
            DockerInstance dockerInstance = dockerInstances.remove( id );
            if ( dockerInstance != null ) {
                dockerInstance.close();
            }
            // TODO: racy, someone else could modify runtime/dockerInstances elsewhere
            List<ConfigDocker> newList = RuntimeConfig.DOCKER_INSTANCES.getList( ConfigDocker.class ).stream().filter( c -> c.getId() != id ).collect( Collectors.toList() );
            ConfigManager.getInstance().getConfig( "runtime/dockerInstances" ).setConfigObjectList( newList.stream().map( ConfigDocker::toMap ).collect( Collectors.toList() ), ConfigDocker.class );
        }
    }

}
