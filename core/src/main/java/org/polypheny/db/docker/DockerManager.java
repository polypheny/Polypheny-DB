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

package org.polypheny.db.docker;

import com.google.common.collect.ImmutableMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.ConfigDocker;
import org.polypheny.db.config.ConfigManager;
import org.polypheny.db.config.RuntimeConfig;

public final class DockerManager {

    private static final DockerManager INSTANCE = new DockerManager();
    private final Map<Integer, DockerInstance> dockerInstances = new ConcurrentHashMap<>();
    private final AtomicBoolean initialized = new AtomicBoolean( false );
    private final Set<ConfigListener> listener = new HashSet<>();


    public static DockerManager getInstance() {
        if ( !INSTANCE.initialized.getAndSet( true ) ) {
            RuntimeConfig.DOCKER_INSTANCES.getList( ConfigDocker.class ).forEach( c -> INSTANCE.addDockerInstance( c.getHost(), c.getAlias(), c.getRegistry(), c.getPort(), c ) );
        }

        return INSTANCE;
    }


    public Optional<DockerInstance> getInstanceById( int instanceId ) {
        // Tests expect a localhost docker instance with id 0
        if ( Catalog.testMode && instanceId == 0 ) {
            return dockerInstances.values().stream().filter( d -> d.getHost().equals( "localhost" ) ).findFirst();
        }
        return Optional.ofNullable( dockerInstances.get( instanceId ) );
    }


    Optional<DockerInstance> getInstanceForContainer( String uuid ) {
        return dockerInstances.values().stream().filter( d -> d.hasContainer( uuid ) ).findFirst();
    }


    public ImmutableMap<Integer, DockerInstance> getDockerInstances() {
        return ImmutableMap.copyOf( dockerInstances );
    }


    public boolean hasHost( String host ) {
        return dockerInstances.values().stream().anyMatch( d -> d.getHost().equals( host ) );
    }


    public boolean hasAlias( String alias ) {
        return dockerInstances.values().stream().anyMatch( d -> d.getAlias().equals( alias ) );
    }


    /**
     * Returns the id of the new DockerInstance for host, or if it already exists the id for that.
     */
    void addDockerInstance( String host, String alias, String registry, int port, @Nullable ConfigDocker existingConfig ) {
        synchronized ( this ) {
            if ( hasHost( host ) ) {
                throw new RuntimeException( "There is already a docker instance connected to " + host );
            }
            if ( hasAlias( alias ) ) {
                throw new RuntimeException( "There is already a docker instance with alias " + alias );
            }
            ConfigDocker configDocker = existingConfig;
            if ( configDocker == null ) {
                // TODO: racy, someone else could modify runtime/dockerInstances elsewhere
                List<ConfigDocker> configList = RuntimeConfig.DOCKER_INSTANCES.getList( ConfigDocker.class );
                configDocker = new ConfigDocker( host, alias, registry, port );
                configList.add( configDocker );
                ConfigManager.getInstance().getConfig( "runtime/dockerInstances" ).setConfigObjectList( configList.stream().map( ConfigDocker::toMap ).collect( Collectors.toList() ), ConfigDocker.class );
            }
            dockerInstances.put( configDocker.getId(), new DockerInstance( configDocker.getId(), host, alias, registry, port ) );
        }
    }


    void updateDockerInstance( int id, String host, String alias, String registry ) {
        synchronized ( this ) {
            DockerInstance dockerInstance = getInstanceById( id ).orElseThrow( () -> new RuntimeException( "No docker instance with id " + id ) );
            if ( !dockerInstance.getHost().equals( host ) && dockerInstances.values().stream().anyMatch( d -> d.getHost().equals( host ) ) ) {
                throw new RuntimeException( "There is already a docker instance connected to " + host );
            }
            if ( !dockerInstance.getAlias().equals( alias ) && dockerInstances.values().stream().anyMatch( d -> d.getAlias().equals( alias ) ) ) {
                throw new RuntimeException( "There is already a docker instance with alias " + alias );
            }

            dockerInstance.updateConfig( host, alias, registry );

            listener.forEach( c -> c.onConfigChange( null ) );

            // TODO: racy, someone else could modify runtime/dockerInstances elsewhere
            List<ConfigDocker> configs = RuntimeConfig.DOCKER_INSTANCES.getList( ConfigDocker.class );
            configs.forEach( c -> {
                if ( c.id == id ) {
                    c.setHost( host );
                    c.setAlias( alias );
                    c.setRegistry( registry );
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


    public void addListener( ConfigListener l ) {
        synchronized ( this ) {
            listener.add( l );
        }
    }


    public void removeListener( ConfigListener l ) {
        synchronized ( this ) {
            listener.remove( l );
        }
    }

}
