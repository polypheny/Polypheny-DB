/*
 * Copyright 2019-2024 The Polypheny Project
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
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.ConfigDocker;
import org.polypheny.db.config.ConfigManager;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.docker.exceptions.DockerUserException;
import org.polypheny.db.docker.models.DockerHost;
import org.polypheny.db.util.RunMode;
import org.polypheny.db.docker.models.DockerInstanceInfo;
import org.polypheny.db.docker.models.HandshakeInfo;
import org.polypheny.db.docker.models.UpdateDockerResponse;


public final class DockerManager {

    private static final DockerManager INSTANCE = new DockerManager();
    private final Map<Integer, DockerInstance> dockerInstances = new ConcurrentHashMap<>();
    private final AtomicBoolean initialized = new AtomicBoolean( false );
    private final Set<ConfigListener> listener = new HashSet<>();


    private DockerManager() {
    }


    public static DockerManager getInstance() {
        if ( !INSTANCE.initialized.getAndSet( true ) ) {
            RuntimeConfig.DOCKER_INSTANCES.getList( ConfigDocker.class ).forEach( c -> INSTANCE.addDockerInstance( new DockerHost( c.getHost(), c.getAlias(), c.getRegistry(), c.getCommunicationPort(), c.getHandshakePort(), c.getProxyPort() ), c ) );
        }

        return INSTANCE;
    }


    public Optional<DockerInstance> getInstanceById( int instanceId ) {
        // Tests expect a localhost docker instance with id 0
        if ( Catalog.mode == RunMode.TEST && instanceId == 0 ) {
            return dockerInstances.values().stream().filter( d -> d.getHost().hostname().equals( "localhost" ) ).findFirst();
        }
        return Optional.ofNullable( dockerInstances.get( instanceId ) );
    }


    public Optional<DockerInstance> getInstanceForContainer( String uuid ) {
        return dockerInstances.values().stream().filter( d -> d.hasContainer( uuid ) ).findFirst();
    }


    public ImmutableMap<Integer, DockerInstance> getDockerInstances() {
        return ImmutableMap.copyOf( dockerInstances );
    }


    public List<DockerInstanceInfo> getDockerInstancesMap() {
        return dockerInstances.values().stream().map( DockerInstance::getInfo ).toList();
    }


    public boolean hasHost( String host ) {
        return dockerInstances.values().stream().anyMatch( d -> d.getHost().hostname().equals( host ) );
    }


    public boolean hasAlias( String alias ) {
        return dockerInstances.values().stream().anyMatch( d -> d.getHost().alias().equals( alias ) );
    }


    void addDockerInstance( @NotNull DockerHost host, @Nullable ConfigDocker existingConfig ) {
        synchronized ( this ) {
            if ( hasHost( host.hostname() ) ) {
                throw new DockerUserException( "There is already a Docker instance connected to " + host.hostname() );
            }
            if ( hasAlias( host.alias() ) ) {
                throw new DockerUserException( "There is already a Docker instance with alias " + host.alias() );
            }
            if ( host.registry() == null ) {
                throw new GenericRuntimeException( "registry must not be null" );
            }
            ConfigDocker configDocker = existingConfig;
            if ( configDocker == null ) {
                // TODO: racy, someone else could modify runtime/dockerInstances elsewhere
                List<ConfigDocker> configList = RuntimeConfig.DOCKER_INSTANCES.getList( ConfigDocker.class );
                configDocker = new ConfigDocker( host.hostname(), host.alias(), host.registry(), host.communicationPort(), host.handshakePort(), host.proxyPort() );
                configList.add( configDocker );
                ConfigManager.getInstance().getConfig( "runtime/dockerInstances" ).setConfigObjectList( configList.stream().map( ConfigDocker::toMap ).collect( Collectors.toList() ), ConfigDocker.class );
            }
            dockerInstances.put( configDocker.getId(), new DockerInstance( configDocker.getId(), host ) );
            Catalog.getInstance().updateSnapshot();
        }
    }


    UpdateDockerResponse updateDockerInstance( int id, String hostname, String alias, String registry ) {
        synchronized ( this ) {
            DockerInstance dockerInstance = getInstanceById( id ).orElseThrow( () -> new DockerUserException( 404, "No Docker instance with id " + id ) );
            if ( !dockerInstance.getHost().hostname().equals( hostname ) && hasHost( hostname ) ) {
                throw new DockerUserException( "There is already a Docker instance connected to " + hostname );
            }
            if ( !dockerInstance.getHost().alias().equals( alias ) && hasAlias( alias ) ) {
                throw new DockerUserException( "There is already a Docker instance with alias " + alias );
            }

            Optional<HandshakeInfo> maybeHandshake = dockerInstance.updateConfig( hostname, alias, registry );

            listener.forEach( c -> c.onConfigChange( null ) );

            // TODO: racy, someone else could modify runtime/dockerInstances elsewhere
            List<ConfigDocker> configs = RuntimeConfig.DOCKER_INSTANCES.getList( ConfigDocker.class );
            configs.forEach( c -> {
                if ( c.id == id ) {
                    c.setHost( hostname );
                    c.setAlias( alias );
                    c.setRegistry( registry );
                }
            } );
            ConfigManager.getInstance().getConfig( "runtime/dockerInstances" ).setConfigObjectList( configs.stream().map( ConfigDocker::toMap ).collect( Collectors.toList() ), ConfigDocker.class );
            Catalog.getInstance().updateSnapshot();

            return new UpdateDockerResponse( maybeHandshake.orElse( null ), dockerInstance.getInfo() );
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
            Catalog.getInstance().updateSnapshot();
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
