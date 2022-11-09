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

import com.github.dockerjava.api.model.ExposedPort;
import com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.config.ConfigDocker;
import org.polypheny.db.config.RuntimeConfig;

/**
 * This class servers as a organization unit which controls all Docker containers in Polypheny.
 * While the callers can and should mostly interact with the underlying containers directly,
 * this instance is used to have a control layer, which allows to restore, start or shutdown multiple of
 * these instances at the same time.
 *
 * For now, we have no way to determent if a previously created/running container with the same name
 * was created by Polypheny, so we try to reuse it.
 */
public abstract class DockerManager {

    public static DockerManagerImpl INSTANCE = null;


    public static DockerManager getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new DockerManagerImpl();
        }
        return INSTANCE;
    }


    /**
     * This method generates a new Polypheny specific container and additionally initializes this container in Docker itself.
     *
     * @return the Container instance
     */
    public abstract Container initialize( Container container );

    /**
     * Starts the provided container,
     * if the container already runs it does nothing,
     * if the container was destroyed it recreates first.
     *
     * @param container the container which is started
     */
    public abstract void start( Container container );


    /**
     * Stops the provided container
     *
     * @param container the container which is stopped
     */
    public abstract void stop( Container container );

    /**
     * Destroys the provided container
     *
     * @param container the container which is stopped
     */
    public abstract void destroy( Container container );

    /**
     * All containers, which belong to the provided adapter, are stopped
     *
     * @param adapterId the id of the adapter
     */
    public abstract void stopAll( int adapterId );

    /**
     * Destroys all containers and removes them from the system, which belong to the provided adapter
     *
     * @param adapterId the id of the adapter
     */
    public abstract void destroyAll( int adapterId );

    public abstract List<String> getUsedNames();

    public abstract List<Integer> getUsedPorts();

    public abstract Map<Integer, List<Integer>> getUsedPortsSorted();

    /**
     * Refreshes the settings of the underlying Docker clients e.g. name, alias etc.
     */
    protected abstract void updateConfigs();

    public abstract DockerStatus probeDockerStatus( int dockerId );


    public abstract void updateIpAddress( Container container );


    /**
     * This enum unifies the name building and provides additional information of an image
     * If one wants to add a new image it has to be added here
     */
    public static class Image {


        @Getter
        private final String name;
        @Getter
        @Setter
        private String version;


        public String getFullName() {
            return this.name + ":" + this.version;
        }


        @Override
        public boolean equals( Object obj ) {
            if ( obj instanceof Image ) {
                Image image = (Image) obj;
                return name.equals( image.name ) && version.equals( image.version );
            }
            return false;
        }


        Image( String name, String version ) {
            this.name = name;
            this.version = version;
        }


        Image( String name ) {
            if ( name.contains( ":" ) ) {
                String[] splits = name.split( ":" );
                this.name = splits[0];
                this.version = splits[1];
            } else {
                this.name = name;
                this.version = "latest";
            }
        }

    }


    public enum ContainerStatus {
        INIT,
        STOPPED,
        RUNNING,
        ERROR,
        DESTROYED
    }


    public static class ContainerBuilder {

        public final Integer adapterId;
        public final Image image;
        public final String uniqueName;
        public Supplier<Boolean> containerReadySupplier = () -> true;
        public Map<Integer, Integer> internalExternalPortMapping = new HashMap<>();
        public boolean checkUnique = false;
        public List<String> initCommands = new ArrayList<>();
        public int dockerInstanceId;
        public int maxTimeoutMs = 2000;

        public List<String> orderCommands = new ArrayList<>();
        private List<String> envCommands = new ArrayList<>();


        public ContainerBuilder( Integer adapterId, String image, String uniqueName, int dockerInstanceId ) {
            this.adapterId = adapterId;
            this.image = new Image( image );
            this.uniqueName = uniqueName;
            this.dockerInstanceId = dockerInstanceId;
        }


        public ContainerBuilder withReadyTest( Supplier<Boolean> containerReadySupplier, int maxTimeoutMs ) {
            this.containerReadySupplier = containerReadySupplier;
            this.maxTimeoutMs = maxTimeoutMs;

            return this;
        }


        public ContainerBuilder withMappedPort( int internalPort, int externalPort ) {
            this.internalExternalPortMapping.put( internalPort, externalPort );

            return this;
        }


        /**
         * This allows to define specific commands which are executed when the container is initialized
         *
         * @param commands a collection of the commands to execute
         * @return the builder
         */
        public ContainerBuilder withInitCommands( List<String> commands ) {
            this.initCommands = Streams.concat( this.initCommands.stream(), commands.stream() ).collect( Collectors.toList() );

            return this;
        }


        /**
         * This allows to define environment variables which are initialized for the container
         *
         * @param variables a list of all variables, which defines them like [ "VARIABLE=value",...]
         * @return the builder
         */
        public ContainerBuilder withEnvironmentVariables( List<String> variables ) {
            this.envCommands = Streams.concat( this.envCommands.stream(), variables.stream() ).collect( Collectors.toList() );

            return this;
        }


        /**
         * This allows to define a single environment variable for the container
         * it can be changed
         *
         * @param variable the environment variable
         * @return the builder
         */
        public ContainerBuilder withEnvironmentVariable( String variable ) {
            this.envCommands.add( variable );

            return this;
        }


        /**
         * Change the used image version
         *
         * @param version the new version of the image
         * @return the builder
         */
        public ContainerBuilder withImageVersion( String version ) {
            image.setVersion( version );

            return this;
        }


        /**
         * This allows to specify commands which are executed when the container and the underlying system have started
         *
         * @param commands the collection of commands to execute
         * @return the builder
         */
        public ContainerBuilder withAfterCommands( List<String> commands ) {
            this.orderCommands.addAll( commands );

            return this;
        }


        public Container build() {

            return new Container(
                    adapterId,
                    uniqueName,
                    image,
                    dockerInstanceId,
                    internalExternalPortMapping,
                    checkUnique,
                    containerReadySupplier,
                    maxTimeoutMs,
                    initCommands,
                    orderCommands,
                    envCommands );
        }


    }


    /**
     * The container is the main interaction instance for calling classes when interacting with Docker.
     * It holds all information for a specific Container
     */
    public static class Container {

        public final Image image;
        public final String uniqueName;
        public final Map<Integer, Integer> internalExternalPortMapping;
        public final Integer adapterId;
        public final List<String> envCommands;
        @Setter
        @Getter
        private ContainerStatus status;
        @Setter
        @Getter
        private String containerId;
        @Getter
        @Setter
        private String ipAddress = RuntimeConfig.USE_DOCKER_NETWORK.getBoolean() ? null : "localhost";


        @Getter
        private final String host;

        @Getter
        private final int dockerInstanceId;

        public final boolean usesInitCommands;
        public final List<String> initCommands;

        public final boolean usesAfterCommands;

        public final List<String> afterCommands;

        public final Supplier<Boolean> isReadySupplier;
        public final int maxTimeoutMs;


        private Container(
                int adapterId,
                String uniqueName,
                Image image,
                int dockerInstanceId,
                Map<Integer, Integer> internalExternalPortMapping,
                boolean checkUnique,
                Supplier<Boolean> isReadySupplier,
                int maxTimeoutMs,
                List<String> initCommands,
                List<String> afterCommands,
                List<String> envCommands
        ) {
            this.adapterId = adapterId;
            this.image = image;
            this.uniqueName = uniqueName;
            this.dockerInstanceId = dockerInstanceId;
            this.internalExternalPortMapping = internalExternalPortMapping;
            this.status = ContainerStatus.INIT;
            this.initCommands = initCommands;
            this.usesInitCommands = !initCommands.isEmpty();
            this.afterCommands = afterCommands;
            this.usesAfterCommands = !afterCommands.isEmpty();
            this.envCommands = envCommands;
            this.isReadySupplier = isReadySupplier;
            this.maxTimeoutMs = maxTimeoutMs;

            this.host = RuntimeConfig.DOCKER_INSTANCES.getWithId( ConfigDocker.class, dockerInstanceId ).getHost();
        }


        /**
         * Starts the container
         *
         * @return the started container
         */
        public Container start() {
            DockerManager.getInstance().start( this );

            return this;
        }


        /**
         * Stops the container
         */
        public void stop() {
            DockerManager.getInstance().stop( this );
        }


        /**
         * Destroys the container, which stops and removes it from the system
         */
        public void destroy() {
            DockerManager.getInstance().destroy( this );
        }


        public List<ExposedPort> getExposedPorts() {
            return internalExternalPortMapping.values().stream().map( ExposedPort::tcp ).collect( Collectors.toList() );
        }


        public String getPhysicalName() {
            return Container.getPhysicalUniqueName( this );
        }


        public static String getPhysicalUniqueName( Container container ) {
            // while not all Docker containers belong to an adapter we annotate it anyway
            String name = container.uniqueName + "_polypheny_" + container.adapterId;
            if ( !Catalog.testMode ) {
                return name;
            } else {
                return name + "_test";
            }
        }


        public static String getFromPhysicalName( String physicalUniqueName ) {
            return physicalUniqueName.split( "_" )[0];
        }


        public void updateIpAddress() {
            DockerManager.getInstance().updateIpAddress( this );
        }

    }

}
