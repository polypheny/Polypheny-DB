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

package org.polypheny.db.adapter;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.adapter.Adapter.AdapterSetting;
import org.polypheny.db.adapter.Adapter.DynamicAdapterSettingsList;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.ConfigDocker;
import org.polypheny.db.config.RuntimeConfig;

/**
 * This interface allows to enhance implementors with the functionality
 * of using a Docker container.
 * For this it adds a specific docker setting which is responsible for the used
 * Docker instance, as it is possible to deploy to different remote and local Docker instances.
 *
 * Docker instance configurations can change dynamically.
 * To handle this, the interface can be used to attach a listener to the implementor
 * ( for adapters this is done automatically ) by calling {@link #attachListener}
 * This method calls the interface method {@link #resetDockerConnection} with the changed configurations
 * after the configurations for the attached Docker instance changes.
 */
public interface DockerDeployable {

    List<AdapterSetting> AVAILABLE_DOCKER_SETTINGS = ImmutableList.of(
            new DynamicAdapterSettingsList<>( "instanceId", "DockerInstance", false, true, false, RuntimeConfig.DOCKER_INSTANCES.getList( ConfigDocker.class ), ConfigDocker::getAlias, ConfigDocker.class )
                    .bind( RuntimeConfig.DOCKER_INSTANCES )
                    .setDescription( "To configure additional Docker instances, use the Docker Config in the Config Manager." )
    );

    default List<AdapterSetting> getDockerSettings() {
        return AVAILABLE_DOCKER_SETTINGS;
    }

    /**
     * This function attaches the callee to the specified docker instance,
     * it will call the appropriate resetConnection function when the Docker configuration changes
     *
     * @param dockerInstanceId the id of the corresponding Docker instance
     */
    default ConfigListener attachListener( int dockerInstanceId ) {
        // we have to track the used docker url we attach a listener
        ConfigListener listener = new ConfigListener() {
            @Override
            public void onConfigChange( Config c ) {
                resetDockerConnection( RuntimeConfig.DOCKER_INSTANCES.getWithId( ConfigDocker.class, dockerInstanceId ) );
            }


            @Override
            public void restart( Config c ) {
                resetDockerConnection( RuntimeConfig.DOCKER_INSTANCES.getWithId( ConfigDocker.class, dockerInstanceId ) );
            }
        };
        RuntimeConfig.DOCKER_INSTANCES.addObserver( listener );
        return listener;
    }

    /**
     * This function is called automatically if the configuration of connected Docker instance changes,
     * it is responsible for handling regenerating the connection if the Docker changes demand it
     *
     * @param c the new configuration of the corresponding Docker instance
     */
    void resetDockerConnection( ConfigDocker c );

}
