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

package org.polypheny.db.adapter;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;

@Getter
public enum DeployMode {

    REMOTE( "remote" ),
    DOCKER( "docker" ),
    EMBEDDED( "embedded" );

    private final String name;


    DeployMode( String name ) {
        this.name = name;
    }


    public static Set<DeployMode> getDeployModes( List<DeploySetting> s ) {
        if ( s.contains( DeploySetting.ALL ) ) {
            return EnumSet.allOf( DeployMode.class );
        } else {
            return s.stream().map( setting -> setting.mode ).collect( Collectors.toSet() );
        }
    }


    public static List<DeploySetting> getDeploySettings( Set<DeployMode> modes ) {
        if ( modes.equals( EnumSet.allOf( DeployMode.class ) ) ) {
            return List.of( DeploySetting.ALL );
        } else {
            return modes.stream().map( DeploySetting::fromDeployMode ).toList();
        }
    }


    public enum DeploySetting {
        REMOTE( DeployMode.REMOTE ),
        DOCKER( DeployMode.DOCKER ),
        EMBEDDED( DeployMode.EMBEDDED ),
        ALL;

        private final DeployMode mode;
        @Getter
        private boolean usedByAll = false;


        DeploySetting( DeployMode mode ) {
            this.mode = mode;
        }


        DeploySetting() {
            usedByAll = true;
            mode = DeployMode.EMBEDDED;
        }


        private static DeploySetting fromDeployMode( DeployMode mode ) {
            return switch ( mode ) {
                case REMOTE -> DeploySetting.REMOTE;
                case DOCKER -> DeploySetting.DOCKER;
                case EMBEDDED -> DeploySetting.EMBEDDED;
            };
        }

    }
}
